#!/usr/bin/env python3
"""Upload a TPC-C load dump (dump_thd*.tsv) into Cosmos Table API.

Usage: upload_dump.py <dump_dir> <dataset_id e.g. otpcc1w_clv> [workers=8]

tsv columns: tbl_id \t partition \t rowkey \t hex(data)
Rows are grouped per (table, partition) and submitted as entity-group
transactions of <=100 upserts. SDK-default retry handles 429s. At the end
prints per-table upload counts and writes the metadata row_cnt_ rows.
"""
import glob
import os
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

from azure.data.tables import TableServiceClient, EntityProperty, EdmType

CONF = os.environ.get("NEMO_AZURE_CONF", "build/configs/ifconfig_azure.txt")
BATCH = int(os.environ.get("UPLOAD_BATCH", "100"))

def main():
    dump_dir, dataset = sys.argv[1], sys.argv[2]
    workers = int(sys.argv[3]) if len(sys.argv) > 3 else 8
    conn = open(CONF).read().strip()
    svc = TableServiceClient.from_connection_string(conn)

    # read all dumps, bucket per (tbl, partition)
    buckets = defaultdict(list)   # (tbl, part) -> [(rowkey, bytes)]
    per_tbl = defaultdict(int)
    t0 = time.time()
    for f in sorted(glob.glob(os.path.join(dump_dir, "dump_thd*.tsv"))):
        with open(f) as fh:
            for ln in fh:
                tbl, part, rk, hx = ln.rstrip("\n").split("\t")
                buckets[(int(tbl), part)].append((rk, bytes.fromhex(hx)))
                per_tbl[int(tbl)] += 1
    total = sum(per_tbl.values())
    print("parsed %d rows in %.0fs across %d (tbl,part) buckets"
          % (total, time.time() - t0, len(buckets)), flush=True)

    # UPLOAD_TABLES=1,2,6 uploads only those tbl ids (read-only mixes do
    # not need every table)
    only = os.environ.get("UPLOAD_TABLES")
    if only:
        keep = {int(x) for x in only.split(",")}
        buckets = {k: v for k, v in buckets.items() if k[0] in keep}
        per_tbl = {t: n for t, n in per_tbl.items() if t in keep}
        total = sum(per_tbl.values())
        print("table filter %s -> %d rows" % (sorted(keep), total), flush=True)
    # interleave round-robin across TABLES (each table's chunks themselves
    # round-robin across its partitions): per-table RU budgets are
    # independent, so consecutive jobs must hit different tables or the
    # workers pile onto one table's 400 RU/s while the others idle
    from itertools import zip_longest
    per_table_buckets = defaultdict(list)
    for (tbl, part), rows in buckets.items():
        per_table_buckets[tbl].append(
            [(tbl, part, rows[i:i + BATCH]) for i in range(0, len(rows), BATCH)])
    per_table = []
    for tbl in sorted(per_table_buckets):
        blists = per_table_buckets[tbl]
        per_table.append([c for rnd in zip_longest(*blists)
                          for c in rnd if c is not None])
    jobs = [j for rnd in zip_longest(*per_table) for j in rnd if j is not None]

    # resume support: completed chunks are recorded in a manifest keyed by
    # (tbl, part, first-rowkey); a restart skips them. The manifest header
    # pins (total, BATCH) so a different dump shape starts fresh.
    manifest_path = os.path.join(dump_dir, ".uploaded_chunks")
    header = "v1 %d %d" % (total, BATCH)
    done_keys = set()
    if os.path.exists(manifest_path):
        with open(manifest_path) as mf:
            lines = mf.read().splitlines()
        if lines and lines[0] == header:
            done_keys = set(lines[1:])
            print("resume: %d chunks already uploaded" % len(done_keys), flush=True)
        else:
            print("resume manifest stale (different dump shape); starting fresh", flush=True)
    if not done_keys:
        with open(manifest_path, "w") as mf:
            mf.write(header + "\n")
    jobs = [j for j in jobs
            if ("%d:%s:%s" % (j[0], j[1], j[2][0][0])) not in done_keys]
    manifest = open(manifest_path, "a")
    import threading
    manifest_lock = threading.Lock()

    clients = {}
    def client_for(tbl):
        if tbl not in clients:
            clients[tbl] = svc.get_table_client("%s-%d" % (dataset, tbl))
        return clients[tbl]
    for tbl in per_tbl:
        client_for(tbl)

    done = [0]
    t0 = time.time()
    # strict-budget pacing: enforce a minimum gap between batches PER TABLE
    # so a tail of single-table jobs cannot exceed its RU/s budget and
    # trigger a 429 debt spiral (UPLOAD_TABLE_GAP_MS, default off)
    table_gap = float(os.environ.get("UPLOAD_TABLE_GAP_MS", "0")) / 1000.0
    last_hit = defaultdict(float)
    def push(job):
        tbl, part, rows = job
        if table_gap > 0:
            wait = last_hit[tbl] + table_gap - time.time()
            if wait > 0:
                time.sleep(wait)
            last_hit[tbl] = time.time()
        ops = [("upsert", {"PartitionKey": part, "RowKey": rk, "Data": data})
               for rk, data in rows]
        max_att = int(os.environ.get("UPLOAD_RETRIES", "50"))
        for attempt in range(max_att):
            try:
                client_for(tbl).submit_transaction(ops)
                break
            except Exception as e:
                if attempt == max_att - 1:
                    raise
                time.sleep(min(10.0, 0.3 * (attempt + 1)))
        with manifest_lock:
            manifest.write("%d:%s:%s\n" % (tbl, part, rows[0][0]))
            manifest.flush()
        done[0] += len(rows)
        if done[0] % 50000 < len(rows):
            el = time.time() - t0
            print("uploaded %d / %d rows (%.0f rows/s, %.1f min elapsed)"
                  % (done[0], total, done[0] / el, el / 60), flush=True)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(push, jobs))
    el = time.time() - t0
    print("UPLOAD DONE: %d rows in %.1f min (%.0f rows/s)"
          % (done[0], el / 60, done[0] / el), flush=True)

    # metadata + verification counts
    for tbl, n in sorted(per_tbl.items()):
        tc = client_for(tbl)
        tc.upsert_entity({"PartitionKey": "metadata", "RowKey": "row_cnt_",
                          "Data": EntityProperty(n, EdmType.INT64)})
        cnt = sum(1 for e in tc.list_entities(select=["PartitionKey"],
                                              results_per_page=1000)
                  if e["PartitionKey"] != "metadata")
        print("verify tbl-%d: uploaded=%d in-store=%d %s"
              % (tbl, n, cnt, "OK" if cnt == n else "**MISMATCH**"), flush=True)

if __name__ == "__main__":
    main()
