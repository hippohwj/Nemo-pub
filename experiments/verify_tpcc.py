#!/usr/bin/env python3
"""Post-import verification for a TPC-C Cosmos dataset (34 W).

Deliberately NOT a port of verify_100g.py: that one re-synthesises every
YCSB row from synth_ycsb.learn() to compare against, and TPC-C has no
synthesiser. Byte-exactness is covered by upload_pages.py --sample-verify
(it samples the shard files themselves and reads them back). What is left
-- and what this checks -- is completeness:

  1. coverage: for every table, the union of the per-process manifests
     equals the shard set in index.txt, with matching item counts
  2. metadata: every table carries the rows the engine's restore path
     reads -- row_cnt_ for record datasets, plus metapage and pg_cnt_ for
     page datasets -- and row_cnt_ matches the dump
  3. RU: every table is back at manual 400 RU/s (the standing rule)

  verify_tpcc.py --conn-file F --shards-root DIR --manifest-dir DIR
                 [--expect-pages] [--skip-ru]

Exit code 0 only if everything passes; each failure prints one line.
"""
import argparse
import glob
import os
import sys

from azure.data.tables import TableServiceClient


def read_index(path):
    out = {}
    with open(path) as f:
        for ln in f:
            if ln.strip():
                shard, cnt = ln.split()
                out[int(shard)] = int(cnt)
    return out


def read_manifests(pattern):
    out = {}
    for path in glob.glob(pattern):
        with open(path) as f:
            for ln in f:
                if ln.strip():
                    shard, cnt = ln.split()[:2]
                    out[int(shard)] = int(cnt)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--conn-file", required=True)
    ap.add_argument("--shards-root", required=True)
    ap.add_argument("--manifest-dir", required=True)
    ap.add_argument("--expect-pages", action="store_true",
                    help="page dataset: also require metapage + pg_cnt_")
    ap.add_argument("--skip-ru", action="store_true")
    args = ap.parse_args()

    conn = open(args.conn_file).read().strip()
    svc = TableServiceClient.from_connection_string(conn)

    tables = sorted(os.path.basename(os.path.dirname(p)) for p in
                    glob.glob(os.path.join(args.shards_root, "*",
                                           "index.txt")))
    assert tables, "no <table>/index.txt under %s" % args.shards_root

    bad = 0
    total_items = 0
    meta_expected = 0
    for tbl in tables:
        d = os.path.join(args.shards_root, tbl)
        index = read_index(os.path.join(d, "index.txt"))
        done = read_manifests(os.path.join(args.manifest_dir, "%s_*.txt" % tbl))
        items = sum(index.values())
        total_items += items

        missing = sorted(set(index) - set(done))
        wrong = sorted(s for s in set(index) & set(done)
                       if index[s] != done[s])
        if missing or wrong:
            bad += 1
            print("FAIL %s: %d shards missing %s, %d count mismatches %s"
                  % (tbl, len(missing), missing[:5], len(wrong), wrong[:5]))
        else:
            print("ok   %s: %d/%d shards, %d items"
                  % (tbl, len(done), len(index), items))

        # metadata rows the engine reads on restore. The dump is the ground
        # truth for WHICH rows a table should carry: in a page dataset only
        # the 11 data-page tables get metapage/pg_cnt_/row_cnt_, while the 10
        # index-page tables legitimately carry none (the engine writes the
        # index metapage under the data table's storage id). Requiring the
        # same set from every table reports 30 phantom failures.
        want = set()
        dumped_row_cnt = None
        meta_path = os.path.join(d, "meta.tsv")
        if os.path.exists(meta_path):
            with open(meta_path) as f:
                for ln in f:
                    c = ln.rstrip("\n").split("\t")
                    if len(c) == 4:
                        want.add(c[2])
                        if c[2] == "row_cnt_":
                            dumped_row_cnt = int(c[3].split(":", 1)[1])
        meta_expected += len(want)
        tc = svc.get_table_client(tbl)
        for rk in sorted(want):
            try:
                ent = tc.get_entity("metadata", rk)
            except Exception as e:  # noqa: BLE001
                bad += 1
                print("FAIL %s: metadata/%s missing (%s)"
                      % (tbl, rk, type(e).__name__))
                continue
            if rk == "row_cnt_" and dumped_row_cnt is not None:
                got = ent["Data"]
                got = got.value if hasattr(got, "value") else got
                if int(got) != dumped_row_cnt:
                    bad += 1
                    print("FAIL %s: row_cnt_ %s != dumped %d"
                          % (tbl, got, dumped_row_cnt))

    if not args.skip_ru:
        prefix = tables[0].rsplit("-", 1)[0]
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        os.environ.setdefault("NEMO_AZURE_CONF", args.conn_file)
        from azure.cosmos import CosmosClient  # noqa: E402
        import re  # noqa: E402
        acct = re.search(r"AccountName=([^;]+)", conn).group(1)
        key = re.search(r"AccountKey=([^;]+)", conn).group(1)
        db = CosmosClient("https://%s.documents.azure.com:443/" % acct,
                          credential=key).get_database_client("TablesDB")
        for tbl in tables:
            tp = db.get_container_client(tbl).get_throughput()
            ru = tp.auto_scale_max_throughput or tp.offer_throughput
            if tp.auto_scale_max_throughput or ru != 400:
                bad += 1
                print("FAIL %s: throughput is %s, expected manual 400"
                      % (tbl, ru))
        print("RU check done for %s* (%d tables)" % (prefix, len(tables)))

    if args.expect_pages and meta_expected == 0:
        bad += 1
        print("FAIL: a page dataset must carry metapage/pg_cnt_/row_cnt_ on "
              "its data-page tables, but the dump declares none")
    print("%s: %d tables, %d items, %d metadata rows checked, %d problem(s)"
          % ("PASS" if bad == 0 else "FAIL", len(tables), total_items,
             meta_expected, bad))
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
