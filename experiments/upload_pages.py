#!/usr/bin/env python3
"""Replay sharded engine page dumps into a Cosmos table (page datasets).

Shards come from shard_dump.py (one gz per logical partition). Rows are
replayed verbatim: PartitionKey/RowKey strings straight from the dump,
Data = raw page bytes (the property scheme the engine's read path was
proven against on obj-100g-0). Metadata rows (metapage binary,
pg_cnt_/row_cnt_ INT64) go last via --finalize, so a complete metadata
set implies a complete table.

Pacing reuses the record uploader's debt-style RU bucket + clamped AIMD
estimator. Resume is shard-granular: finished shards are appended to the
manifest and skipped on rerun. Shard order is deterministically shuffled
(seed 20260811) to spread concurrent EGTs across physical partitions.
"""
import argparse
import glob
import gzip
import os
import queue
import random
import sys
import threading
import time

from azure.core.exceptions import HttpResponseError
from azure.data.tables import (EdmType, EntityProperty, TableServiceClient,
                               UpdateMode)

SHUFFLE_SEED = 20260811


class RUBucket:
    """Debt-style token bucket (never deadlocks on a large batch)."""

    def __init__(self, ru_per_s):
        self.rate = float(ru_per_s)
        self.tokens = 0.0
        self.last = time.time()
        self.lock = threading.Lock()

    def acquire(self, ru):
        with self.lock:
            now = time.time()
            self.tokens = min(self.rate,
                              self.tokens + (now - self.last) * self.rate)
            self.last = now
            self.tokens -= ru
            wait = -self.tokens / self.rate if self.tokens < 0 else 0.0
        if wait > 0:
            time.sleep(wait)


class Estimator:
    """AIMD RU-per-page estimate clamped to [0.5, 2]x the seed."""

    def __init__(self, seed):
        self.value = seed
        self.floor = 0.5 * seed
        self.cap = 2.0 * seed
        self.clean = 0
        self.lock = threading.Lock()

    def throttled(self):
        with self.lock:
            self.value = min(self.cap, self.value * 1.1)
            self.clean = 0

    def ok(self):
        with self.lock:
            self.clean += 1
            if self.clean >= 5:
                self.value = max(self.floor, self.value * 0.97)
                self.clean = 0

    def get(self):
        with self.lock:
            return self.value


def is_throttle(exc):
    if getattr(exc, "status_code", None) == 429:
        return True
    resp = getattr(exc, "response", None)
    return getattr(resp, "status_code", None) == 429


def shard_lines(path):
    with gzip.open(path, "rt") as fh:
        for ln in fh:
            tbl, part, rk, hx = ln.rstrip("\n").split("\t")
            yield part, rk, hx


def retry_throttled(fn, what, retries=60):
    """Wrap a single-entity call in the same 429 backoff upload_batch uses.

    The finalize and sample-verify passes run AFTER the bulk upload, when the
    table is still absorbing the write burst, so their point operations can be
    throttled too -- and an unhandled 429 there kills the driver with every
    row already uploaded.
    """
    for attempt in range(retries):
        try:
            return fn()
        except HttpResponseError as e:
            if not is_throttle(e):
                raise
            time.sleep(min(30.0, 0.5 * (1.4 ** min(attempt, 10))))
    raise RuntimeError("429 retries exhausted for %s" % what)


def upload_batch(tc, rows, bucket, est, retries, stats):
    """rows: list of (part, rk, hex). One same-partition EGT."""
    ops = []
    for part, rk, hx in rows:
        ent = {"PartitionKey": part, "RowKey": rk,
               "Data": bytes.fromhex(hx)}
        ops.append(("upsert", ent, {"mode": UpdateMode.REPLACE}))
    bucket.acquire(est.get() * len(ops))
    for attempt in range(retries):
        try:
            tc.submit_transaction(ops)
            est.ok()
            return
        except HttpResponseError as e:
            if is_throttle(e):
                est.throttled()
                with stats["lock"]:
                    stats["429"] += 1
                time.sleep(min(30.0, 0.5 * (1.4 ** min(attempt, 10))))
            elif "too large" in str(e).lower() and len(rows) > 1:
                # Cosmos EGT body cap: split and recurse (double-paced: the
                # halves acquire RU again -- conservative, harmless)
                half = len(rows) // 2
                upload_batch(tc, rows[:half], bucket, est, retries, stats)
                upload_batch(tc, rows[half:], bucket, est, retries, stats)
                return
            else:
                print("EGT error (%s): %s" % (rows[0][0], e), file=sys.stderr,
                      flush=True)
                time.sleep(min(30.0, 1.0 * (attempt + 1)))
    raise RuntimeError("EGT failed %d times for partition %s"
                       % (retries, rows[0][0]))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--conn-file", required=True)
    ap.add_argument("--table", required=True)
    ap.add_argument("--shard-dir", required=True)
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--ru-budget", type=float, default=400.0)
    ap.add_argument("--ru-per-page", type=float, default=85.0,
                    help="seed RU per 8.5KB page write; AIMD-corrected "
                         "within [0.5, 2]x at runtime")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--batch", type=int, default=40,
                    help="pages per EGT; 8.5KB pages hit the Cosmos "
                         "transaction size cap near ~50-100, oversize "
                         "batches auto-split at runtime")
    ap.add_argument("--retries", type=int, default=50)
    ap.add_argument("--num-procs", type=int, default=1,
                    help="total cooperating processes (shards striped "
                         "deterministically across them)")
    ap.add_argument("--proc-id", type=int, default=0)
    ap.add_argument("--resume-glob",
                    help="read completed shards from every manifest "
                         "matching this glob (own manifest always read)")
    ap.add_argument("--max-shards", type=int, default=0,
                    help="test mode: stop after N shards")
    ap.add_argument("--create-table", action="store_true",
                    help="create table if missing (data-plane creation "
                         "defaults to 800 RU/s -- set_ru 400 after!)")
    ap.add_argument("--finalize", action="store_true",
                    help="upload meta.tsv rows after shards complete")
    ap.add_argument("--sample-verify", type=int, default=0)
    args = ap.parse_args()

    conn = open(args.conn_file).read().strip()
    svc = TableServiceClient.from_connection_string(conn)
    if args.create_table:
        svc.create_table_if_not_exists(args.table)
        print("table %s ensured (data-plane default 800 RU/s -- "
              "run set_ru.py set 400 afterwards!)" % args.table, flush=True)
    tc = svc.get_table_client(args.table)

    index = {}
    with open(os.path.join(args.shard_dir, "index.txt")) as f:
        for ln in f:
            shard, cnt = ln.split()
            index[int(shard)] = int(cnt)

    done = set()
    manifests = [args.manifest]
    if args.resume_glob:
        manifests += glob.glob(args.resume_glob)
    for path in set(manifests):
        if os.path.exists(path):
            with open(path) as f:
                for ln in f:
                    done.add(int(ln.split()[0]))
    todo = sorted(index)
    random.Random(SHUFFLE_SEED).shuffle(todo)
    todo = [s for s in todo[args.proc_id::args.num_procs] if s not in done]
    if args.max_shards:
        todo = todo[:args.max_shards]
    total_pages = sum(index[s] for s in todo)
    print("%d shards done, %d todo (%d pages)"
          % (len(done), len(todo), total_pages), flush=True)

    bucket = RUBucket(args.ru_budget)
    est = Estimator(args.ru_per_page)
    stats = {"lock": threading.Lock(), "pages": 0, "429": 0,
             "t0": time.time()}
    manifest_lock = threading.Lock()
    q = queue.Queue()
    for s in todo:
        q.put(s)
    failures = []

    def report():
        el = time.time() - stats["t0"]
        rate = stats["pages"] / max(el, 1e-9)
        eta = (total_pages - stats["pages"]) / max(rate, 1e-9)
        print("%d / %d pages (%.0f pages/s, est %.1f RU/page, 429s=%d, "
              "ETA %.0f min)" % (stats["pages"], total_pages, rate,
                                 est.get(), stats["429"], eta / 60),
              flush=True)

    def worker(wid):
        time.sleep(wid * 0.2)  # stagger start
        tcw = TableServiceClient.from_connection_string(conn) \
            .get_table_client(args.table)
        while True:
            try:
                shard = q.get_nowait()
            except queue.Empty:
                return
            try:
                rows = []
                n = 0
                for part, rk, hx in shard_lines(
                        os.path.join(args.shard_dir, "s%d.tsv.gz" % shard)):
                    rows.append((part, rk, hx))
                    if len(rows) == args.batch:
                        upload_batch(tcw, rows, bucket, est, args.retries,
                                     stats)
                        n += len(rows)
                        rows = []
                if rows:
                    upload_batch(tcw, rows, bucket, est, args.retries, stats)
                    n += len(rows)
                assert n == index[shard], \
                    "shard %d: uploaded %d != index %d" % (shard, n,
                                                           index[shard])
                with manifest_lock:
                    with open(args.manifest, "a") as f:
                        f.write("%d %d\n" % (shard, n))
                        f.flush()
                        os.fsync(f.fileno())
                old = stats["pages"]
                with stats["lock"]:
                    stats["pages"] += n
                if stats["pages"] // 50000 != old // 50000:
                    report()
            except Exception as e:  # noqa: BLE001
                print("shard %d FAILED: %s" % (shard, e), file=sys.stderr,
                      flush=True)
                failures.append(shard)

    threads = [threading.Thread(target=worker, args=(i,), daemon=True)
               for i in range(args.workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    report()
    if failures:
        print("FAILED shards: %s -- rerun to resume" % sorted(failures),
              file=sys.stderr, flush=True)
        sys.exit(1)

    if args.finalize and not args.max_shards:
        with open(os.path.join(args.shard_dir, "meta.tsv")) as f:
            for ln in f:
                tbl, part, rk, field = ln.rstrip("\n").split("\t")
                if field.startswith("INT:"):
                    ent = {"PartitionKey": part, "RowKey": rk,
                           "Data": EntityProperty(int(field[4:]),
                                                  EdmType.INT64)}
                else:
                    ent = {"PartitionKey": part, "RowKey": rk,
                           "Data": bytes.fromhex(field)}
                retry_throttled(
                    lambda e=ent: tc.upsert_entity(e, mode=UpdateMode.REPLACE),
                    "metadata %s" % rk)
                print("metadata %s uploaded" % rk, flush=True)

    if args.sample_verify:
        rng = random.Random(7)
        shards = sorted(index)
        picked = min(len(shards), max(1, args.sample_verify // 10))
        per_shard = args.sample_verify // picked
        bad = checked = 0
        for shard in rng.sample(shards, picked):
            lines = list(shard_lines(
                os.path.join(args.shard_dir, "s%d.tsv.gz" % shard)))
            for part, rk, hx in rng.sample(lines,
                                           min(per_shard, len(lines))):
                got = retry_throttled(lambda p=part, r=rk: tc.get_entity(p, r),
                                      "sample %s/%s" % (part, rk))
                data = got["Data"]
                data = data.value if hasattr(data, "value") else bytes(data)
                checked += 1
                if data != bytes.fromhex(hx):
                    bad += 1
                    print("MISMATCH %s/%s" % (part, rk), file=sys.stderr,
                          flush=True)
        print("sample-verify: %d/%d byte-exact" % (checked - bad, checked),
              flush=True)
        if bad:
            sys.exit(1)


if __name__ == "__main__":
    main()
