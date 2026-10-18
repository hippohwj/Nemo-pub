#!/usr/bin/env python3
"""Stream-upload a key range of the obj-100g YCSB dataset into Cosmos Table
API (plan A: rows synthesized on the fly via synth_ycsb, no dump files).

  upload_100g.py --conn-file F --ref ref_head.tsv --table obj-100g-0 \
                 --start-key 0 --end-key 10000 --ru-budget 400 [options]

Key properties:
- constant memory: keys are generated chunk by chunk, nothing is buffered
- chunks are 100-key aligned so an EGT never crosses a partition
  (g_partition_sz=10000 is a multiple of the batch size)
- writes are upsert-REPLACE EGTs: measured at 400 RU/s saturation,
  create=11.1 / upsert-replace=11.6 / upsert-merge=11.9 RU/row
  (~equal; replace is idempotent with no 409-fallback complexity)
- RU pacing: a debt-style token bucket holds the process at --ru-budget
  RU/s. The Cosmos Tables endpoint returns NO x-ms-request-charge header
  (verified empirically on upsert/EGT/get), so the RU cost of a
  row cannot be read off responses; instead the per-row estimate is tuned
  AIMD-style around the measured 11.5 seed: x1.1 on every 429, x0.97 after
  5 clean batches, clamped to [0.5, 2]x the seed so a transient 429 burst
  cannot spiral the estimate up and silently strangle throughput (that
  failure mode cost P2 60% of its budget).
- resume: completed chunks go to --manifest; a restart skips them
- --no-egt: per-row upserts (Azurite smoke — Azurite rejects EGT batches)
- --dump-dir: plan-B mode, stream rows from dump_thd*.tsv instead of
  synthesizing (same pacing/manifest machinery)
- --sample-verify N: after the upload, N random point reads compared
  byte-for-byte against the synthesizer
"""
import argparse
import glob
import os
import random
import sys
import threading
import time

from azure.core.exceptions import HttpResponseError

from azure.data.tables import TableServiceClient, UpdateMode

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from synth_ycsb import PART_SZ, learn  # noqa: E402


class RUBucket:
    """Debt-style token bucket: one acquisition may exceed the 1s burst —
    it borrows and the caller sleeps the debt off afterwards — so pacing
    holds at rate RU/s long-run and can never deadlock on a large batch."""

    def __init__(self, ru_per_s):
        self.rate = float(ru_per_s)
        self.tokens = 0.0  # start empty: no synchronized first-second burst
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
    """AIMD RU-per-row estimate around a measured seed. Clamped to
    [0.5, 2]x the seed: 429s nudge it up, clean batches pull it back, and
    a transient throttle burst can never inflate it enough to strangle
    throughput for the rest of the run."""

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


class Stats:
    def __init__(self, total_rows, est):
        self.lock = threading.Lock()
        self.rows = 0
        self.throttled = 0
        self.total = total_rows
        self.est = est
        self.t0 = time.time()

    def add(self, n):
        with self.lock:
            self.rows += n
            report = self.rows % 50000 < n
            snap = (self.rows, self.throttled)
        if report:
            rows, thr = snap
            el = time.time() - self.t0
            rate = rows / max(el, 1e-9)
            eta = (self.total - rows) / max(rate, 1e-9)
            print("%d / %d rows (%.0f rows/s, est %.1f RU/row, 429s=%d, "
                  "ETA %.0f min)" % (rows, self.total, rate, self.est.get(),
                                     thr, eta / 60), flush=True)


def chunk_ranges(start, end, batch):
    k = start
    while k < end:
        nxt = min(end, (k // batch + 1) * batch)
        yield (k, nxt)
        k = nxt


def tsv_rows(dump_dir):
    """Plan-B source: yield (key, partition, rowkey, data) from dump tsvs."""
    for f in sorted(glob.glob(os.path.join(dump_dir, "dump_thd*.tsv"))):
        with open(f) as fh:
            for ln in fh:
                tbl, part, rk, hx = ln.rstrip("\n").split("\t")
                yield int(rk), part, rk, bytes.fromhex(hx)


def is_throttle(exc):
    if getattr(exc, "status_code", None) == 429:
        return True
    resp = getattr(exc, "response", None)
    return getattr(resp, "status_code", None) == 429


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--conn-file", required=True)
    ap.add_argument("--table", default="obj-100g-0")
    ap.add_argument("--ref", help="reference tsv for the synthesizer (plan A)")
    ap.add_argument("--dump-dir", help="plan B: stream rows from dump tsvs")
    ap.add_argument("--start-key", type=int, default=0)
    ap.add_argument("--end-key", type=int, required=True)
    ap.add_argument("--ru-budget", type=float, default=400.0)
    ap.add_argument("--ru-per-row", type=float, default=11.5,
                    help="seed RU/row estimate (measured: "
                         "upsert-replace 11.6 at 400 RU/s saturation); "
                         "AIMD-corrected within [0.5, 2]x at runtime")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--batch", type=int, default=100)
    ap.add_argument("--no-egt", action="store_true",
                    help="per-row upserts instead of 100-row EGTs (Azurite)")
    ap.add_argument("--manifest")
    ap.add_argument("--resume-glob",
                    help="ALSO read completed chunks from every manifest "
                         "matching this glob (own manifest is always read). "
                         "Chunk ids are absolute key ranges on the global "
                         "100-key grid, so resume works across different "
                         "--start/--end segmentations and proc counts")
    ap.add_argument("--retries", type=int, default=50)
    ap.add_argument("--sample-verify", type=int, default=0)
    ap.add_argument("--create-table", action="store_true")
    args = ap.parse_args()

    assert PART_SZ % args.batch == 0, "batch must divide g_partition_sz"
    assert args.ref or args.dump_dir, "need --ref (plan A) or --dump-dir (B)"

    synth = learn(args.ref) if args.ref else None
    conn = open(args.conn_file).read().strip()
    svc = TableServiceClient.from_connection_string(conn)
    table = svc.get_table_client(args.table)
    if args.create_table:
        svc.create_table_if_not_exists(args.table)
        print("NOTE: data-plane-created tables default to 800 RU/s — "
              "run set_ru.py before large uploads", flush=True)

    manifest_path = args.manifest or "upload_manifest_%d_%d.txt" % (
        args.start_key, args.end_key)
    done = set()
    sources = [manifest_path]
    if args.resume_glob:
        sources += glob.glob(os.path.expanduser(args.resume_glob))
    for src in set(sources):
        if os.path.exists(src):
            with open(src) as mf:
                done.update(ln.strip() for ln in mf if ln.strip())
    if done:
        print("resume: %d chunks already uploaded (from %d manifests)"
              % (len(done), len(set(sources))), flush=True)
    manifest = open(manifest_path, "a")
    manifest_lock = threading.Lock()

    jobs = [(a, b) for (a, b) in
            chunk_ranges(args.start_key, args.end_key, args.batch)
            if ("%d-%d" % (a, b)) not in done]
    # deterministic shuffle: sequential key order put every in-flight EGT
    # of a proc on the SAME physical partition (one partition saturated,
    # the rest idle — cost the first 40k burst half its throughput);
    # shuffled chunks spread across all partitions. Resume is unaffected:
    # chunk ids live on the global 100-key grid regardless of order.
    random.Random(20260811).shuffle(jobs)
    total_rows = sum(b - a for a, b in jobs)
    print("uploading %d rows in %d chunks to %s (budget %.0f RU/s)"
          % (total_rows, len(jobs), args.table, args.ru_budget), flush=True)

    bucket = RUBucket(args.ru_budget)
    est = Estimator(args.ru_per_row)
    stats = Stats(total_rows, est)
    job_iter = iter(jobs)
    job_lock = threading.Lock()
    fail = []

    # plan B: pre-bucket the tsv stream into the same chunk grid
    dump_rows = None
    if args.dump_dir:
        dump_rows = {}
        for key, part, rk, data in tsv_rows(args.dump_dir):
            if args.start_key <= key < args.end_key:
                a = key - key % args.batch
                dump_rows.setdefault(a, []).append((key, part, rk, data))

    def rows_for(a, b):
        if synth:
            return [synth.gen(k) for k in range(a, b)]
        return [(part, rk, data)
                for _key, part, rk, data in sorted(dump_rows.get(a, []))]

    def send(entities):
        """One paced, retried write; returns nothing or raises."""
        bucket.acquire(est.get() * len(entities))
        for attempt in range(args.retries):
            try:
                if args.no_egt:
                    for e in entities:
                        table.upsert_entity(e, mode=UpdateMode.REPLACE)
                else:
                    table.submit_transaction(
                        [("upsert", e, {"mode": UpdateMode.REPLACE})
                         for e in entities])
                est.ok()
                return
            except Exception as exc:
                if is_throttle(exc):
                    with stats.lock:
                        stats.throttled += 1
                    est.throttled()
                if attempt == args.retries - 1:
                    raise
                time.sleep(min(10.0, 0.3 * (attempt + 1)))

    def push(worker_id):
        time.sleep(worker_id * 0.2)  # desync workers' first batches
        while not fail:
            with job_lock:
                try:
                    a, b = next(job_iter)
                except StopIteration:
                    return
            rows = rows_for(a, b)
            if not rows:
                continue
            try:
                send([{"PartitionKey": p, "RowKey": rk, "Data": d}
                      for p, rk, d in rows])
            except Exception as exc:
                print("worker %d FAILED on chunk %d-%d: %r"
                      % (worker_id, a, b, exc), flush=True)
                fail.append((a, b))
                return
            with manifest_lock:
                manifest.write("%d-%d\n" % (a, b))
                manifest.flush()
            stats.add(len(rows))

    threads = [threading.Thread(target=push, args=(i,), daemon=True)
               for i in range(args.workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    el = time.time() - stats.t0
    rate = stats.rows / max(el, 1e-9)
    print("UPLOAD %s: %d rows in %.1f min (%.0f rows/s), AIMD est "
          "%.1f RU/row, throughput-implied <= %.1f RU/row, 429s=%d"
          % ("FAILED" if fail else "DONE", stats.rows, el / 60, rate,
             est.get(), args.ru_budget / max(rate, 1e-9), stats.throttled),
          flush=True)
    if fail:
        sys.exit(1)

    if args.sample_verify and synth:
        rng = random.Random(42)
        n = args.sample_verify
        bad = 0
        for _ in range(n):
            k = rng.randrange(args.start_key, args.end_key)
            part, rk, data = synth.gen(k)
            ent = table.get_entity(part, rk)
            got = ent["Data"]
            got = bytes(got.value) if hasattr(got, "value") else bytes(got)
            if got != data:
                bad += 1
                print("VERIFY MISMATCH key %d" % k, flush=True)
        print("sample-verify: %d/%d OK" % (n - bad, n), flush=True)
        if bad:
            sys.exit(1)


if __name__ == "__main__":
    main()
