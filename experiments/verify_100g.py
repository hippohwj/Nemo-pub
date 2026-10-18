#!/usr/bin/env python3
"""Post-import verification for obj-100g-0 — run while RU is still bumped.

Deliberately NO full-table count (an O(N) scan over 100M rows costs tens of
millions of RUs). Instead:
  1. manifest coverage: the union of all uploaded chunks must be exactly
     [0, num-rows) with no gaps or overlaps
  2. sampled point reads (default 10,000 random keys, ~1 RU each) compared
     byte-for-byte against the synthesizer
  3. boundary reads: global first/last key + each --segment edge
  4. --write-count: upsert metadata/row_cnt_ (Int64) that the engine's
     restore path reads (same row upload_dump.py writes)

  verify_100g.py --conn-file F --ref ref_head.tsv \
                 --manifest-glob '~/nemo100g/manifests/m_*.txt' \
                 [--num-rows 100000000] [--samples 10000] [--write-count]
"""
import argparse
import glob
import os
import random
import sys
import time

from azure.data.tables import TableServiceClient, EntityProperty, EdmType

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from synth_ycsb import learn  # noqa: E402


def read_manifests(pattern):
    spans = []
    for path in sorted(glob.glob(os.path.expanduser(pattern))):
        with open(path) as fh:
            for ln in fh:
                ln = ln.strip()
                if ln:
                    a, b = ln.split("-")
                    spans.append((int(a), int(b)))
    return spans


def coverage_gaps(spans, num_rows):
    spans.sort()
    gaps, overlaps = [], []
    pos = 0
    for a, b in spans:
        if a > pos:
            gaps.append((pos, a))
        elif a < pos:
            overlaps.append((a, pos))
        pos = max(pos, b)
    if pos < num_rows:
        gaps.append((pos, num_rows))
    return gaps, overlaps


def check_key(table, synth, key):
    part, rk, want = synth.gen(key)
    ent = table.get_entity(part, rk)
    got = ent["Data"]
    got = bytes(got.value) if hasattr(got, "value") else bytes(got)
    return got == want


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--conn-file", required=True)
    ap.add_argument("--table", default="obj-100g-0")
    ap.add_argument("--ref", required=True)
    ap.add_argument("--manifest-glob", required=True)
    ap.add_argument("--num-rows", type=int, default=100000000)
    ap.add_argument("--samples", type=int, default=10000)
    ap.add_argument("--segments", type=int, default=4,
                    help="proc count of the import; edges get boundary reads")
    ap.add_argument("--write-count", action="store_true")
    ap.add_argument("--seed", type=int, default=20260811)
    args = ap.parse_args()

    synth = learn(args.ref)
    conn = open(args.conn_file).read().strip()
    table = TableServiceClient.from_connection_string(conn) \
        .get_table_client(args.table)
    failed = False

    # 1. manifest coverage
    spans = read_manifests(args.manifest_glob)
    rows = sum(b - a for a, b in spans)
    gaps, overlaps = coverage_gaps(spans, args.num_rows)
    print("manifests: %d chunks, %d rows" % (len(spans), rows))
    if gaps or overlaps:
        failed = True
        for g in gaps[:10]:
            print("  GAP: keys [%d, %d) never uploaded" % g)
        for o in overlaps[:10]:
            print("  OVERLAP: keys [%d, %d) uploaded twice (harmless "
                  "for upserts, listed for the record)" % o)
        failed = failed and bool(gaps)  # overlaps alone don't fail
    else:
        print("  coverage exact: [0, %d), no gaps" % args.num_rows)

    # 2. sampled point reads
    rng = random.Random(args.seed)
    bad = 0
    t0 = time.time()
    for i in range(args.samples):
        key = rng.randrange(0, args.num_rows)
        if not check_key(table, synth, key):
            bad += 1
            print("  MISMATCH key %d" % key)
        if (i + 1) % 2000 == 0:
            print("  sampled %d/%d (%.0f reads/s)"
                  % (i + 1, args.samples, (i + 1) / (time.time() - t0)),
                  flush=True)
    print("samples: %d/%d OK" % (args.samples - bad, args.samples))
    failed = failed or bad > 0

    # 3. boundaries
    seg = args.num_rows // args.segments
    edges = sorted({0, args.num_rows - 1} |
                   {i * seg for i in range(1, args.segments)} |
                   {i * seg - 1 for i in range(1, args.segments)})
    ebad = [k for k in edges if not check_key(table, synth, k)]
    print("boundaries: %d/%d OK %s" % (len(edges) - len(ebad), len(edges),
                                       ("FAILED: %s" % ebad) if ebad else ""))
    failed = failed or bool(ebad)

    # 4. metadata row
    if args.write_count and not failed:
        table.upsert_entity({"PartitionKey": "metadata", "RowKey": "row_cnt_",
                             "Data": EntityProperty(args.num_rows,
                                                    EdmType.INT64)})
        print("metadata row_cnt_ = %d written" % args.num_rows)
    elif args.write_count:
        print("SKIPPED metadata write: verification failed")

    print("VERIFY %s" % ("FAILED" if failed else "PASSED"))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
