#!/usr/bin/env python3
"""Build the per-table RU / concurrency plan for a TPC-C Cosmos import.

Cosmos Table API bills throughput PER TABLE and set_ru.py sets every
matching table to the SAME value, so an aggregate budget has to be split
by hand.  Split it by work (rows x RU-per-row), then clamp:

  * >= 400   Cosmos per-table minimum
  * <= 40000 the 400 RU/s floor is lost permanently above this
             (floor = max(400, storageGB, highest-ever RU / 100))
  * multiple of 100 (manual throughput granularity)

Concurrency comes out of the same table: a worker claims one whole shard,
so a table can never use more workers than it has shards -- the page
dataset has entire tables with a single shard.

  tpcc_ru_plan.py --shards-root DIR --budget 40000 [--ru-per-item 6.5]
                  [--max-workers 24] [--items-per-proc 400000]

Prints a TSV plan (table, ru, procs, workers, batch, ru_per_item) for
run_tpcc_import.sh plus a human summary on stderr.
"""
import argparse
import glob
import os
import sys

MIN_RU = 400
MAX_RU = 40000          # never exceed: permanent floor damage
SPLIT_RU = 10000        # above this Cosmos adds physical partitions


def table_stats(root):
    """table -> (items, shards) from each sub-directory's index.txt."""
    out = {}
    for idx in sorted(glob.glob(os.path.join(root, "*", "index.txt"))):
        tbl = os.path.basename(os.path.dirname(idx))
        items = shards = 0
        with open(idx) as f:
            for ln in f:
                if ln.strip():
                    items += int(ln.split()[1])
                    shards += 1
        out[tbl] = (items, shards)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--shards-root", required=True)
    ap.add_argument("--budget", type=float, default=40000)
    ap.add_argument("--ru-per-item", type=float, default=6.5,
                    help="6.5 for ~100B TPC-C rows, 85 for 8.5KB pages "
                         "(AIMD corrects within [0.5,2]x at runtime)")
    ap.add_argument("--batch", type=int, default=100,
                    help="items per EGT: 100 rows / 40 pages")
    ap.add_argument("--max-workers", type=int, default=24)
    ap.add_argument("--items-per-proc", type=int, default=400000,
                    help="split a table across processes above this")
    args = ap.parse_args()

    stats = table_stats(args.shards_root)
    assert stats, "no <table>/index.txt under %s" % args.shards_root
    total_items = sum(n for n, _ in stats.values())

    # proportional split, then clamp; clamping only ever lowers the sum
    # (MIN_RU raises tiny tables, but those are worth far less than the
    # rounding slack on the big ones), so re-check the total at the end
    plan = []
    for tbl, (items, shards) in sorted(stats.items(),
                                       key=lambda kv: -kv[1][0]):
        share = args.budget * items / total_items
        ru = int(max(MIN_RU, min(MAX_RU, round(share / 100) * 100)))
        procs = max(1, min(4, -(-items // args.items_per_proc)))
        workers = max(1, min(args.max_workers, shards // procs or 1))
        plan.append((tbl, ru, procs, workers, items, shards))

    total_ru = sum(p[1] for p in plan)
    for tbl, ru, procs, workers, items, shards in plan:
        print("%s\t%d\t%d\t%d\t%d\t%g"
              % (tbl, ru, procs, workers, args.batch, args.ru_per_item))
        eta = items * args.ru_per_item / ru / 60
        print("  %-28s %10d items %5d shards -> %6d RU, %d proc x %d wrk, "
              "RU-bound ETA %.0f min" % (tbl, items, shards, ru, procs,
                                         workers, eta), file=sys.stderr)
    need = total_items * args.ru_per_item
    print("TOTAL %d items, %d tables, %d RU/s provisioned (budget %d), "
          "%.0f M RU of work, RU-bound ETA %.1f h, cost $%.2f/h"
          % (total_items, len(plan), total_ru, args.budget, need / 1e6,
             need / total_ru / 3600, total_ru * 0.008 / 100), file=sys.stderr)
    if total_ru > args.budget:
        print("NOTE: per-table minimums push the sum %d RU/s over the "
              "budget -- unavoidable with %d tables at 400 RU/s each."
              % (total_ru - args.budget, len(plan)), file=sys.stderr)
    hot = [p[0] for p in plan if p[1] > SPLIT_RU]
    print("pre-split wait needed for: %s" % (", ".join(hot) or "none"),
          file=sys.stderr)


if __name__ == "__main__":
    main()
