#!/usr/bin/env python3
"""Re-shard engine dumps for parallel upload (page + TPC-C datasets).

Input:  <dump-dir>/dump_*.tsv.gz written by the IDataStore::Write hook
        (lines: tbl \t part \t rowkey \t hex | INT:<v>).
Output: <out-dir>/s<shard>.tsv.gz  one file per logical partition
        (shard = int(rowkey) // 10000 == the engine's PartitionKey), so
        every shard is EGT-safe (single partition) and the uploader can
        shuffle shards to spread load across physical partitions.
        <out-dir>/meta.tsv         the metadata rows (uploaded last)
        <out-dir>/index.txt        shard \t line-count (coverage source)

--by-table (TPC-C): the dump spans many tables whose partition numbers
COLLIDE, and an entity-group transaction may never span tables, so each
table gets its own sub-directory with its own s*/index.txt/meta.tsv triple
-- exactly the layout upload_pages.py expects, one instance per table.

Streaming with bounded memory: per-shard buffers, largest flushed as a
fresh gzip member (concatenated members zcat fine) when the pool exceeds
--buffer-mb. --unlink deletes each input file once consumed (VM-B runs
the 34 W imports with single-digit GB of headroom).
"""
import argparse
import glob
import gzip
import os
import shutil
from collections import defaultdict

PART_SZ = 10000


def flush(out_dir, shard, lines):
    with gzip.open(os.path.join(out_dir, "s%s.tsv.gz" % shard), "ab",
                   compresslevel=1) as f:
        f.write("".join(lines).encode())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dump-dir", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--buffer-mb", type=int, default=2048)
    ap.add_argument("--by-table", action="store_true",
                    help="one sub-directory per table (TPC-C: partition "
                         "numbers collide across tables and EGTs may not "
                         "span tables)")
    ap.add_argument("--unlink", action="store_true",
                    help="delete each dump file once consumed (bounds peak "
                         "disk to roughly one copy of the dump)")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    if args.by_table:
        for old in glob.glob(os.path.join(args.out_dir, "*", "s*.tsv.gz")):
            os.remove(old)
    else:
        for old in glob.glob(os.path.join(args.out_dir, "s*.tsv.gz")):
            os.remove(old)

    # keys are (tbl, shard) with --by-table, plain shard otherwise
    bufs = defaultdict(list)
    sizes = defaultdict(int)
    counts = defaultdict(int)
    meta = defaultdict(list)
    tables = set()
    pool = 0
    n_in = 0

    def dir_of(tbl):
        if not args.by_table:
            return args.out_dir
        d = os.path.join(args.out_dir, tbl)
        os.makedirs(d, exist_ok=True)
        return d

    def spill(key):
        tbl, shard = key if args.by_table else (None, key)
        flush(dir_of(tbl) if args.by_table else args.out_dir, shard,
              bufs[key])
        n = sizes[key]
        del bufs[key], sizes[key]
        return n

    files = sorted(glob.glob(os.path.join(args.dump_dir, "dump_*.tsv.gz")))
    assert files, "no dump_*.tsv.gz in %s" % args.dump_dir
    for path in files:
        with gzip.open(path, "rt") as fh:
            for ln in fh:
                n_in += 1
                tbl, part, rk = ln.split("\t", 3)[:3]
                tables.add(tbl)
                if part == "metadata":
                    meta[tbl].append(ln)
                    continue
                shard = int(rk) // PART_SZ
                assert str(shard) == part, "shard/part mismatch: %s" % ln[:80]
                key = (tbl, shard) if args.by_table else shard
                bufs[key].append(ln)
                sizes[key] += len(ln)
                counts[key] += 1
                pool += len(ln)
                if pool > args.buffer_mb * 1024 * 1024:
                    pool -= spill(max(sizes, key=sizes.get))
        if args.unlink:
            os.remove(path)
    for key in list(bufs):
        spill(key)

    n_shards = 0
    if args.by_table:
        for tbl in sorted(tables):
            d = dir_of(tbl)
            with open(os.path.join(d, "meta.tsv"), "w") as f:
                f.write("".join(meta.get(tbl, [])))
            with open(os.path.join(d, "index.txt"), "w") as f:
                for key in sorted(k for k in counts if k[0] == tbl):
                    f.write("%d\t%d\n" % (key[1], counts[key]))
                    n_shards += 1
    else:
        with open(os.path.join(args.out_dir, "meta.tsv"), "w") as f:
            for tbl in sorted(meta):
                f.write("".join(meta[tbl]))
        with open(os.path.join(args.out_dir, "index.txt"), "w") as f:
            for shard in sorted(counts):
                f.write("%d\t%d\n" % (shard, counts[shard]))
                n_shards += 1
    print("sharded %d lines -> %d shards in %d table dir(s) + %d metadata rows"
          % (n_in, n_shards, len(tables) if args.by_table else 1,
             sum(len(v) for v in meta.values())), flush=True)


if __name__ == "__main__":
    main()
