#!/usr/bin/env python3
"""Scan Nemo experiment outputs for per-op remote latencies.

Each line in output/*.txt is one run's JSON dict. Per-op average latency =
sum(time_ms) / count. Throttling and queueing only inflate these averages, so
the MINIMUM across runs approximates the uncontended per-op latency.
log_flush_latency has real percentiles (perc50) recorded per run.
"""
import glob
import json
import math
import os
import sys

OUT_DIR = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output")

ops = {
    "rd": ("remote_rd_time_ms", "remote_rds_"),
    "wr": ("remote_wr_time_ms", "remote_wrs_"),
    "rw": ("remote_rw_time_ms", "remote_rws_"),
    "scan": ("remote_scan_time_ms", "remote_scans_"),
}

# op -> list of (avg_us, count, file)
results = {k: [] for k in ops}
log_p50s = []  # (p50_us, file)

nfiles = nruns = 0
for path in sorted(glob.glob(os.path.join(OUT_DIR, "*.txt"))):
    nfiles += 1
    for line in open(path, errors="replace"):
        line = line.strip()
        if not line.startswith("{"):
            continue
        # some lines have trailing commas before }
        try:
            run = json.loads(line.replace(", }", "}").replace(",}", "}"))
        except json.JSONDecodeError:
            continue
        nruns += 1
        for op, (tkey, ckey) in ops.items():
            t, c = run.get(tkey), run.get(ckey)
            if t and c and c > 100:  # ignore tiny samples
                results[op].append((t * 1000.0 / c, c, os.path.basename(path)))
        p50 = run.get("log_flush_latency_perc50_us")
        if p50 and run.get("num_flushes_", 0) > 100:
            log_p50s.append((p50, os.path.basename(path)))

print(f"scanned {nfiles} files, {nruns} runs\n")
for op in ops:
    rs = sorted(results[op])
    if not rs:
        print(f"{op:5s}: no data")
        continue
    lo = rs[0]
    p10 = rs[max(0, int(len(rs) * 0.1) - 1)]
    med = rs[len(rs) // 2]
    print(f"{op:5s}: {len(rs):5d} runs | min {lo[0]:9.1f} us (n={lo[1]}, {lo[2]})")
    print(f"       p10 of runs {p10[0]:9.1f} us | median of runs {med[0]:9.1f} us")

if log_p50s:
    ls = sorted(log_p50s)
    print(f"\nlogfl: {len(ls):5d} runs | min p50 {ls[0][0]:9.1f} us ({ls[0][1]})")
    print(f"       median of per-run p50s {ls[len(ls)//2][0]:9.1f} us")
