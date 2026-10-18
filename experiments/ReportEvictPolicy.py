#!/usr/bin/env python3
# Report for the buffer-replacement microbenchmark.
# Reads output/exp_evict_policy_smoke.txt (one JSON row per run) and prints:
#   panel (a): hit rate (and tput) vs buffer ratio per policy and skew, at the
#              standard 16 workers, equal-BYTE budgets (per-policy metadata
#              charged);
#   panel (b): eviction cost per policy at a chosen point: evict/s, us per
#              task, us per record, hand-steps/victim, lock wait, LRU moves.
# Usage: python3 ReportEvictPolicy.py [--recsz 128] [--workers 16] [--runtime 60]
import json, sys, argparse, os
from collections import defaultdict

ap = argparse.ArgumentParser()
ap.add_argument("--file", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "../output/exp_evict_policy_smoke.txt"))
ap.add_argument("--recsz", type=int, default=128)
ap.add_argument("--workers", type=int, default=16)
ap.add_argument("--runtime", type=int, default=60)
a = ap.parse_args()

rows = [json.loads(l) for l in open(a.file) if l.strip()]
def pol(r):
    if str(r.get("g_batch_eviction")).lower() == "true": return "nemo"
    if str(r.get("g_evict_lru")).lower() == "true": return "lru"
    return "clock"
sel = [r for r in rows if r.get("g_record_size") == a.recsz and r.get("g_num_worker_threads") == a.workers
       and r.get("runtime_") == a.runtime and str(r.get("g_evict_policy_bench")).lower() == "true"]
NUM_ROWS = 1024000 if a.recsz == 1008 else 10240000
WS = NUM_ROWS * (1000 if a.recsz == 1008 else a.recsz)
# newest row wins per (policy, ratio, zipf, read)
latest = {}
for r in sel:
    k = (pol(r), round(100.0 * r["g_total_buf_sz"] / WS, 2), r.get("zipf_theta_"), r.get("read_perc_"))
    latest[k] = r
print("panel (a): equal-byte hit rate | tput  (recsz=%d, workers=%d, runtime=%d)" % (a.recsz, a.workers, a.runtime))
for z in sorted({k[2] for k in latest}, reverse=True):
    ratios = sorted({k[1] for k in latest if k[2] == z})
    print("\n  zipf=%s" % z)
    print("  %-6s " % "policy" + "".join("%18s" % ("%g%%" % r) for r in ratios))
    for p in ["nemo", "clock", "lru"]:
        line = "  %-6s " % p
        for rt in ratios:
            r = latest.get((p, rt, z, 1.0)) or latest.get((p, rt, z, 1))
            line += "%18s" % ("%.4f | %6.0f" % (r.get("tuple_hit_rate_", 0), r["throughput_txn_per_sec"]) if r else "-")
        print(line)
    # deltas vs nemo
    print("  %-6s " % "d(hit)" + "".join("%18s" % ("c%+.3f l%+.3f" % (
        (latest.get(("clock", rt, z, 1.0)) or {}).get("tuple_hit_rate_", 0) - (latest.get(("nemo", rt, z, 1.0)) or {}).get("tuple_hit_rate_", 0),
        (latest.get(("lru", rt, z, 1.0)) or {}).get("tuple_hit_rate_", 0) - (latest.get(("nemo", rt, z, 1.0)) or {}).get("tuple_hit_rate_", 0)))
        for rt in ratios))

print("\npanel (b): eviction cost per policy (all rows for recsz, any workers)")
print("  %-6s %4s %6s %8s %9s %10s %9s %12s %10s %8s" % ("policy", "wk", "buf%", "tput", "evict/s", "rec/task", "us/task", "us/record", "steps/vic", "lockwait"))
for r in sorted([r for r in rows if r.get("g_record_size") == a.recsz and str(r.get("g_evict_policy_bench")).lower() == "true"],
                key=lambda r: (r.get("g_num_worker_threads"), pol(r), r["g_total_buf_sz"])):
    ev = float(r.get("evict_records_", 0)); tasks = float(r.get("evict_tasks_", 0)) or 1
    print("  %-6s %4d %6.1f %8.0f %9.0f %10.1f %9.2f %12.3f %10.2f %7.1fms" % (
        pol(r), r.get("g_num_worker_threads"), 100.0 * r["g_total_buf_sz"] / WS, r["throughput_txn_per_sec"],
        ev / r["runtime_"], ev / tasks, r.get("evict_task_ns_", 0) / 1000.0 / tasks,
        r.get("evict_task_ns_", 0) / 1000.0 / (ev or 1), r.get("evict_hand_steps_", 0) / (ev or 1),
        r.get("evict_lock_wait_ns_", 0) / 1e6))
