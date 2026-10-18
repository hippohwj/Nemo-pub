#!/usr/bin/env python3
# Buffer-replacement microbenchmark, step 1: smoke-test the existing
# record-level CLOCK evictor (g_batch_eviction=false, g_eviction_simple_clock)
# against Nemo's batched random-traversal evictor (g_batch_eviction=true) on
# the 1 GB YCSB sim dataset (obj-1g-0), read-only, and report hit rate and
# throughput. Env: NEMO_POLICIES (nemo,clock), NEMO_RATIOS (0.10),
# NEMO_ZIPF (0.9), NEMO_RUNTIME (60), NEMO_WORKERS (16), NEMO_EVICT_THDS (32
# for nemo; clock uses 1 unless NEMO_CLOCK_THDS set), NEMO_READ (1.0).
from exp_helper import *
import sys, json

os.chdir("../build/")
out_dir = "../output/"
os.makedirs(out_dir, exist_ok=True)
POLICIES = os.environ.get("NEMO_POLICIES", "nemo,clock").split(",")
RATIOS = [float(x) for x in os.environ.get("NEMO_RATIOS", "0.10").split(",")]
ZIPF = float(os.environ.get("NEMO_ZIPF", "0.9"))
RUNTIME = int(os.environ.get("NEMO_RUNTIME", "60"))
WORKERS = int(os.environ.get("NEMO_WORKERS", "16"))
NEMO_THDS = int(os.environ.get("NEMO_EVICT_THDS", "32"))
CLOCK_THDS = int(os.environ.get("NEMO_CLOCK_THDS", "1"))
READ = float(os.environ.get("NEMO_READ", "1.0"))
# dataset: NEMO_RECSZ=1008 -> obj-1g-0 (1.024 M x 1 KB); NEMO_RECSZ=128 ->
# obj-10g-r128 (10.24 M x 128 B). Working set = rows x record size.
RECSZ = int(os.environ.get("NEMO_RECSZ", "1008"))
# NEMO_ROWS overrides the row count (default: the paper's 10.24 M for r<N>
# datasets, 1.024 M for the legacy obj-1g-0 1 KB dataset)
NUM_ROWS = int(os.environ.get("NEMO_ROWS", "1024000" if RECSZ == 1008 else "10240000"))
WORKING_SET = NUM_ROWS * (1000 if RECSZ == 1008 else RECSZ)
# per-record replacement metadata charged against the byte budget (equal-byte
# comparison): nemo 5 bits (~1 B), record-CLOCK 8 B ring pointer, LRU 16 B
META = {"nemo": 1, "clock": 8, "lru": 16}
CHARGE = os.environ.get("NEMO_CHARGE_META", "1") == "1"
OUT = out_dir + "exp_evict_policy_smoke.txt"

config = {"config_name": "exp_evict_policy_smoke"}
db_config = {
    "g_out_fname": OUT, "g_save_output": "true",
    "g_idx_btree_fanout": 250, "g_num_worker_threads": WORKERS,
    "g_enable_group_commit": True, "g_commit_queue_limit": 100,
    "g_commit_group_sz": 0, "g_commit_pool_sz": 1, "g_log_freq_us": 100,
    "g_remote_req_retries": 3, "g_num_restore_thds": 8,
    "g_early_lock_release": True, "g_retain_idx_page": False,
    "g_zipf_random_hotspots": True, "g_enable_latency_distr_log": False,
    "g_sim_snapshot_dir": "simstore_snapshots", "g_sim_scan_per_row_ns": 0,
    "g_sim_async_thds": 8,
    "g_sim_rd_latency_us": 2800, "g_sim_wr_latency_us": 8000,
    "g_sim_scan_latency_us": 12000, "g_sim_batch_latency_us": 5400,
    "g_sim_log_latency_us": 5400,
    "g_index_type": "BTREE", "g_buf_type": "OBJBUF",
    "g_evict_threshold": 0.995, "g_batch_eviction_period_ms": 10,
}
ycsb_config = {"num_rows_": NUM_ROWS, "runtime_": RUNTIME, "read_perc_": READ,
               "zipf_theta_": ZIPF}
db_config["g_record_size"] = RECSZ
db_config["g_evict_policy_bench"] = True

results = []
for pol in POLICIES:
    for ratio in RATIOS:
        db = dict(db_config)
        if pol == "nemo":
            db.update({"g_batch_eviction": True, "g_eviction_simple_clock": True,
                       "g_num_evict_thds": NEMO_THDS})
        elif pol == "clock":
            db.update({"g_batch_eviction": False, "g_eviction_simple_clock": True,
                       "g_evict_lru": False, "g_num_evict_thds": CLOCK_THDS})
        elif pol == "lru":
            db.update({"g_batch_eviction": False, "g_eviction_simple_clock": True,
                       "g_evict_lru": True, "g_num_evict_thds": CLOCK_THDS})
        else:
            sys.exit("unknown policy " + pol)
        db["g_evict_meta_bytes"] = META[pol] if CHARGE else 0
        db["g_total_buf_sz"] = round(WORKING_SET * ratio)
        trial = "evict-%s-r%d-rows%d-buf%g-zipf%g-read%g-meta%d" % (pol, RECSZ, NUM_ROWS, ratio, ZIPF, READ, db["g_evict_meta_bytes"])
        cfg = set_config(config, db, ycsb_config, "configs/sample.cfg")
        print("=== running", trial, flush=True)
        ok = try_exec("ExpYCSB", cfg, trial)
        results.append((trial, ok))
        print("===", trial, "OK" if ok else "FAILED", flush=True)

rows = [json.loads(l) for l in open(OUT) if l.strip()][-len(results):]
print("\nSUMMARY (policy, buf, tput, hit, rds/txn, avail-slots-wait ms/txn, evict thds):")
for r, (trial, ok) in zip(rows, results):
    t = float(r.get("txn_cnt_", 1)) or 1
    if str(r.get("g_batch_eviction")).lower() == "true": pol = "nemo"
    elif str(r.get("g_evict_lru")).lower() == "true": pol = "lru"
    else: pol = "clock"
    ev = float(r.get("evict_records_", 0)); tasks = float(r.get("evict_tasks_", 0)) or 1
    print("  %-6s buf=%5.1f%%  tput=%9.1f  hit=%.4f  rds/txn=%.3f  await_evict=%.3fms  evict/s=%9.0f  rec/task=%6.1f  us/task=%7.1f  steps/victim=%6.2f  lockwait=%7.1fms  thds=%s  %s" % (
        pol, 100.0 * r["g_total_buf_sz"] / WORKING_SET, r["throughput_txn_per_sec"],
        r.get("tuple_hit_rate_", 0), r["remote_rds_"] / t, r.get("rw_await_evict_time_ms", 0) / t,
        ev / r["runtime_"], ev / tasks, r.get("evict_task_ns_", 0) / 1000.0 / tasks,
        r.get("evict_hand_steps_", 0) / (ev or 1), r.get("evict_lock_wait_ns_", 0) / 1e6,
        r.get("g_num_evict_thds"), "OK" if ok else "FAILED"))
sys.exit(0 if all(ok for _, ok in results) else 1)
