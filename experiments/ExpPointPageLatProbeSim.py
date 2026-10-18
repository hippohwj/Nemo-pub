#!/usr/bin/env python3
# Page-read latency probe (verification for the sim-vs-Cosmos gap
# analysis): rerun the page-based baselines of the sim YCSB grid with the
# remote READ latency raised from the record-read floor (2800 us) to the
# page-read floor (4200 us — min per-read across 92/63 historical Cosmos
# PGBUF/HYBRBUF runs; records: 2810 us over 51 OBJBUF runs).
#
# Hypothesis under test: the sim grid's single-tier read latency under-charges
# page reads ~1.5x, which (together with the engine-era rds/txn drift)
# explains why sim speedups sit at roughly half the Cosmos ones. Pre-registered
# predictions (throughput = 16 workers / (a + rds_per_txn * rd_lat), a solved
# from the published 2800 us rows):
#   btree-ri  ro-25%: ~34,000 txn/60s   btree-ri ro-1%: ~9,400
#   btree-ri  ro-75%: ~98,700           twotree-ri ro-1%: ~12,000
# Acceptance: measured per-read ~4.2 ms; rds/txn unchanged vs the published
# grid (latency must not change cache behavior); throughput within ~10% of
# predictions.
#
# Everything except g_sim_rd_latency_us is IDENTICAL to
# ExpPointVaryBufForALLSim.py (in-process mode, read-only, RI variants only).
# Output goes to a separate file — never into the published grid.
#
# Run (phase per baseline; 2-Tree phase expects the metadata-only top-table
# snapshot swapped in, see the driver script):
#   NEMO_MEMCAP=32G NEMO_BASELINES=2 python3 ExpPointPageLatProbeSim.py
#   NEMO_MEMCAP=32G NEMO_BASELINES=3 python3 ExpPointPageLatProbeSim.py
from exp_helper import *
import sys

SIM_LATENCY = {
    # g_sim_rd_latency_us is set PER BASELINE below (two-tier read floors):
    # nemo/records 2800 us, page tables 4200 us
    "g_sim_wr_latency_us": 8000,   # unchanged: ro runs barely write; the
    "g_sim_scan_latency_us": 12000,  # page-WRITE floor (10.7 ms) rests on
    "g_sim_batch_latency_us": 5400,  # only 2 historical runs — not probed here
    "g_sim_log_latency_us": 5400,
}
# per-baseline read floor: records (OBJBUF) vs 8 KB pages (PGBUF/HYBRBUF)
RD_FLOOR_US = {1: 2800, 2: 4200, 3: 4200}

if __name__ == "__main__":
    cmake_dir = "../build/"
    os.chdir(cmake_dir)
    out_dir = "../output/"
    exp_name = "exp_point_buf_pagelat_sim"
    config = {
        "config_name": exp_name,
    }
    db_config = {
        "g_out_fname": out_dir + "%s.txt" % exp_name,
        "g_save_output": "true",
        "g_idx_btree_fanout": 250,
        "g_num_worker_threads": 16,
        "g_enable_group_commit": True,
        "g_commit_queue_limit": 100,
        "g_commit_group_sz": 0,
        "g_commit_pool_sz": 1,
        "g_log_freq_us": 100,
        "g_remote_req_retries": 3,
        "g_num_restore_thds": 8,
        "g_early_lock_release": True,
        "g_zipf_random_hotspots": True,
        "g_enable_latency_distr_log": False,
        "g_sim_snapshot_dir": "simstore_snapshots",
        "g_sim_scan_per_row_ns": 0,
        "g_sim_async_thds": 8,
        "g_evict_threshold": 0.995,
        "g_num_evict_thds": 32,
        "g_batch_eviction_period_ms": 10,
    }
    db_config.update(SIM_LATENCY)
    ycsb_config = {
        "num_rows_": 10240000,
        "runtime_": 60,
        "read_perc_": 1,
        "zipf_theta_": 0.9,
    }
    working_set = 12000000000  # in bytes
    executable = "ExpYCSB"

    baselines = [int(b) for b in
                 os.environ.get("NEMO_BASELINES", "2,3").split(",")]
    # 0.25 first: it is the smoke point — check its JSON (per-read ~4.2 ms,
    # rds/txn ~ the published grid's) before trusting the rest.
    # NEMO_BUF_RATIOS / NEMO_RETAIN override for the D6 ro-column completion
    # (narrowed scope: read-only theta=0.9 only).
    buf_ratios = [float(r) for r in os.environ.get(
        "NEMO_BUF_RATIOS", "0.25,0.01,0.1,0.5,0.75").split(",")]
    retain = os.environ.get("NEMO_RETAIN", "True") == "True"

    for baseline in baselines:
      for buf_sz in buf_ratios:
        for k in ("g_top_tree_buf_sz", "g_bottom_tree_buf_sz"):
            db_config.pop(k, None)  # no cross-baseline leakage
        if baseline == 1:
            # nemo CONTROL at its unchanged record-read floor: reproduces
            # the published grid numerator in the same session as the
            # page-latency rows, so the corrected speedups are same-batch
            db_config["g_index_type"] = "BTREE"
            db_config["g_buf_type"] = "OBJBUF"
            db_config["g_eviction_simple_clock"] = True
            db_config["g_batch_eviction"] = True
        elif baseline == 2:
            db_config["g_index_type"] = "BTREE"
            db_config["g_buf_type"] = "PGBUF"
            db_config["g_eviction_simple_clock"] = True
            db_config["g_batch_eviction"] = False
        elif baseline == 3:
            db_config["g_index_type"] = "TWOTREE"
            db_config["g_buf_type"] = "HYBRBUF"
            db_config["g_batch_eviction"] = False
            db_config["g_eviction_simple_clock"] = False
            total = round(working_set * buf_sz)
            bottom_size = 1000000000 if total > 2000000000 else total // 2
            db_config["g_top_tree_buf_sz"] = total - bottom_size
            db_config["g_bottom_tree_buf_sz"] = bottom_size
        else:
            sys.exit("Unknown baseline!")
        db_config["g_sim_rd_latency_us"] = RD_FLOOR_US[baseline]
        db_config["g_total_buf_sz"] = round(working_set * buf_sz)
        db_config["g_retain_idx_page"] = retain  # no-op for nemo
        trial_id = "pagelat{}-idxtype{}-buf{}-read1-zipf0.9-idxretain{}".format(
            RD_FLOOR_US[baseline], baseline, buf_sz, retain)
        config_file = set_config(config, db_config, ycsb_config,
                                 "configs/sample.cfg")
        res = try_exec(executable, config_file, trial_id)
        print(trial_id, "OK" if res else "FAILED", flush=True)
