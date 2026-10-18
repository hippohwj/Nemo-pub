#!/usr/bin/env python3
# Sim-store version of ExpPointVaryBufForALL.py: the paper's YCSB point
# experiment (Nemo / B+Tree-RI/EI / 2-Tree-RI/EI x read ratio x buffer ratio)
# on the simulated remote store — stable per-op latency, no request caps.
#
# Prerequisites (from the build directory, -DREMOTE_STORAGE=SIMULATED):
#   ./LoadYCSB configs/load_data_sim_10g.cfg    # obj-10g record dataset
#   ./LoadYCSB configs/load_pages_sim_10g.cfg   # page250-10g page dataset
# Then: python3 ../experiments/ExpPointVaryBufForALLSim.py
from exp_helper import *
import sys

# calibrated from 1145 historical Cosmos runs (experiments/
# CalibrateSimLatency.py): min across-run per-op averages = least-throttled
# estimate. Override via a single scale factor for latency-sensitivity sweeps.
LATENCY_SCALE = 1.0
SIM_LATENCY = {
    "g_sim_rd_latency_us": 2800,
    "g_sim_wr_latency_us": 8000,
    "g_sim_scan_latency_us": 12000,
    "g_sim_batch_latency_us": 5400,
    "g_sim_log_latency_us": 5400,
}

# NEMO_SIMNET_ADDR=127.0.0.1:34467 switches to the networked sim store
# exp_helper starts one fresh SimStoreServer per run and
# these recalibrated injections land the END-TO-END per-op
# latency on the in-process ACTUALS (rd 2808.6us etc.) under the grid's
# 16-worker load. The subtracted ~43us is loopback RTT + client wake-up
# latency measured inside the loaded engine — NOT the idle single-thread
# SimNetBench overhead (~100-140us; wake-ups cost more on an idle machine).
# Use NEMO_MEMCAP=16G so engine and server each get a 16G scope. Results go
# to a separate *_net.txt file — never mixed into the published grid.
SIMNET_ADDR = os.environ.get("NEMO_SIMNET_ADDR", "")
SIM_LATENCY_NET = {
    "g_sim_rd_latency_us": 2765,
    "g_sim_wr_latency_us": 7965,
    "g_sim_scan_latency_us": 11965,
    "g_sim_batch_latency_us": 5365,
    "g_sim_log_latency_us": 5365,
}

if __name__ == "__main__":
    cmake_dir = "../build/"
    os.chdir(cmake_dir)
    out_dir = "../output/"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    exp_name = "exp_point_buf_for_all_sim"
    if LATENCY_SCALE != 1.0:
        exp_name += "_lat%g" % LATENCY_SCALE
    if SIMNET_ADDR:
        exp_name += "_net"
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
        "g_retain_idx_page": False,
        "g_zipf_random_hotspots": True,
        "g_enable_latency_distr_log": False,
        "g_sim_snapshot_dir": "simstore_snapshots",
        "g_sim_scan_per_row_ns": 0,
        "g_sim_async_thds": 8,
        # OBJBUF background eviction triggers at the default threshold
        # (0.7): the headroom is part of Nemo's async batch-eviction design
        # (misses never block on eviction) and matches the TPC-C grids.
        # Do NOT set 1.0 (evict-at-full): every miss then waits inline for
        # single-slot eviction and throughput collapses ~25x;
        # PGBUF/HYBRBUF evict inline at full and ignore this.
        # Eviction provisioning: supply sized to the admission rate
        # (32 I/O-bound evictors; 5 stalled the 1% points ~2x), and the
        # watermark treated as a free-slot reaction buffer rather than a
        # utilization target — 0.7 with adequate supply discards 30% of
        # the cache and collapses hot-set points (ro-75% -59%). (32,
        # 0.995, 10 ms) beat every probed point on both benchmarks; do
        # not lower the threshold together with high supply.
        "g_evict_threshold": 0.995,
        "g_num_evict_thds": 32,
        "g_batch_eviction_period_ms": 10,
    }
    latencies = SIM_LATENCY_NET if SIMNET_ADDR else SIM_LATENCY
    for knob, us in latencies.items():
        db_config[knob] = int(us * LATENCY_SCALE)
    if SIMNET_ADDR:
        db_config["g_sim_server_addr"] = SIMNET_ADDR
    ycsb_config = {
        "num_rows_": 10240000,
        "runtime_": 60,
    }
    working_set = 12000000000 # in bytes
    executable = "ExpYCSB"
    # try_compile(executable)

    # buf sz, read ratio, zipf theta
    exps = []
    # 1 tuple 2 page 3 two-tree; NEMO_BASELINES=3 reruns a subset
    baselines = [int(b) for b in
                 os.environ.get("NEMO_BASELINES", "1,2,3").split(",")]
    for baseline in baselines:
        for read_perc in [1, 0.8]:
          for buf_sz in [0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 1]:
            for zipf in [0.9]:
              if baseline in [2, 3]:
                 #for page index or two-tree
                 for idx_retain in [True, False]:
                    exps.append([buf_sz, read_perc, zipf, baseline, idx_retain])
              else:
                 exps.append([buf_sz, read_perc, zipf, baseline])


    for exp in exps:
        max_attempts = 2
        attempts = 0
        baseline = exp[3]
        buf_sz = exp[0]
        read_perc = exp[1]
        zipf = exp[2]
        if len(exp) == 5:
          idx_retain = exp[4]
        else:
          idx_retain = False

        while attempts < max_attempts:
          if baseline == 1:
            db_config["g_index_type"] = "BTREE"
            db_config["g_buf_type"] = "OBJBUF"
            db_config["g_eviction_simple_clock"] = True
            # paper Algorithm 2 / TPC-C convention: batched eviction via
            # random tree traversal, NOT the record-level CLOCK ring that
            # batch=False falls back to (the paper's explicit non-goal)
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
            # the two-tree top buffer has no background evictors (simple-clock
            # waiters deadlock once it fills); use the inline EvictAndLoad
            # path, whose dirty-victim writeback goes through TwoTreeEviction
            db_config["g_eviction_simple_clock"] = False
            # paper convention (ExpPointTwoTreeVaryBuf.py): bottom tree gets a
            # fixed 1 GB, except at tiny ratios where it gets half the total
            total = round(working_set * buf_sz)
            bottom_size = 1000000000 if total > 2000000000 else total // 2
            db_config["g_top_tree_buf_sz"] = total - bottom_size
            db_config["g_bottom_tree_buf_sz"] = bottom_size
          else:
             sys.exit("Unknown baseline!")

          db_config["g_total_buf_sz"] = round(working_set * buf_sz)
          ycsb_config["read_perc_"] = read_perc
          ycsb_config["zipf_theta_"] = zipf
          db_config["g_retain_idx_page"] = idx_retain
          trial_id = "sim-idxtype{}-buf{}-read{}-zipf{}-idxretain{}".format(baseline, buf_sz, read_perc, zipf, idx_retain)
          config_file = set_config(config, db_config, ycsb_config,
                                   "configs/sample.cfg")
          res = try_exec(executable, config_file, trial_id)
          if res:
            break
          else:
            attempts += 1
        if res == False:
           print("failed")
        else:
           print("success after {} attempts".format(attempts))
        print(trial_id, flush=True)
