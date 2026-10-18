#!/usr/bin/env python3
# Cosmos-side version of the paper's YCSB point experiment (Nemo /
# B+Tree-RI/EI / 2-Tree-RI/EI x read ratio x buffer ratio), mirroring
# ExpPointVaryBufForALLSim.py run-for-run so the sim-vs-cloud comparison is
# same-engine, same-config (the pre-2026 Cosmos grid was
# taken on the old usage-count OBJBUF eviction and must not be mixed in).
#
# Prerequisites (from the build directory, -DREMOTE_STORAGE=AZURE_TABLE):
#   configs/ifconfig_azure.txt holds a real Cosmos Table API connection string
#   ./LoadYCSB configs/load_data_10g.cfg     # obj-10g record dataset
#   ./LoadYCSB configs/load_pages_10g.cfg    # page250-10g page dataset
# Then: python3 ../experiments/ExpPointVaryBufForALL.py
#
# Env knobs (all optional):
#   NEMO_BASELINES=1,2,3   subset of systems (1 nemo, 2 pgbuf, 3 two-tree)
#   NEMO_BUF_RATIOS=0.01,...  subset of buffer ratios
#   NEMO_READ_PERCS=1,0.8  subset of read ratios
#   NEMO_MEMCAP=32G        wrap each run in systemd-run MemoryMax (see
#                          exp_helper.try_exec); ro-100%+ OBJBUF points at
#                          10 GB scale are known to exceed 32 GB RSS
from exp_helper import *
import sys

if __name__ == "__main__":
    cmake_dir = "../build/"
    os.chdir(cmake_dir)
    out_dir = "../output/"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    exp_name = "exp_point_buf_for_all_cosmos"
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
        # Cosmos throttles at the provisioned RU/s; ops must retry through
        # 429 storms instead of failing the run (sim uses 3)
        "g_remote_req_retries": 2000,
        # namespace our tables away from the legacy ones on the shared
        # account (obj-10g_clv-0 etc.); NEMO_DATASET_SUFFIX= to disable
        "g_dataset_suffix": os.environ.get("NEMO_DATASET_SUFFIX", "_123"),
        "g_num_restore_thds": 8,
        "g_early_lock_release": True,
        "g_retain_idx_page": False,
        "g_zipf_random_hotspots": True,
        "g_enable_latency_distr_log": False,
        # OBJBUF eviction provisioning: identical to the sim grid and the
        # TPC-C grids (see ExpPointVaryBufForALLSim.py for the full
        # rationale). Supply sized to the admission rate; the watermark is
        # a free-slot reaction buffer, NOT a utilization target — do not
        # lower the threshold together with high supply, and do not set
        # 1.0 (evict-at-full collapses throughput ~25x).
        "g_evict_threshold": 0.995,
        "g_num_evict_thds": 32,
        "g_batch_eviction_period_ms": 10,
    }
    # 100g dataset: NEMO_NUM_ROWS=100000000
    # NEMO_WORKING_SET=117187500000 NEMO_RUNTIME=900 — defaults preserve
    # the original 10g grid behavior run-for-run
    ycsb_config = {
        "num_rows_": int(os.environ.get("NEMO_NUM_ROWS", 10240000)),
        "runtime_": int(os.environ.get("NEMO_RUNTIME", 60)),
    }
    working_set = int(os.environ.get("NEMO_WORKING_SET", 12000000000))
    executable = "ExpYCSB"
    # try_compile(executable)

    # buf sz, read ratio, zipf theta
    exps = []
    # 1 tuple 2 page 3 two-tree; NEMO_BASELINES=3 reruns a subset
    baselines = [int(b) for b in
                 os.environ.get("NEMO_BASELINES", "1,2,3").split(",")]
    buf_ratios = [float(r) for r in
                  os.environ.get("NEMO_BUF_RATIOS",
                                 "0.01,0.05,0.1,0.25,0.5,0.75,1").split(",")]
    read_percs = [float(r) for r in
                  os.environ.get("NEMO_READ_PERCS", "1,0.8").split(",")]
    # NEMO_IDX_RETAIN=True limits page/two-tree runs to the -RI variant
    # (the 100g series' scope); unset runs both RI and EI as before
    retain_env = os.environ.get("NEMO_IDX_RETAIN")
    retain_opts = [retain_env == "True"] if retain_env else [True, False]
    for baseline in baselines:
        for read_perc in read_percs:
          for buf_sz in buf_ratios:
            for zipf in [0.9]:
              if baseline in [2, 3]:
                 #for page index or two-tree
                 for idx_retain in retain_opts:
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
          # read-only points run with logging off (Cosmos probe convention:
          # avoids the per-run *-log table recreate at the 800 RU/s
          # data-plane default; semantically void for pure reads). Note:
          # the sim grid left the default (on) — documented config delta.
          db_config["g_enable_logging"] = (read_perc < 1)
          trial_id = "cosmos-idxtype{}-buf{}-read{}-zipf{}-idxretain{}".format(baseline, buf_sz, read_perc, zipf, idx_retain)
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
