#!/usr/bin/env bash
# 2-Tree-RI rerun with the eviction fixes (ON VM-C): {1,10,25,50,75}% x
# 30s at 16K RU, scaled bottom rule, results to a NEW series file
# (exp_point_2tree_fixed marks the post-fix engine). Restore 400 after.
set -uo pipefail
BASE="${NEMO_BASE:-$HOME/nemo100g}"
PY=$BASE/venv/bin/python
CONN=$BASE/configs/ifconfig_azure.txt
cd "${NEMO_REPO:-$HOME/NemoRun}/build"
export LD_LIBRARY_PATH=${JEMALLOC_LIB:-$HOME/mydata/jemalloc/lib}
export XDG_RUNTIME_DIR=/run/user/$(id -u)
WS=117187500000
OUT="../output/exp_point_2tree_fixed_16k.txt"

restore_all() {
  NEMO_AZURE_CONF=$CONN "$PY" "$BASE/scripts/set_ru.py" set 400 "100g-0" \
    || echo "**RESTORE FAILED - set_ru.py set 400 100g-0 manually!**"
}
trap restore_all EXIT
trap 'exit 143' HUP INT TERM
pkill -f "ru_watchdog[.]sh" 2>/dev/null || true
setsid bash "$BASE/scripts/ru_watchdog.sh" 3 "100g-0" \
  "[E]xpYCSB|[p]3_2tree_fixed[.]sh" > /dev/null 2>&1 < /dev/null &
echo "[2tf] watchdog armed"
echo "[2tf] bumping page250-100g-0 to 16000"
NEMO_AZURE_CONF=$CONN "$PY" "$BASE/scripts/set_ru.py" set 16000 "page250-100g-0"

for R in 0.01 0.1 0.25 0.5 0.75; do
  TOTAL=$(python3 -c "print(round($WS*$R))")
  BOTTOM=$(python3 -c "t=$TOTAL; print(10000000000 if t > 20000000000 else t//2)")
  TOP=$((TOTAL - BOTTOM))
  NAME="fix2t_$R"
  cat > "configs/$NAME.cfg" <<EOF
{
    "config_name": "$NAME",
    "ycsb_config": {
        "warmup_time_": 10, "runtime_": 30, "num_rows_": 100000000,
        "read_perc_": 1, "zipf_theta_": 0.9
    },
    "db_config": {
        "g_index_type": "TWOTREE", "g_buf_type": "HYBRBUF",
        "g_retain_idx_page": true, "g_eviction_simple_clock": false,
        "g_batch_eviction": false,
        "g_top_tree_buf_sz": $TOP, "g_bottom_tree_buf_sz": $BOTTOM,
        "g_dataset_suffix": "", "g_idx_btree_fanout": 250,
        "g_early_lock_release": true, "g_save_output": "true",
        "g_out_fname": "$OUT",
        "g_enable_logging": false, "g_enable_group_commit": true,
        "g_check_loaded": false, "g_restore_from_remote": true,
        "g_load_to_remote_only": false, "g_commit_pool_sz": 1,
        "g_commit_queue_limit": 100, "g_commit_group_sz": 0,
        "g_log_freq_us": 100, "g_total_buf_sz": $TOTAL,
        "g_num_worker_threads": 16, "g_remote_req_retries": 2000,
        "g_num_restore_thds": 8, "g_zipf_random_hotspots": true,
        "g_evict_threshold": 0.995, "g_num_evict_thds": 32
    }
}
EOF
  MEMCAP=24G
  case "$R" in 0.25) MEMCAP=60G;; 0.5) MEMCAP=90G;; 0.75) MEMCAP=125G;; esac
  echo "=== [2tf] $NAME top=$TOP bottom=$BOTTOM start $(date +%H:%M:%S) ==="
  systemd-run --user --scope -q -p "MemoryMax=$MEMCAP" \
    ./ExpYCSB "configs/$NAME.cfg" > "${NEMO_REPO:-$HOME/NemoRun}/$NAME.log" 2>&1
  echo "[2tf] $NAME rc=$? end $(date +%H:%M:%S)"
  grep -a "Throughput" "${NEMO_REPO:-$HOME/NemoRun}/$NAME.log" | tail -1
done

echo "[2tf] restoring 400"
restore_all
NEMO_AZURE_CONF=$CONN "$PY" "$BASE/scripts/set_ru.py" list | grep "100g-0"
echo TWOTREE_FIXED_DONE
