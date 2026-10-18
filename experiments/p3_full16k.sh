#!/usr/bin/env bash
# Full paper-grid completion round (runs ON VM-C): the missing
# {25,75,100}% x 3 systems x 30s at 16K RU/s, appended to the same
# series file as the {1,10,50}% baseline round. 2-Tree uses the scaled
# split (bottom = 10GB when total > 20GB). Phase-wise bumps with prompt
# restore + trap + watchdog on both 100g tables + final readback.
set -uo pipefail
BASE="${NEMO_BASE:-$HOME/nemo100g}"
PY=$BASE/venv/bin/python
CONN=$BASE/configs/ifconfig_azure.txt
cd "${NEMO_REPO:-$HOME/NemoRun}/build"
export LD_LIBRARY_PATH=${JEMALLOC_LIB:-$HOME/mydata/jemalloc/lib}
export XDG_RUNTIME_DIR=/run/user/$(id -u)
WS=117187500000
OUT="../output/exp_point_baseline30s_16k.txt"

restore_all() {
  NEMO_AZURE_CONF=$CONN "$PY" "$BASE/scripts/set_ru.py" set 400 "100g-0" \
    || echo "**RESTORE FAILED - set_ru.py set 400 100g-0 manually!**"
}
trap restore_all EXIT
trap 'exit 143' HUP INT TERM
pkill -f "ru_watchdog[.]sh" 2>/dev/null || true
setsid bash "$BASE/scripts/ru_watchdog.sh" 3 "100g-0" \
  "[E]xpYCSB|[p]3_full16k[.]sh" > /dev/null 2>&1 < /dev/null &
echo "[full16k] watchdog armed (both 100g tables)"

write_cfg() {  # $1 name  $2 ratio  $3 extra-db-json (no trailing comma)
  local BUF
  BUF=$(python3 -c "print(round($WS*$2))")
  cat > "configs/$1.cfg" <<EOF
{
    "config_name": "$1",
    "ycsb_config": {
        "warmup_time_": 10, "runtime_": 30, "num_rows_": 100000000,
        "read_perc_": 1, "zipf_theta_": 0.9
    },
    "db_config": {
        $3,
        "g_dataset_suffix": "", "g_idx_btree_fanout": 250,
        "g_early_lock_release": true, "g_save_output": "true",
        "g_out_fname": "$OUT",
        "g_enable_logging": false, "g_enable_group_commit": true,
        "g_check_loaded": false, "g_restore_from_remote": true,
        "g_load_to_remote_only": false, "g_commit_pool_sz": 1,
        "g_commit_queue_limit": 100, "g_commit_group_sz": 0,
        "g_log_freq_us": 100, "g_total_buf_sz": $BUF,
        "g_num_worker_threads": 16, "g_remote_req_retries": 2000,
        "g_num_restore_thds": 8, "g_zipf_random_hotspots": true,
        "g_evict_threshold": 0.995, "g_num_evict_thds": 32
    }
}
EOF
}

run_point() {  # $1 name  $2 memcap
  echo "=== [full16k] $1 start $(date +%H:%M:%S) ==="
  systemd-run --user --scope -q -p "MemoryMax=$2" \
    ./ExpYCSB "configs/$1.cfg" > "${NEMO_REPO:-$HOME/NemoRun}/$1.log" 2>&1
  echo "[full16k] $1 rc=$? end $(date +%H:%M:%S)"
  grep -a "Throughput" "${NEMO_REPO:-$HOME/NemoRun}/$1.log" | tail -1
}

memcap_for() {  # $1 ratio
  case "$1" in
    0.25) echo 60G;; 0.75) echo 125G;; *) echo 170G;;
  esac
}

echo "[full16k] phase 1: Nemo -- bumping obj-100g-0 to 16000"
NEMO_AZURE_CONF=$CONN "$PY" "$BASE/scripts/set_ru.py" set 16000 "obj-100g-0"
for R in 0.25 0.75 1; do
  write_cfg "f16k_nemo_$R" "$R" '"g_index_type": "BTREE", "g_buf_type": "OBJBUF",
        "g_retain_idx_page": false, "g_eviction_simple_clock": true,
        "g_batch_eviction": true, "g_batch_eviction_period_ms": 10'
  run_point "f16k_nemo_$R" "$(memcap_for $R)"
done
echo "[full16k] phase 1 done -- restoring 400"
restore_all

echo "[full16k] phase 2: 2-Tree + B+Tree -- bumping page250-100g-0 to 16000"
NEMO_AZURE_CONF=$CONN "$PY" "$BASE/scripts/set_ru.py" set 16000 "page250-100g-0"
for R in 0.25 0.75 1; do
  TOTAL=$(python3 -c "print(round($WS*$R))")
  BOTTOM=$(python3 -c "t=$TOTAL; print(10000000000 if t > 20000000000 else t//2)")
  TOP=$((TOTAL - BOTTOM))
  write_cfg "f16k_twotree_$R" "$R" "\"g_index_type\": \"TWOTREE\", \"g_buf_type\": \"HYBRBUF\",
        \"g_retain_idx_page\": true, \"g_eviction_simple_clock\": false,
        \"g_batch_eviction\": false,
        \"g_top_tree_buf_sz\": $TOP, \"g_bottom_tree_buf_sz\": $BOTTOM"
  run_point "f16k_twotree_$R" "$(memcap_for $R)"
done
for R in 0.25 0.75 1; do
  write_cfg "f16k_btree_$R" "$R" '"g_index_type": "BTREE", "g_buf_type": "PGBUF",
        "g_retain_idx_page": true, "g_eviction_simple_clock": true,
        "g_batch_eviction": false'
  run_point "f16k_btree_$R" "$(memcap_for $R)"
done
echo "[full16k] phase 2 done -- restoring 400"
restore_all

echo "[full16k] final readback:"
NEMO_AZURE_CONF=$CONN "$PY" "$BASE/scripts/set_ru.py" list
echo FULL16K_DONE
