#!/usr/bin/env bash
# Generate byte-exact reference slices of the obj-100g YCSB dataset via the
# C++ dump hook (g_dump_load_dir). These TSVs are the ground truth that
# synth_ycsb.py must reproduce byte-for-byte before any real upload runs
# (the synthesizer self-test gate).
#
# Usage: dump_ycsb_ref.sh [out_dir]   (default: experiments/ref_100g)
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
BUILD="$REPO/build"
OUT="${1:-$REPO/experiments/ref_100g}"
NUM_ROWS=100000000   # table name obj-100g-0 requires num_rows_/1e6 == 100

mkdir -p "$OUT"

dump_slice() {  # name start end
  local name=$1 start=$2 end=$3
  local work; work=$(mktemp -d)
  cat > "$work/load.cfg" <<EOF
{
    "config_name": "dump_ref_${name}",
    "ycsb_config": {
        "warmup_time_": 1,
        "runtime_": 1,
        "num_rows_": ${NUM_ROWS},
        "read_perc_": 0.5,
        "zipf_theta_": 0,
        "loading_startkey": ${start},
        "loading_endkey": ${end}
    },
    "db_config": {
        "g_save_output": false,
        "g_force_write": false,
        "g_enable_logging": true,
        "g_commit_group_sz": 0,
        "g_index_type": "REMOTE",
        "g_buf_type": "NOBUF",
        "g_restore_from_remote": false,
        "g_load_to_remote_only": true,
        "g_load_range": true,
        "g_num_load_thds": 1,
        "g_check_loaded": false,
        "g_partition_sz": 10000,
        "g_num_worker_threads": 16,
        "g_dataset_suffix": "",
        "g_dump_load_dir": "${work}"
    }
}
EOF
  (cd "$BUILD" && LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-$HOME/.local/lib} \
    systemd-run --user --scope -p MemoryMax=32G --quiet \
    ./LoadYCSB "$work/load.cfg" > "$work/load.log" 2>&1) \
    || { echo "FAIL $name; log:"; tail -20 "$work/load.log"; exit 1; }
  mv "$work/dump_thd0.tsv" "$OUT/ref_${name}.tsv"
  echo "ref_${name}.tsv: $(wc -l < "$OUT/ref_${name}.tsv") rows," \
       "$(wc -c < "$OUT/ref_${name}.tsv") bytes (keys ${start}..$((end-1)))"
  rm -rf "$work"
}

dump_slice head 0        1000
dump_slice mid  50000000 50001000
dump_slice tail 99999000 100000000

echo "reference slices in $OUT"
