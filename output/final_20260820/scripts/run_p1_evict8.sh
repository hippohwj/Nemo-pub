#!/usr/bin/env bash
# 1% troubleshooting, experiment 2 (spin discriminator): identical to
# run_point.sh except NEMO_RUNTIME=90 and NEMO_EVICT_THDS=8 (8 evict + 16
# worker <= 16 vCPU, no oversubscription). Prediction if the no-sleep evict
# spin is real: per-read falls from ~41 ms back to the 6-9 ms band and
# throughput rises ~4-6x. RU policy untouched (write-split 40K).
set -u
export NEMO_RU_SETTLE=60
R=0.01
BASE=$HOME/nemo100g
exec 9>"$BASE/logs/fig5_grid.lock"
flock -n 9 || { echo "lock held -- refusing"; exit 1; }
pgrep -f "[t]pcc_cosmos_grid[.]sh|[E]xpTPCC" >/dev/null && { echo "busy -- refusing"; exit 1; }
LOG=$BASE/logs/p1evict8_$(date +%Y%m%d_%H%M%S).log
echo "start $(date -Is) ratio=$R evict_thds=8 -> $LOG"
env NEMO_MIX=std NEMO_WARMUP=30 NEMO_RUNTIME=90 NEMO_RU=40000 \
    NEMO_LOG=1 NEMO_DROP_LOG=1 NEMO_SCAN_CPP=1 \
    NEMO_DUMP_WRITES=0 NEMO_EGT=1 \
    NEMO_LOG_LAT_US=5860 \
    NEMO_RU_SPLIT=write NEMO_DIRTY_LAST=0 NEMO_FLUSH_OUTSIDE=1 \
    NEMO_EVICT_THDS=8 NEMO_LOG_WAIT=0 NEMO_LATCH_BENCH=0 \
    NEMO_WARMUP_BUDGET=10000 \
  bash "$HOME/NemoRun/experiments/tpcc_cosmos_grid.sh" "$R" nemo >> "$LOG" 2>&1
echo "rc=$? end $(date -Is)"
grep -E "^Throughput" "$LOG"
