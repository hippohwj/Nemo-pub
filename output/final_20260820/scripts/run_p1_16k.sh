#!/usr/bin/env bash
# 1% troubleshooting, experiment 1: flat 16K RU on every record table, evict
# threads kept at 8 (only the RU policy changes vs run_p1_evict8.sh). The
# evict8 run measured 41.0% of remote I/O as 429 backoff, 13837/14408 of it on
# otpcc34w_123-7 (ITEM, the one 400-RU floor table under the write split).
# Prediction: per-read ~6.5-7 ms, throughput ~180-210 unless evict-wait grows.
set -u
export NEMO_RU_SETTLE=60
R=0.01
BASE=$HOME/nemo100g
exec 9>"$BASE/logs/fig5_grid.lock"
flock -n 9 || { echo "lock held -- refusing"; exit 1; }
pgrep -f "[t]pcc_cosmos_grid[.]sh|[E]xpTPCC" >/dev/null && { echo "busy -- refusing"; exit 1; }
LOG=$BASE/logs/p1_16k_$(date +%Y%m%d_%H%M%S).log
echo "start $(date -Is) ratio=$R evict_thds=8 flat-16K -> $LOG"
OV="otpcc34w_123-0=16000,otpcc34w_123-1=16000,otpcc34w_123-2=16000,otpcc34w_123-3=16000,otpcc34w_123-4=16000,otpcc34w_123-5=16000,otpcc34w_123-6=16000,otpcc34w_123-7=16000,otpcc34w_123-8=16000,otpcc34w_123-9=16000,otpcc34w_123-10=16000"
env NEMO_MIX=std NEMO_WARMUP=30 NEMO_RUNTIME=90 NEMO_RU=176000 \
    NEMO_RU_OVERRIDE="$OV" \
    NEMO_LOG=1 NEMO_DROP_LOG=1 NEMO_SCAN_CPP=1 \
    NEMO_DUMP_WRITES=0 NEMO_EGT=1 \
    NEMO_LOG_LAT_US=5860 \
    NEMO_RU_SPLIT=write NEMO_DIRTY_LAST=0 NEMO_FLUSH_OUTSIDE=1 \
    NEMO_EVICT_THDS=8 NEMO_LOG_WAIT=0 NEMO_LATCH_BENCH=0 \
    NEMO_WARMUP_BUDGET=10000 \
  bash "$HOME/NemoRun/experiments/tpcc_cosmos_grid.sh" "$R" nemo >> "$LOG" 2>&1
echo "rc=$? end $(date -Is)"
grep -E "^Throughput" "$LOG"
