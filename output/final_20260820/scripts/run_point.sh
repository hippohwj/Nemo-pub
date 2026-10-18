#!/usr/bin/env bash
# One buffer ratio per invocation: ./run_point.sh 0.75
#
# Uniform 30 s warm-up, 240 s measured, guard 10000.
#
# Why one point per run. Points are not independent when several share a grid
# invocation: reset_districts rewinds D_NEXT_O_ID to 3000 for each point but
# leaves the ~9800 ORDER/ORDER_LINE rows per district the previous point wrote,
# so a later point spends its window rewriting keys that already exist. Measured
# on 2026-08-19: the 75 % point reads 3879.5 and 3893.5 when it runs FIRST
# (0.4 % apart, two separate runs) and 6055-6753 when it runs last -- a 1.6-1.7x
# order effect. Running one point per invocation gives every point the same
# position; it does not erase the accumulated rows, so the dataset still has to
# be treated as "saturated after the first run".
#
# Guard at 10000, not 9000: 30 s of warm-up spends ~9810 slots per district
# (measured 327/s), so 10000 leaves the warm-up untruncated and uniform across
# ratios while still stopping a runaway before the 12000 slots run out. The
# 240 s window then takes 1237 more at the 75 % point (3893 txn/s x 0.45 x 240
# / 340 districts), for 11047 of 12000 -- 8 % margin.
set -u
export NEMO_RU_SETTLE=60
R=${1:?usage: run_point.sh <ratio>}
BASE=$HOME/nemo100g
exec 9>"$BASE/logs/fig5_grid.lock"
flock -n 9 || { echo "lock held -- refusing"; exit 1; }
pgrep -f "[t]pcc_cosmos_grid[.]sh|[E]xpTPCC" >/dev/null && { echo "busy -- refusing"; exit 1; }
LOG=$BASE/logs/point_${R}_$(date +%Y%m%d_%H%M%S).log
echo "start $(date -Is) ratio=$R -> $LOG"
env NEMO_MIX=std NEMO_WARMUP=30 NEMO_RUNTIME=240 NEMO_RU=40000 \
    NEMO_LOG=1 NEMO_DROP_LOG=1 NEMO_SCAN_CPP=1 \
    NEMO_DUMP_WRITES=0 NEMO_EGT=1 \
    NEMO_LOG_LAT_US=5860 \
    NEMO_RU_SPLIT=write NEMO_DIRTY_LAST=0 NEMO_FLUSH_OUTSIDE=1 \
    NEMO_EVICT_THDS=32 NEMO_LOG_WAIT=0 NEMO_LATCH_BENCH=0 \
    NEMO_WARMUP_BUDGET=10000 \
  bash "$HOME/NemoRun/experiments/tpcc_cosmos_grid.sh" "$R" nemo >> "$LOG" 2>&1
echo "rc=$? end $(date -Is)"
grep -E "^Throughput" "$LOG"
P=$(ls -t $BASE/logs/engine_tpcc_cosmos_nemo_${R}_*.log 2>/dev/null | head -1)
[ -n "$P" ] && grep -hE "warm-up ended" "$P" | sed "s|\[INFO\] ||; s| (/home.*||"
