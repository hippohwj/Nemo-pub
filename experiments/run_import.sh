#!/usr/bin/env bash
# obj-100g-0 import driver — runs on the import host.
#
#   run_import.sh <total_ru> [procs=4] [end_key=100000000] [duration_s=0]
#
# Bumps the table to <total_ru> (hard cap 40000: above that the 400 RU/s
# floor is lost permanently via the highest-ever/100 rule), fans out
# upload_100g.py over contiguous key segments, runs sampled verification
# while RU is still high, and restores 400 RU/s via THREE independent
# layers (one trap alone is not enough):
#   1. duration_s: timed bursts stop via plain sequential code — no
#      signals involved (do NOT wrap this script in `timeout`)
#   2. EXIT trap: kills uploaders, restores 400 (Ctrl-C / errors)
#   3. ru_watchdog.sh: setsid-detached; after the import tree dies for
#      any reason (even SIGKILL) it forces 400 with readback-retry
# Rerun the same command after any crash: manifests make it resume.
set -euo pipefail

RU_TOTAL=${1:?usage: run_import.sh <total_ru> [procs] [end_key] [duration_s]}
PROCS=${2:-4}
NUM=${3:-100000000}
DURATION=${4:-0}
BASE="${NEMO_BASE:-$HOME/nemo100g}"
PY="$BASE/venv/bin/python"
CONN="$BASE/configs/ifconfig_azure.txt"
TABLE="obj-100g-0"
STAMP=$(date +%Y%m%d_%H%M%S)

if [ "$RU_TOTAL" -gt 40000 ]; then
  echo "REFUSING: $RU_TOTAL > 40000 would permanently raise the RU floor" >&2
  exit 1
fi

PIDS=()
cleanup() {
  # kill uploader children FIRST: orphans would keep hammering the table
  # after the RU drops back to 400 (429 crawl + open manifests)
  if [ "${#PIDS[@]}" -gt 0 ]; then
    kill "${PIDS[@]}" 2>/dev/null || true
    wait "${PIDS[@]}" 2>/dev/null || true
  fi
  echo "[run_import] restoring 400 RU/s on $TABLE"
  NEMO_AZURE_CONF="$CONN" "$PY" "$BASE/scripts/set_ru.py" set 400 "$TABLE" \
    | tee "$BASE/logs/ru_restore_$STAMP.txt" \
    || echo "**RU RESTORE FAILED — run set_ru.py set 400 $TABLE manually!**"
}
trap cleanup EXIT
# bash skips EXIT traps on fatal signals unless they are converted to exit
trap 'exit 143' HUP INT TERM

mkdir -p "$BASE/logs" "$BASE/manifests"
# independent restore watchdog: survives any fate of this process tree
pkill -f "ru_watchdog[.]sh" 2>/dev/null || true
setsid bash "$BASE/scripts/ru_watchdog.sh" > /dev/null 2>&1 < /dev/null &
echo "[run_import] watchdog armed (logs/watchdog.log)"
echo "[run_import] RU before:"
NEMO_AZURE_CONF="$CONN" "$PY" "$BASE/scripts/set_ru.py" list \
  | tee "$BASE/logs/ru_before_$STAMP.txt"

echo "[run_import] bumping $TABLE to $RU_TOTAL RU/s"
NEMO_AZURE_CONF="$CONN" "$PY" "$BASE/scripts/set_ru.py" set "$RU_TOTAL" "$TABLE"

SEG=$((NUM / PROCS))
for i in $(seq 0 $((PROCS - 1))); do
  A=$((i * SEG))
  if [ "$i" -eq $((PROCS - 1)) ]; then B=$NUM; else B=$((A + SEG)); fi
  "$PY" "$BASE/scripts/upload_100g.py" \
    --conn-file "$CONN" --table "$TABLE" \
    --ref "$BASE/ref/ref_head.tsv" \
    --start-key "$A" --end-key "$B" \
    --ru-budget $((RU_TOTAL / PROCS)) --workers 8 \
    --manifest "$BASE/manifests/m_${A}_${B}.txt" \
    --resume-glob "$BASE/manifests/*.txt" \
    > "$BASE/logs/up_${A}_${B}.log" 2>&1 &
  PIDS+=($!)
  echo "  proc $i: keys [$A, $B) budget $((RU_TOTAL / PROCS)) RU/s pid ${PIDS[-1]}"
done

# poll instead of blocking wait: a timed burst stops via sequential code,
# not signals (bash killed by SIGKILL skips EXIT traps)
T0=$(date +%s)
STOPPED_EARLY=0
while :; do
  ALIVE=0
  for p in "${PIDS[@]}"; do
    kill -0 "$p" 2>/dev/null && ALIVE=1
  done
  [ "$ALIVE" -eq 0 ] && break
  if [ "$DURATION" -gt 0 ] && [ $(($(date +%s) - T0)) -ge "$DURATION" ]; then
    echo "[run_import] duration ${DURATION}s reached — stopping uploaders"
    STOPPED_EARLY=1
    kill "${PIDS[@]}" 2>/dev/null || true
    break
  fi
  sleep 10
done
FAIL=0
for p in "${PIDS[@]}"; do wait "$p" 2>/dev/null || FAIL=1; done
if [ "$STOPPED_EARLY" -eq 1 ]; then
  ROWS=$(cat "$BASE"/manifests/*.txt 2>/dev/null |
         awk -F- '{s += $2 - $1} END {print s+0}')
  echo "[run_import] timed burst done: $ROWS rows total in manifests;"
  echo "  full verify skipped (partial coverage) — use burst_check.py"
  exit 0  # trap restores RU
fi
if [ "$FAIL" -ne 0 ]; then
  echo "[run_import] an uploader FAILED — see logs; rerun to resume" >&2
  exit 1
fi

echo "[run_import] verifying (RU still high)"
"$PY" "$BASE/scripts/verify_100g.py" \
  --conn-file "$CONN" --table "$TABLE" \
  --ref "$BASE/ref/ref_head.tsv" \
  --manifest-glob "$BASE/manifests/*.txt" \
  --num-rows "$NUM" --samples 10000 --segments "$PROCS" --write-count \
  | tee "$BASE/logs/verify_$STAMP.txt"
# trap now restores 400 RU/s
