#!/usr/bin/env bash
# page250-100g-0 import driver (R3) -- runs ON VM-C.
#
#   run_page_import.sh <total_ru> [procs=4] [duration_s=0]
#
# Same restore discipline as run_import.sh (record campaign): hard 40k
# cap, EXIT trap + signal->exit conversion + setsid ru_watchdog, timed
# stops via sequential polling (never wrap this script in `timeout`).
# Upload is shard-resumable: rerun the same command after any crash.
# R1 calibration: python EGT round-trips are client-latency-bound
# (~90 pages/s per 8-worker process), so R3 fans out procs x threads.
set -euo pipefail

RU_TOTAL=${1:?usage: run_page_import.sh <total_ru> [procs] [duration_s]}
PROCS=${2:-4}
DURATION=${3:-0}
BASE="${NEMO_BASE:-$HOME/nemo100g}"
PY="$BASE/venv/bin/python"
CONN="$BASE/configs/ifconfig_azure.txt"
TABLE="page250-100g-0"
SHARDS="$BASE/shards/100g"
MANIFEST="$BASE/manifests/pages_100g.txt"
STAMP=$(date +%Y%m%d_%H%M%S)

if [ "$RU_TOTAL" -gt 40000 ]; then
  echo "REFUSING: $RU_TOTAL > 40000 would permanently raise the RU floor" >&2
  exit 1
fi

PIDS=()
cleanup() {
  if [ "${#PIDS[@]}" -gt 0 ]; then
    kill "${PIDS[@]}" 2>/dev/null || true
    wait "${PIDS[@]}" 2>/dev/null || true
  fi
  echo "[r3] restoring 400 RU/s on $TABLE"
  NEMO_AZURE_CONF="$CONN" "$PY" "$BASE/scripts/set_ru.py" set 400 "$TABLE" \
    | tee "$BASE/logs/r3_ru_restore_$STAMP.txt" \
    || echo "**RU RESTORE FAILED -- run set_ru.py set 400 $TABLE manually!**"
}
trap cleanup EXIT
trap 'exit 143' HUP INT TERM

mkdir -p "$BASE/logs" "$BASE/manifests"
pkill -f "ru_watchdog[.]sh" 2>/dev/null || true
setsid bash "$BASE/scripts/ru_watchdog.sh" 3 "$TABLE" \
  "upload_pages[.]py|run_page_import[.]sh" > /dev/null 2>&1 < /dev/null &
echo "[r3] watchdog armed (logs/watchdog.log)"

"$PY" - <<PYEOF
from azure.data.tables import TableServiceClient
conn = open("$CONN").read().strip()
TableServiceClient.from_connection_string(conn).create_table_if_not_exists("$TABLE")
print("table $TABLE ensured")
PYEOF

echo "[r3] RU before:"
NEMO_AZURE_CONF="$CONN" "$PY" "$BASE/scripts/set_ru.py" list \
  | tee "$BASE/logs/r3_ru_before_$STAMP.txt"
echo "[r3] bumping $TABLE to $RU_TOTAL RU/s"
NEMO_AZURE_CONF="$CONN" "$PY" "$BASE/scripts/set_ru.py" set "$RU_TOTAL" "$TABLE"

if [ "$RU_TOTAL" -gt 10000 ]; then
  echo "[r3] waiting for the physical partition split (target >= 4 ranges)"
  for i in $(seq 1 90); do
    N=$(NEMO_AZURE_CONF="$CONN" "$PY" "$BASE/scripts/pkranges.py" "$TABLE" \
        2>/dev/null || echo 1)
    echo "  pkranges check $i: $N"
    [ "$N" -ge 4 ] && break
    sleep 60
  done
fi

PIDS=()
for p in $(seq 0 $((PROCS - 1))); do
  "$PY" "$BASE/scripts/upload_pages.py" \
    --conn-file "$CONN" --table "$TABLE" \
    --shard-dir "$SHARDS" --manifest "$BASE/manifests/pages_100g_p$p.txt" \
    --resume-glob "$BASE/manifests/pages_100g*.txt" \
    --num-procs "$PROCS" --proc-id "$p" \
    --ru-budget $((RU_TOTAL / PROCS)) --ru-per-page 85 \
    --workers 24 --batch 40 \
    > "$BASE/logs/upload_pages_100g_p${p}_$STAMP.log" 2>&1 &
  PIDS+=($!)
  echo "[r3] uploader proc $p pid ${PIDS[-1]}"
done

T0=$(date +%s)
STOPPED_EARLY=0
while :; do
  ALIVE=0
  for pid in "${PIDS[@]}"; do
    kill -0 "$pid" 2>/dev/null && ALIVE=1
  done
  [ "$ALIVE" -eq 0 ] && break
  if [ "$DURATION" -gt 0 ] && [ $(($(date +%s) - T0)) -ge "$DURATION" ]; then
    echo "[r3] duration ${DURATION}s reached -- stopping uploaders"
    STOPPED_EARLY=1
    kill "${PIDS[@]}" 2>/dev/null || true
    break
  fi
  sleep 20
done
FAIL=0
for pid in "${PIDS[@]}"; do wait "$pid" 2>/dev/null || FAIL=1; done

if [ "$STOPPED_EARLY" -eq 1 ]; then
  echo "[r3] timed stop; rerun the same command to resume"
  exit 0
fi
if [ "$FAIL" -ne 0 ]; then
  echo "[r3] an uploader exited nonzero -- rerun to resume" >&2
  exit 1
fi

echo "[r3] all procs done; finalize + sample-verify (RU still high)"
"$PY" "$BASE/scripts/upload_pages.py" \
  --conn-file "$CONN" --table "$TABLE" \
  --shard-dir "$SHARDS" --manifest "$MANIFEST" \
  --resume-glob "$BASE/manifests/pages_100g*.txt" \
  --ru-budget "$RU_TOTAL" --finalize --sample-verify 1000 \
  2>&1 | tee "$BASE/logs/r3_finalize_$STAMP.log"
echo "[r3] upload + finalize + sample-verify done"
# trap restores 400
