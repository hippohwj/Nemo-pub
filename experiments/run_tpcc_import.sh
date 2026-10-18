#!/usr/bin/env bash
# TPC-C multi-table Cosmos import driver (34 W datasets) -- runs ON VM-B.
#
#   run_tpcc_import.sh <plan.tsv> <shards-root> <tag> [duration_s] [max_par]
#
# plan.tsv (from tpcc_ru_plan.py): table \t ru \t procs \t workers \t batch
#                                  \t ru_per_item
#
# Differences from the single-table 100g drivers (run_import.sh /
# run_page_import.sh), all forced by TPC-C having 11 (records) or 21
# (pages) Cosmos tables instead of one:
#   * RU is provisioned PER TABLE from the plan (set_ru.py sets every
#     matching table to the same value, so the split must be explicit)
#   * a freshly created table defaults to 800 RU/s -- set immediately
#   * only tables above 10k RU/s wait for the physical-partition split
#   * uploaders are scheduled across tables with a concurrency cap
# Unchanged: the three-layer RU restore (timed stop / EXIT trap / detached
# watchdog), shard-level resume, finalize-last metadata invariant.
set -euo pipefail

PLAN=${1:?usage: run_tpcc_import.sh <plan.tsv> <shards-root> <tag> [duration_s] [max_par]}
SHARDS=${2:?missing shards-root}
TAG=${3:?missing tag (e.g. otpcc34w)}
DURATION=${4:-0}
MAXPAR=${5:-16}

BASE="${NEMO_BASE:-$HOME/nemo100g}"
PY="$BASE/venv/bin/python"
CONN="$BASE/configs/ifconfig_azure.txt"
SCRIPTS="${NEMO_REPO:-$HOME/NemoRun}/experiments"
MANI="$BASE/manifests/$TAG"
LOGS="$BASE/logs"
STAMP=$(date +%Y%m%d_%H%M%S)
SAMPLES=${SAMPLES:-200}
mkdir -p "$MANI" "$LOGS"

# dataset prefix: every table is "<prefix>-<oid>", so one substring
# restores the whole dataset (set_ru.py matches on substring)
PREFIX=$(head -1 "$PLAN" | cut -f1 | sed 's/-[0-9]*$//')
[ -n "$PREFIX" ] || { echo "cannot derive dataset prefix from $PLAN" >&2; exit 1; }

TOTAL_RU=0
while IFS=$'\t' read -r tbl ru procs workers batch rupi; do
  [ -z "${tbl:-}" ] && continue
  if [ "$ru" -gt 40000 ]; then
    echo "REFUSING: $tbl at $ru > 40000 would permanently raise the RU floor" >&2
    exit 1
  fi
  TOTAL_RU=$((TOTAL_RU + ru))
done < "$PLAN"
echo "[imp] $PREFIX: $(wc -l < "$PLAN") tables, ${TOTAL_RU} RU/s aggregate (\$$(awk "BEGIN{printf \"%.2f\", $TOTAL_RU*0.008/100}")/h)"

# Cosmos locks a table's offer while a scale-up (and the partition split it
# triggers) is in flight: "There is another scale-up operation which is
# currently in progress" comes back 423 for MINUTES. Every RU change here
# therefore retries, and none of them may abort the driver (set -e would
# otherwise kill a run that is merely early).
ru_set_retry() {  # <table> <target-ru> [attempts]
  local tbl=$1 target=$2 tries=${3:-15} a
  for a in $(seq 1 "$tries"); do
    # --exact: "<dataset>-1" is a substring of "<dataset>-10" and of every
    # index-page table id starting with 1, so a substring set silently resets
    # them (cost us 45 min of a 400 RU/s index table in T5)
    if NEMO_AZURE_CONF="$CONN" "$PY" "$SCRIPTS/set_ru.py" set "$target" "$tbl" --exact \
         2>&1 | grep -qE "OK|already"; then
      return 0
    fi
    echo "  $tbl -> $target RU/s locked (scale in progress), retry $a/$tries"
    sleep 60
  done
  echo "  **$tbl could NOT be set to $target RU/s**"
  return 1
}

PIDS=()
cleanup() {
  if [ "${#PIDS[@]}" -gt 0 ]; then
    kill "${PIDS[@]}" 2>/dev/null || true
    wait "${PIDS[@]}" 2>/dev/null || true
  fi
  echo "[imp] restoring 400 RU/s on $PREFIX*"
  for a in 1 2 3 4 5 6 7 8 9 10; do
    NEMO_AZURE_CONF="$CONN" "$PY" "$SCRIPTS/set_ru.py" set 400 "$PREFIX" \
      >> "$LOGS/${TAG}_ru_restore_$STAMP.txt" 2>&1 || true
    if ! NEMO_AZURE_CONF="$CONN" "$PY" "$SCRIPTS/set_ru.py" list 2>/dev/null \
         | grep "$PREFIX" | grep -qv "manual 400 RU/s"; then
      echo "[imp] all $PREFIX* tables confirmed at 400 RU/s"
      return
    fi
    echo "[imp] restore attempt $a incomplete (scale lock), retrying"
    sleep 60
  done
  echo "**RU RESTORE INCOMPLETE -- run set_ru.py set 400 $PREFIX manually!**"
}
trap cleanup EXIT
trap 'exit 143' HUP INT TERM

# layer 3: detached watchdog. Bracket every word of the pattern or it
# matches its own argv and never fires.
pkill -f "ru_watchdog[.]sh" 2>/dev/null || true
setsid bash "$SCRIPTS/ru_watchdog.sh" 3 "$PREFIX" \
  "[u]pload_pages[.]py|[r]un_tpcc_import[.]sh" > /dev/null 2>&1 < /dev/null &
echo "[imp] watchdog armed for $PREFIX* (logs/watchdog.log)"

# one RU listing up front: tables already at their target are skipped, which
# matters because re-setting a table whose scale-up is still running is
# exactly what returns 423 Locked
NEMO_AZURE_CONF="$CONN" "$PY" "$SCRIPTS/set_ru.py" list > "$LOGS/${TAG}_ru_before_$STAMP.txt" 2>&1 || true

echo "[imp] creating tables and setting RU"
while IFS=$'\t' read -r tbl ru procs workers batch rupi; do
  [ -z "${tbl:-}" ] && continue
  "$PY" -c 'import sys
from azure.data.tables import TableServiceClient
conn = open(sys.argv[1]).read().strip()
TableServiceClient.from_connection_string(conn).create_table_if_not_exists(sys.argv[2])
print("  table %s ensured (data-plane default 800 RU/s)" % sys.argv[2])' \
    "$CONN" "$tbl"
  CUR=$(awk -v t="$tbl" '$1==t {print $3}' "$LOGS/${TAG}_ru_before_$STAMP.txt")
  if [ "${CUR:-}" = "$ru" ]; then
    echo "  $tbl already at $ru RU/s"
  else
    ru_set_retry "$tbl" "$ru" || true
  fi
done < "$PLAN"

# Pre-split wait, bounded. Cosmos splits an EMPTY table lazily: a fresh
# table provisioned at 25.9k RU/s can sit at 2 physical partitions for many
# minutes because splits follow storage growth (an already-populated table
# splits much sooner). It does split, just slowly, so bound the
# wait instead of blocking on a fixed target; the rest happens under load.
PRESPLIT_CHECKS=${PRESPLIT_CHECKS:-10}
echo "[imp] waiting for pre-splits (tables above 10k RU/s only, max ${PRESPLIT_CHECKS} min)"
while IFS=$'\t' read -r tbl ru procs workers batch rupi; do
  [ -z "${tbl:-}" ] && continue
  [ "$ru" -le 10000 ] && continue
  WANT=$(( (ru + 9999) / 10000 ))   # ~10k RU/s per physical partition
  for i in $(seq 1 "$PRESPLIT_CHECKS"); do
    # keep only the trailing integer: an older deployed pkranges.py prints
    # the ranges on stdout too, and `[ "$N" -ge 4 ]` then dies on the text
    N=$(NEMO_AZURE_CONF="$CONN" "$PY" "$SCRIPTS/pkranges.py" "$tbl" \
        2>/dev/null | grep -oE '[0-9]+' | tail -1)
    [ -n "$N" ] || N=1
    echo "  $tbl pkranges check $i: $N (want >= $WANT)"
    [ "$N" -ge "$WANT" ] && break
    [ "$i" -eq "$PRESPLIT_CHECKS" ] && \
      echo "  $tbl still at $N partitions -- starting anyway, Cosmos splits under load"
    sleep 60
  done
done < "$PLAN"

running() {
  local n=0 pid
  for pid in "${PIDS[@]:-}"; do
    [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null && n=$((n + 1))
  done
  echo "$n"
}

T0=$(date +%s)
STOPPED_EARLY=0
timed_out() {
  [ "$DURATION" -gt 0 ] && [ $(($(date +%s) - T0)) -ge "$DURATION" ]
}

echo "[imp] launching uploaders (max $MAXPAR concurrent processes)"
while IFS=$'\t' read -r tbl ru procs workers batch rupi; do
  [ -z "${tbl:-}" ] && continue
  for p in $(seq 0 $((procs - 1))); do
    while [ "$(running)" -ge "$MAXPAR" ]; do
      if timed_out; then STOPPED_EARLY=1; break 3; fi
      sleep 10
    done
    "$PY" "$SCRIPTS/upload_pages.py" \
      --conn-file "$CONN" --table "$tbl" \
      --shard-dir "$SHARDS/$tbl" \
      --manifest "$MANI/${tbl}_p$p.txt" \
      --resume-glob "$MANI/${tbl}_p*.txt" \
      --num-procs "$procs" --proc-id "$p" \
      --ru-budget $((ru / procs)) --ru-per-page "$rupi" \
      --workers "$workers" --batch "$batch" \
      > "$LOGS/up_${tbl}_p${p}_$STAMP.log" 2>&1 &
    PIDS+=($!)
    echo "  $tbl proc $p pid ${PIDS[-1]} (ru $((ru / procs)), $workers wrk)"
  done
done < "$PLAN"

while [ "$(running)" -gt 0 ]; do
  if timed_out; then
    echo "[imp] duration ${DURATION}s reached -- stopping uploaders"
    STOPPED_EARLY=1
    kill "${PIDS[@]}" 2>/dev/null || true
    break
  fi
  sleep 20
done
FAIL=0
for pid in "${PIDS[@]:-}"; do
  [ -n "$pid" ] || continue
  wait "$pid" 2>/dev/null || FAIL=1
done
PIDS=()

if [ "$STOPPED_EARLY" -eq 1 ]; then
  echo "[imp] timed stop; rerun the same command to resume"
  exit 0
fi
if [ "$FAIL" -ne 0 ]; then
  echo "[imp] an uploader exited nonzero -- rerun to resume" >&2
  exit 1
fi

echo "[imp] all shards done; per-table finalize + sample-verify (RU still high)"
while IFS=$'\t' read -r tbl ru procs workers batch rupi; do
  [ -z "${tbl:-}" ] && continue
  "$PY" "$SCRIPTS/upload_pages.py" \
    --conn-file "$CONN" --table "$tbl" \
    --shard-dir "$SHARDS/$tbl" \
    --manifest "$MANI/${tbl}_final.txt" \
    --resume-glob "$MANI/${tbl}_*.txt" \
    --ru-budget "$ru" --ru-per-page "$rupi" \
    --workers "$workers" --batch "$batch" \
    --finalize --sample-verify "$SAMPLES" \
    2>&1 | tee "$LOGS/final_${tbl}_$STAMP.log" | sed 's/^/  /'
done < "$PLAN"

echo "[imp] import + finalize + sample-verify done for $PREFIX"
# trap restores 400
