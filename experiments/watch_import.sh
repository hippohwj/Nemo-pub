#!/usr/bin/env bash
# Aggregate progress of a running obj-100g import (run on VM-B, any time).
BASE="${NEMO_BASE:-$HOME/nemo100g}"
NUM=${1:-100000000}
while true; do
  DONE=0
  for m in "$BASE"/manifests/*.txt; do
    [ -f "$m" ] || continue
    n=$(awk -F- '{s += $2 - $1} END {print s+0}' "$m")
    DONE=$((DONE + n))
  done
  PCT=$(awk -v d="$DONE" -v n="$NUM" 'BEGIN {printf "%.2f", 100*d/n}')
  LAST=$(tail -qn 1 "$BASE"/logs/up_*.log 2>/dev/null | tail -1)
  printf '\r%s rows / %s (%s%%) | %s' "$DONE" "$NUM" "$PCT" "${LAST:0:80}"
  sleep 10
done
