#!/usr/bin/env bash
# Independent RU restore watchdog.
# Launched setsid-detached by run_import.sh; survives ANY fate of the
# import process tree (SIGKILL, ssh teardown, OOM). Waits until no
# uploader / driver is left, then forces the table back to 400 RU/s with
# readback-retry until confirmed. Exits after success.
#
#   ru_watchdog.sh [grace_checks=3] [table=obj-100g-0] [pgrep_pattern]
set -u

BASE="${NEMO_BASE:-$HOME/nemo100g}"
PY="$BASE/venv/bin/python"
CONN="$BASE/configs/ifconfig_azure.txt"
# take set_ru.py from THIS script's directory, not from the ~/nemo100g/scripts
# deployment copy: a stale copy with a hard-coded table name can dutifully
# restore somebody else's table while ours stays provisioned, and the
# last-resort restore must not depend on two directories agreeing.
SDIR="$(cd "$(dirname "$0")" && pwd)"
SET_RU="$SDIR/set_ru.py"
[ -f "$SET_RU" ] || SET_RU="$BASE/scripts/set_ru.py"
GRACE=${1:-3}
TABLE=${2:-obj-100g-0}
PATTERN=${3:-"upload_100g[.]py|run_import[.]sh"}

log() { echo "$(date +%H:%M:%S) $*" >> "$BASE/logs/watchdog.log"; }

log "watchdog armed (grace=$GRACE checks)"
idle=0
while true; do
  if pgrep -f "$PATTERN" > /dev/null; then
    idle=0
  else
    idle=$((idle + 1))
  fi
  if [ "$idle" -ge "$GRACE" ]; then
    break
  fi
  sleep 60
done

log "import tree gone; forcing 400 RU/s"
for attempt in $(seq 1 30); do
  OUT=$(NEMO_AZURE_CONF="$CONN" "$PY" "$SET_RU" set 400 "$TABLE" 2>&1)
  log "set attempt $attempt: $OUT"
  # $TABLE is a substring pattern covering many tables, so the readback is
  # many lines. The old check was `case "$READ" in *"manual 400 RU/s"*`, which
  # a 32-line list satisfies as soon as ONE table is at 400 -- it would declare
  # success with ORDER_LINE still at 25,100. Require instead that the list is
  # non-empty (an empty one means the list call itself failed, which must not
  # read as success) and that NO line is above 400.
  READ=$(NEMO_AZURE_CONF="$CONN" "$PY" "$SET_RU" list 2>/dev/null | grep "$TABLE")
  TOT=$(printf '%s\n' "$READ" | grep -c .)
  BAD=$(printf '%s\n' "$READ" | grep -v "manual 400 RU/s" | grep -c .)
  log "readback: $TOT tables, $BAD still above 400"
  if [ "$TOT" -gt 0 ] && [ "$BAD" -eq 0 ]; then
    log "confirmed all $TABLE* at 400 RU/s; watchdog done"; exit 0
  fi
  printf '%s\n' "$READ" | grep -v "manual 400 RU/s" | while read -r l; do log "  still high: $l"; done
  sleep 60
done
log "**WATCHDOG FAILED to restore 400 after 30 attempts**"
exit 1
