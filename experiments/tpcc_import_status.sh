#!/usr/bin/env bash
# One-shot status of the TPC-C 34 W page import on VM-B. Run it locally, any
# time, from anywhere -- it holds no state and starts nothing:
#
#   experiments/tpcc_import_status.sh          # 15s rate sample
#   SAMPLE=0 experiments/tpcc_import_status.sh # instant, no rate/ETA
#   env: NEMO_VMB (user@host)  NEMO_KEY (ssh key)  SAMPLE (seconds)
#
# Progress needs a sub-shard signal. Neither the manifests nor the uploader's
# own report() give one: each worker takes a WHOLE ~9.9k-page shard and the 24
# workers of a process share one RU bucket, so every shard crawls at ~4.5
# pages/s and the first wave lands together ~36 min in -- both counters read 0
# until then. Bytes pushed (wchar from /proc, over the measured 16420 B a
# hex-encoded 8 KB page takes on the wire) is the only thing that moves
# continuously. It runs 5-10% high (TLS framing, retried batches).
#
# Accounting is per table and takes the LARGER of the two counters, capped at
# that table's own total -- see the note above the awk block for why anything
# else (summing them, or one global cap) misreports badly. Cross-check: the
# RU plan is work-proportional, so every table should finish at about the same
# time; pages_total / (table RU / ~110 RU-per-page) predicts the same figure.
#
# The byte counter is per-process and resets if the import is relaunched; after
# a relaunch it measures that launch only, while the manifest count is absolute
# (which is exactly why the larger of the two is the right answer).
set -uo pipefail

HOST=${NEMO_VMB:?set NEMO_VMB to <user>@<import-host>}
KEY=${NEMO_KEY:-$HOME/.ssh/id_rsa}
SAMPLE=${SAMPLE:-15}
TRIES=${TRIES:-3}

# Port 22 to this host goes through a proxy that drops connections at random
# ("Connection closed by ... port 22"), often twice in a row. Retry rather than
# hand back a bare error: the whole point of this script is that it can be run
# any time without thinking about it. Retrying is safe because the remote side
# only reads -- it starts nothing and writes nothing.
run_remote() {
  ssh -i "$KEY" -o BatchMode=yes -o ConnectTimeout=25 -o ServerAliveInterval=15 \
      -o ControlPath=none "$HOST" bash -s -- "$SAMPLE" <<'REMOTE'
SAMPLE=$1
BPP=16420              # bytes one hex-encoded 8 KB page occupies on the wire
BASE="${NEMO_BASE:-$HOME/nemo100g}"
MANI=$BASE/manifests/p34w_v2
SHARDS=$HOME/dumps/affected_p34w
DRVLOG=$BASE/logs/reimport_p34w_60k.log

echo "=== TPC-C 34W page dataset import · $(date -u '+%Y-%m-%d %H:%M:%S UTC') ==="

DRV=$(pgrep -c -f "[r]un_tpcc_import[.]sh" || true)
UPS=$(pgrep -c -f "[u]pload_pages[.]py" || true)

if [ "$DRV" -gt 0 ]; then
  STATE="uploading"
elif [ "$UPS" -gt 0 ]; then
  STATE="uploading (driver exited, uploaders still running)"
else
  STATE="no import process"
fi
printf 'phase     %s (driver %s, uploaders %s)\n' "$STATE" "$DRV" "$UPS"

# Progress is accounted PER TABLE, and the byte estimate is clamped to each
# table's own total. A single global clamp is useless near the end: wchar runs
# high (retried batches, TLS framing), so the leaders saturate the cap and hide
# the laggards -- it read a flat 100% while the manifests were at 51%.
# Per table:
#   settled = manifest pages (deduped; the three duplicate drivers each logged
#             the same shards). Exact, but it only moves when a WHOLE shard
#             lands, and a table whose shard count is close to its worker count
#             (-3 is 13 shards / 13 workers) shows ~0 until they all finish.
#   pushed  = wchar of that table's uploaders / BPP, the sub-shard signal.
# Capping per table (rather than once on the sum) keeps an overshooting table
# from masking one that is behind: a single global cap read a flat 100% while
# six tables were still uploading and the manifests stood at 51%.
sample_tbl_bytes() {  # -> "<table> <wchar>" per running uploader
  local p tbl v
  for p in $(pgrep -f "[u]pload_pages[.]py" 2>/dev/null); do
    tbl=$(tr '\0' '\n' < "/proc/$p/cmdline" 2>/dev/null |
          grep -A1 -x -- --table | tail -1)
    [ -n "$tbl" ] || continue
    v=$(awk '/^wchar/{print $2}' "/proc/$p/io" 2>/dev/null)
    echo "$tbl ${v:-0}"
  done
}

A=$(sample_tbl_bytes)
if [ "$SAMPLE" -gt 0 ] && [ "$UPS" -gt 0 ]; then
  sleep "$SAMPLE"
  B=$(sample_tbl_bytes)
else
  B=$A; SAMPLE=0
fi

{
  for d in "$SHARDS"/*/; do
    t=$(basename "$d")
    tot=$(awk -F'\t' '{n+=$2} END {print n+0}' "$d/index.txt" 2>/dev/null)
    set=$(cat "$MANI/${t}"_p*.txt 2>/dev/null | sort -u |
          awk '{s+=$2} END {print s+0}')
    echo "T $t $tot $set"
  done
  echo "$A" | awk '{print "A", $1, $2}'
  echo "$B" | awk '{print "B", $1, $2}'
} | awk -v bpp="$BPP" -v sample="$SAMPLE" '
  $1=="T" {tot[$2]=$3; set[$2]=$4; order[++n]=$2}
  $1=="A" {a[$2]+=$3}
  $1=="B" {b[$2]+=$3}
  END {
    for (i=1; i<=n; i++) {
      t = order[i]
      # NOT set[t] + pushed: wchar is cumulative since the uploader started,
      # so it already covers the shards that have since settled -- adding them
      # double counts and read 94.9% when the manifests were at 51%. settled
      # additionally covers shards a previous launch had finished, which wchar
      # cannot see, so the truth is whichever of the two is larger.
      pushed = int(b[t]/bpp)
      done_ = (set[t] > pushed ? set[t] : pushed)
      if (done_ > tot[t]) done_ = tot[t]
      T += tot[t]; D += done_; S += set[t]
      if (sample > 0) rate += (b[t]-a[t])/bpp/sample
      if (done_ < tot[t]) busy++
    }
    printf "progress  %'"'"'d / %'"'"'d pages (%.1f%%)   finalized %'"'"'d pages, %d tables pending\n",
           D, T, 100*D/T, S, busy
    if (rate > 0)
      printf "rate      %'"'"'d pages/s   ~%d min remaining\n", rate, ((T-D)/rate)/60 + 0.5
  }'

# RU: the standing rule is 400 unless a measurement or import is running.
# Report the ELEVATED tables on their own -- a flat aggregate over all 32
# tables reads 69500 during an import that provisions 60700, because the 22
# idle tables contribute their 400 floor each and hide the number that matters.
# The per-table cap (40000, the floor is max(400, GB, highest-ever/100)) is
# checked against the largest single table, never the sum.
RU=$(NEMO_AZURE_CONF=$BASE/configs/ifconfig_azure.txt \
     "$BASE/venv/bin/python" "${NEMO_REPO:-$HOME/NemoRun}/experiments/set_ru.py" list 2>/dev/null |
     grep -E "tpcc34w_123" || true)
if [ -n "$RU" ]; then
  echo "$RU" | awk -v drv="$DRV" '
    {for (i=1;i<=NF;i++) if ($i=="RU/s") {v=$(i-1); n++
       if (v != 400) {hot++; tot+=v; if (v>mx) {mx=v; who=$1}}}}
    END {
      if (hot==0) {printf "RU        all %d tables at 400 [ok]\n", n; exit}
      printf "RU        %d/%d tables bumped, %d RU/s total above floor, max %s = %d",
             hot, n, tot, who, mx
      print (mx > 40000 ? "  [warn] above the 40000 per-table cap" : "  (per-table cap 40000 ok)")
      if (drv==0) printf "  [warn] no import running but RU not restored -- check watchdog/ru_guard\n"
    }'
fi

W=$(pgrep -c -f "[r]u_watchdog[.]sh" || true)
G=$(crontab -l 2>/dev/null | grep -c "ru_guard" || true)
printf 'guards    %s watchdog(s), %s cron ru_guard entr(ies)\n' "$W" "$G"

# Only this launch's uploader logs. The directory also holds the 40k attempt
# and the three duplicate drivers that once ran against one RU budget; their 17
# "EGT failed 50 times" shards are real but already superseded -- a failed
# shard never reaches the manifest, so the current run re-uploads it. Grepping
# them every time would report permanent phantom failures.
STAMP=$(ls -t "$BASE"/logs/up_ptpcc34w_123-*.log 2>/dev/null | head -1 |
        sed 's/.*_\([0-9]\{8\}_[0-9]\{6\}\)\.log/\1/')
BAD=$(grep -ahE "FAILED|Traceback|REFUSING|INCOMPLETE" "$DRVLOG" \
      "$BASE"/logs/up_ptpcc34w_123-*_"$STAMP".log 2>/dev/null | tail -5 || true)
[ -n "$BAD" ] && { echo "--- anomalies ---"; echo "$BAD"; }

echo "--- tail of driver log ---"
tail -6 "$DRVLOG" 2>/dev/null

if [ "$DRV" -eq 0 ] && [ "$UPS" -eq 0 ]; then
  echo "--- import finished, next step ---"
  echo "  python3 verify_tpcc.py ...   # coverage / metadata / RU check over the merged manifests"
fi
REMOTE
}

for attempt in $(seq 1 "$TRIES"); do
  out=$(run_remote 2>&1)
  # A real reading always contains the progress line; anything else (dropped
  # connection, auth failure) is a transport error worth another attempt.
  case "$out" in
    *progress*) echo "$out"; exit 0 ;;
  esac
  [ "$attempt" -lt "$TRIES" ] && sleep 3
done
echo "cannot reach $HOST (tried $TRIES times). Last output:" >&2
echo "$out" >&2
exit 1
