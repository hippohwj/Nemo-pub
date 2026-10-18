#!/usr/bin/env bash
# TPC-C 34 W grid against real Cosmos -- Nemo (OBJBUF) vs B+Tree (PGBUF).
# Runs ON VM-B from ~/NemoRun/build (the AZURE_TABLE build).
#
#   tpcc_cosmos_grid.sh [ratios] [systems]
#     ratios   comma list, default "0.01,0.10,0.25,0.50,0.75" -- the TPC-C
#              x-axis stops at 75 %. STANDING RULE: the 100 % point is never
#              run. At a full-size buffer the generative warm-up advances
#              nearly all of a district's order slots, and the measurement
#              window then hits the slot-exhaustion guard in TPCCTxn.cpp.
#              Giving that one point a shorter warm-up does run, but it makes
#              the x-axis inconsistent. The page baseline's 100 % point does
#              complete (PGBUF has no generative warm-up) -- it is dropped
#              too, because both systems must share one x-axis.
#     systems  comma list of {nemo,btree-ri,btree-ei}, default "nemo,btree-ri"
#   env: NEMO_MIX=std|ro  NEMO_RUNTIME  NEMO_WARMUP  NEMO_RU
#        NEMO_LOG=0|1  NEMO_LOG_RU  NEMO_DROP_LOG=0|1
#        NEMO_PGBUF_FILL=0|1  NEMO_PAGE_RD_MS  NEMO_WSET_FRAMES
#        NEMO_WARM_SAFETY  NEMO_SLOT_B  NEMO_WORKERS
#
# Logging: the standard mix MUST run with it on. g_enable_logging=false skips
# the log store entirely and ITxn then null-derefs on the first write commit.
# Each dataset gets its own "<ds>-log"
# table; we pre-create it so its RU is ours to set (the engine would leave it
# at the data-plane default of 800), and drop it when the phase ends -- the
# log store never replays, so a stale log table is pure cost.
#
# Each system reads its OWN dataset, so RU is bumped per PHASE and restored
# the moment that phase ends (standing rule: tables sit at 400 unless a
# measurement is actively running):
#     nemo      -> otpcc34w_123-*  (11 record tables)
#     btree-ri  -> ptpcc34w_123-*  (21 page tables)
# The per-table split of the aggregate budget comes from the import plans
# (work-proportional, already clamped to [400, 40000]); see tpcc_ru_plan.py.
#
# Restore layers, same as the import driver: per-phase restore, EXIT trap,
# signal->exit conversion, detached watchdog covering BOTH datasets
# ("tpcc34w_123" matches all 32 tables), final full readback.
set -uo pipefail

RATIOS=${1:-0.01,0.10,0.25,0.50,0.75}
# the standing rule above, enforced: a default change alone does not stop
# someone passing 1.00 on the command line six weeks from now
case ",$RATIOS," in
  *,1.00,*|*,1,*|*,1.0,*)
    echo "REFUSING: the TPC-C 100 % buffer point is not run (see the header)." >&2
    echo "  Override deliberately with NEMO_ALLOW_FULL_BUF=1 if you accept the" >&2
    echo "  slot-exhaustion crash and the inconsistent x-axis." >&2
    [ "${NEMO_ALLOW_FULL_BUF:-0}" = 1 ] || exit 1 ;;
esac
SYSTEMS=${2:-nemo,btree-ri}
MIX=${NEMO_MIX:-std}
RUNTIME=${NEMO_RUNTIME:-300}
WARMUP=${NEMO_WARMUP:-60}
RU_BUDGET=${NEMO_RU:-16000}
ENABLE_LOG=${NEMO_LOG:-1}
LOG_RU=${NEMO_LOG_RU:-2000}
DROP_LOG=${NEMO_DROP_LOG:-1}
WORKERS=${NEMO_WORKERS:-16}
# NEMO_EVICT_BENCH=1 adds g_evict_policy_bench to the Nemo phase. It reports
# evict_records_/evict_tasks_/evict_task_ns_ -- the only account of the
# eviction write-back path, which is otherwise invisible: EvictRandomLeafNode
# flushes through WriteBatchAsyncCallback, and neither that nor WriteBatch
# increments any counter, not even remote_io_time_. The 34 W Cosmos grid
# measured remote_wrs_ = remote_rws_ = 0 with per-transaction latency scaling
# as (dirty rows) x ~10-20 ms, i.e. the cost is all on that unaccounted path.
# NOT observation-only: under this flag GetAllocated()/ReserveSlots stop
# charging index-node bytes against the record budget (ObjectBufferManager.h),
# so the effective buffer is larger and throughput is NOT comparable to a run
# with the flag off. Compare objbuf_resident_records_, not the budget.
EVICT_BENCH=${NEMO_EVICT_BENCH:-0}
EVICT_BENCH_KNOB=""
[ "$EVICT_BENCH" = 1 ] && EVICT_BENCH_KNOB=', "g_evict_policy_bench": true'

# NEMO_WR_FANOUT=k issues the rows of one eviction flush group k-ways
# concurrently (AzureTableClient::BatchStoreSync). k=1 is the original serial
# loop; the 34 W grid measured 110 rows x 20.1 ms = 2.2 s per eviction task on
# it, with every worker needing a slot queued behind that.
# NEMO_DIRTY_LAST=1 inverts the two-tier eviction so COLD CLEAN rows go
# before cold dirty ones (IndexBTreeObject::FindEvictionCandidates). This is
# the "keep the writes in memory" lever: the page baseline reaches 0.002
# remote writes per transaction at 75 % buffer because its clock spares
# dirty pages, while Nemo evicts them by preference and pays 229 RU/txn.
DIRTY_LAST=${NEMO_DIRTY_LAST:-0}
DIRTY_LAST_KNOB=""
[ "$DIRTY_LAST" = 1 ] && DIRTY_LAST_KNOB=', "g_evict_dirty_last": true'


# NEMO_EVICT_THDS overrides g_num_evict_thds for the Nemo phase. The
# eviction write-back is one row in flight per evictor thread
# (AzureTableClient::BatchStoreSync is a serial loop), so the thread count
# IS the write concurrency. Fanning out WITHIN a flush group does not
# help -- a leaf is contiguous in key space, so its rows share one
# partition and Cosmos serialises them; more threads spread the writes
# over more leaves, hence more partitions.
# NEMO_LOG_WAIT=0 sets g_enable_log=false. That knob has exactly one use in
# the tree -- the gate in ILogStore::WaitForFlush -- so commits stop waiting
# for their LSN to be flushed while the log store itself still exists and
# still writes (unlike g_enable_logging=false, which removes the store and
# null-derefs on the first write commit). DIAGNOSTIC ONLY: a commit that
# does not wait for its log is not durable, so no published number may come
# from a run with this off. It exists to size the log path
# (throughput = flushes/s x txns/flush when commits wait on the log).
# NEMO_LATCH_BENCH=1 turns on g_latch_bench: per-acquisition wait time for
# both modes plus EX hold time, measured only in the window after warm-up.
# Latch waits land in no other counter (cc_time_ covers the lock table,
# not B-tree node latches).
# NEMO_FLUSH_OUTSIDE=1 sets g_evict_flush_outside_latch: the eviction
# write-back is issued after the leaf/parent latches are dropped instead of
# under them (under them, one eviction pins a subtree for the whole serial
# row-write sequence). NOT YET CORRECT -- a reader missing between unlink
# and write lands on the stale value; diagnostic A/B only.
# Group commit sizing. ILogStore::PeriodicLog flushes when either
#   txns_.size() >= g_commit_group_sz   (default 32)
#   or g_log_freq_us elapses            (default 50 us)
# With 16 workers at most 16 commits can ever be pending, so the size
# predicate can never fire and every flush goes out on the 50 us timeout.
# Setting the group to the worker count lets it fire on size, with the
# timeout as the safety net.
LOG_FREQ_US=${NEMO_LOG_FREQ_US:-50}
COMMIT_GROUP=${NEMO_COMMIT_GROUP:-32}

FLUSH_OUTSIDE=${NEMO_FLUSH_OUTSIDE:-0}
FLUSH_KNOB=""
[ "$FLUSH_OUTSIDE" = 1 ] && FLUSH_KNOB=', "g_evict_flush_outside_latch": true'

LATCH_BENCH=${NEMO_LATCH_BENCH:-0}
LATCH_KNOB=""
[ "$LATCH_BENCH" = 1 ] && LATCH_KNOB=', "g_latch_bench": true'

LOG_WAIT=${NEMO_LOG_WAIT:-1}
LOG_WAIT_KNOB=""
[ "$LOG_WAIT" = 0 ] && LOG_WAIT_KNOB='"g_enable_log": false,'

EVICT_THDS=${NEMO_EVICT_THDS:-32}

WR_FANOUT=${NEMO_WR_FANOUT:-1}
WR_FANOUT_KNOB=""
[ "$WR_FANOUT" != 1 ] && WR_FANOUT_KNOB=", \"g_batch_write_fanout\": $WR_FANOUT"

# NEMO_RU_SPLIT=bytes|write  (default bytes -- the read-heavy rule)
#
# "bytes" splits the budget by uncompressed stored bytes -- the right rule for
# a read-heavy mix (splitting by row count starves the big hot table). Under
# the standard mix it fails the other way round, because write traffic does
# not follow stored bytes: the mix is heavily write-dominated in RU terms, and
# small tables (DISTRICT, STOCK) take writes from every transaction while the
# byte-heavy ORDER_LINE needs a fraction of what bytes hands it. The 429s that
# follow are retried inside AzureTableClient::StoreSync with a usleep backoff,
# throttling the write path while remote_io_time_ still looks small.
#
# "write" gives every table a floor sized to its write demand and splits what
# is left by that same demand. Applied to the Nemo (record) phase only -- the
# page dataset writes whole pages, a different access model.
RU_SPLIT=${NEMO_RU_SPLIT:-bytes}

# NEMO_DUMP_WRITES=1 sends every remote WRITE to a local gzip TSV under
# $BASE/dumpwrites/<point> instead of Cosmos, via g_dump_load_dir. The read
# half of every operation still goes to the store -- the point is to measure
# the read path with the write path taken out, not to fake the whole workload.
# Applied to BOTH systems: the hook covers IDataStore::Write (PageBufferManager
# flush), WriteBatchAsyncCallback (Nemo eviction write-back) and WriteAndRead
# (PageBufferManager's steady-state evict-and-load). Leaving WriteAndRead out
# would have handed Nemo free writes while the page baseline paid full price.
#
# Two consequences, both deliberate and neither hidden:
#   - the dataset in Cosmos stops receiving updates, so a row that is evicted
#     and read back returns its PRE-eviction value. g_tpcc_consistency_check
#     must stay off and this run cannot support any correctness claim. The
#     flip side is that every point now reads the same pristine state, so
#     point-to-point comparability improves.
#   - writes stop consuming RU, so the write-demand split has nothing left to
#     weight; RU_SPLIT is forced back to bytes below.
# NEMO_SCAN_GAP_MERGE=<n> sets g_scan_gap_merge: how many RESIDENT keys may sit
# between two holes of a range scan before they are still issued as one remote
# query. RangeSearch records the true holes but used to collapse them into
# their bounding interval, so one scan always cost one query at the price of
# refetching rows the buffer already held.
# 0 = issue the exact holes (most queries, no waste); the default 1000000000
# reproduces the old bounding-box behaviour, so this is an A/B knob and not a
# silent change of what the grid measures.
# NEMO_SCAN_CPP=1 sets g_scan_cpp_query: range scans go through the C++ SDK's
# table query instead of the embedded interpreter. Cosmos rejects the SDK's
# query with 400 InvalidInput unless "DataServiceVersion: 3.0" replaces its
# "MaxDataServiceVersion: 3.0;Native"; that one header is the whole
# incompatibility (see AzureTableClient::LoadRangeCpp). The C++ path is an
# order of magnitude cheaper per query and per row and scales with threads
# instead of serialising on the GIL.
# Default off so this is an A/B knob, not a silent change to what is measured.
SCAN_CPP=${NEMO_SCAN_CPP:-0}
SCAN_CPP_KNOB=""
[ "$SCAN_CPP" = 1 ] && SCAN_CPP_KNOB=', "g_scan_cpp_query": true'
SCAN_GAP_MERGE=${NEMO_SCAN_GAP_MERGE:-}
SCAN_GAP_KNOB=""
[ -n "$SCAN_GAP_MERGE" ] && SCAN_GAP_KNOB=", \"g_scan_gap_merge\": $SCAN_GAP_MERGE"
# NEMO_DUMP_WR_LAT_US=<us> sets g_dump_write_latency_us: with writes dumped
# locally, charge each row the latency the remote write WOULD have cost. Local
# dumping alone measures a machine with free writes, which deletes a real cost
# of record granularity; this makes the cost a dial instead. Set it to the
# calibrated per-op cost of a single uncontended remote write (a few ms) so
# thread count produces the queueing, rather than baking the queueing in.
# NEMO_LOG_LAT_US=<us> sets g_log_write_latency_us: with g_dump_load_dir active
# the log group goes to local disk and is charged one Cosmos round trip instead
# of being written remotely. Charged PER FLUSH, not per record, because that is
# what a correct log costs -- the remote path it replaces builds every entity
# in a group with the same PartitionKey/RowKey, so only one record per group
# survives. Set it to the calibrated p50 of a single-entity write.
# Only bites when NEMO_LOG_WAIT=1, i.e. when commits actually wait for the log.
# NEMO_WARMUP_BUDGET=<n> sets g_warmup_slot_budget: end the warm-up at whichever
# comes first, NEMO_WARMUP or the first district spending n order/history slots.
# Generative warm-up runs the full mix at CPU speed, so at a large buffer it
# can spend most of a district's NewOrder slots while the measured window
# needs only a few -- without the budget, large-buffer points die on slot
# exhaustion.
# NEMO_EGT=1 sets g_evict_batch_egt: eviction write-backs go out as entity-group
# transactions of up to 100 rows instead of one upsert per row. Needs the
# DataServiceVersion header fix; see AzureTableClient::BatchStoreEgt. Batching
# cuts per-row cost roughly 7x and multiplies the sustainable write-back rate.
NEMO_EGT=${NEMO_EGT:-0}
EGT_KNOB=""
[ "$NEMO_EGT" = 1 ] && EGT_KNOB=', "g_evict_batch_egt": true'
NEMO_WARMUP_BUDGET=${NEMO_WARMUP_BUDGET:-0}
WARMUP_BUDGET_KNOB=""
[ "$NEMO_WARMUP_BUDGET" != 0 ] && WARMUP_BUDGET_KNOB=", \"g_warmup_slot_budget\": $NEMO_WARMUP_BUDGET"
NEMO_LOG_LAT_US=${NEMO_LOG_LAT_US:-0}
LOG_LAT_KNOB=""
[ "$NEMO_LOG_LAT_US" != 0 ] && LOG_LAT_KNOB=", \"g_log_write_latency_us\": $NEMO_LOG_LAT_US"
NEMO_DUMP_WR_LAT_US=${NEMO_DUMP_WR_LAT_US:-0}
DUMP_WR_LAT_KNOB=""
[ "$NEMO_DUMP_WR_LAT_US" != 0 ] && DUMP_WR_LAT_KNOB=", \"g_dump_write_latency_us\": $NEMO_DUMP_WR_LAT_US"
DUMP_WRITES=${NEMO_DUMP_WRITES:-0}
if [ "$DUMP_WRITES" = 1 ] && [ "$RU_SPLIT" = write ]; then
  echo "[grid] NEMO_DUMP_WRITES=1: forcing RU_SPLIT=bytes (writes cost no RU)"
  RU_SPLIT=bytes
fi
DUMP_KNOB=""

# ---- PGBUF warm-up sizing -------------------------------------------
# The page baseline never reaches steady state inside a short window: Nemo's
# warm-up is latency-free (g_generative_warmup fabricates rows locally,
# g_warmup_skip_log drops the log) while every page the baseline needs has to
# cross the network. In the SIMULATED build that asymmetry is removed with
# g_sim_warmup_no_latency; on Cosmos there is nothing to remove -- the pages
# are really fetched -- so the equivalent is to size the warm-up window so the
# pool is actually faulted in before measurement starts. That is the formula
# the g_pgbuf_no_preload comment gives: frames / (workers / page_read_latency).
#
# Capped at the WORKING SET, not the pool: with the pre-load off the pool only
# ever holds what the workload references, and the simulator shows the page
# column stops filling past ~435k frames (misses/txn 0.37 at 75 % -> 0.32 at
# 100 %, i.e. the extra 145k frames buy nothing). Without the cap the 100 %
# point would burn 165 s of RU on frames that never get used.
#
#   1%  60 s | 10%  66 s | 25% 165 s | 50% 330 s | 75% 495 s | 100% 495 s
#
# NEMO_PGBUF_FILL=0 falls back to the flat NEMO_WARMUP for both systems
# (the old flat-window treatment).
PGBUF_FILL=${NEMO_PGBUF_FILL:-1}
# in-memory frame size: g_pagebuf_num_slots 58,054 for a 493 MB pool -> 8,492 B
# (the 8 KB storage page plus this buffer's per-slot metadata)
SLOT_B=${NEMO_SLOT_B:-8492}
# measured page read at the 40k tier, standard mix (PGBUF page read; the
# OBJBUF per-row read is several times cheaper)
PAGE_RD_MS=${NEMO_PAGE_RD_MS:-15.8}
# working-set cap = the 75 % pool, where the simulator's miss curve flattens
WSET_FRAMES=${NEMO_WSET_FRAMES:-435410}
WARM_SAFETY=${NEMO_WARM_SAFETY:-1.15}

BASE="${NEMO_BASE:-$HOME/nemo100g}"
PY=$BASE/venv/bin/python
CONN=$BASE/configs/ifconfig_azure.txt
SCRIPTS=${NEMO_REPO:-$HOME/NemoRun}/experiments
STAMP=$(date +%Y%m%d_%H%M%S)
OUT="../output/exp_tpcc_cosmos_34w_${MIX}.txt"
# 34 W logical size: 145 MB per warehouse
DATA_SZ=4930000000

cd "${NEMO_REPO:-$HOME/NemoRun}/build" || exit 1
export LD_LIBRARY_PATH=${JEMALLOC_LIB:-$HOME/mydata/jemalloc/lib}
export XDG_RUNTIME_DIR=/run/user/$(id -u)

restore_all() {
  echo "[grid] restoring 400 RU/s on tpcc34w_123*"
  for a in 1 2 3 4 5; do
    NEMO_AZURE_CONF=$CONN "$PY" "$SCRIPTS/set_ru.py" set 400 "tpcc34w_123" \
      > "$BASE/logs/grid_restore_$STAMP.txt" 2>&1
    NEMO_AZURE_CONF=$CONN "$PY" "$SCRIPTS/set_ru.py" list 2>/dev/null \
      | grep tpcc34w_123 | grep -qv "manual 400 RU/s" || { echo "[grid] all at 400"; return; }
    sleep 45
  done
  echo "**RESTORE INCOMPLETE -- run set_ru.py set 400 tpcc34w_123 manually!**"
}
trap restore_all EXIT
trap 'exit 143' HUP INT TERM

pkill -f "ru_watchdog[.]sh" 2>/dev/null || true
# 9>&- : do not let the watchdog inherit a caller's flock. run_fig5_grid.sh
# holds its single-instance lock on fd 9 and execs into this script, so without
# this the detached watchdog keeps that lock for up to 30 more minutes after
# the grid exits and the next launch is refused as "another launcher holds the
# lock". Harmless when fd 9 is not open.
setsid bash "$SCRIPTS/ru_watchdog.sh" 3 "tpcc34w_123" \
  "[E]xpTPCC|[t]pcc_cosmos_grid[.]sh" > /dev/null 2>&1 < /dev/null 9>&- &
echo "[grid] watchdog armed for tpcc34w_123* (32 tables)"

# ---- per-system knobs -------------------------------------------------
# nemo: the two steady-state fixes.
#   g_generative_warmup       warmup misses are regenerated from the TPC-C
#                             loader instead of read remotely -- on Cosmos
#                             this is what makes a real warmup affordable
#                             (only keys dirtied this run are fetched)
#   g_evict_write_once_first  blind-insert rows are admitted cold and evicted
#                             first, so the insert stream stops flushing the
#                             hot read set
#   g_warmup_skip_log/g_pgbuf_debug: upstream grid convention (no log traffic
#                             during warmup; per-table residency census)
# g_tpcc_blind_inserts is NOT optional here: the two-tier evictor keys on the
# BM_WRITE_ONCE tag, and that tag is only applied to blind-insert admissions.
# With blind inserts off the census reports write-once = 0 on every table, the
# evictor has nothing to prefer, ORDER_LINE's insert stream keeps 72% of the
# buffer and ITEM sits at 11-17% resident. Turning it on: ITEM 88-92% resident,
# 2.6-3.3x the throughput, and the 25-75% plateau turns back into a rising
# curve. It is a declared deviation (inserts skip the base-row fetch), the same
# one upstream's grid template records.
# Both generative warmup and two-tier eviction are OBJBUF-only; on PGBUF they
# are no-ops. The page baseline's
# equivalent is g_retain_idx_page + g_retain_idx_evict_cold.
# btree-ri also carries the two transferable halves of the warm-up fix:
#   g_pgbuf_no_preload  the restore pre-load walks every table in SCHEMA order,
#                       index pages before data pages, until the pool is full,
#                       admitting each with usage count 1. At 34 W / 50 % that
#                       leaves 118,098 frames (41 % of the pool) holding
#                       HISTORY, a table no transaction reads, and the clock
#                       cannot complete a sweep inside the window to evict it.
#                       Off, the pool holds only what the workload references.
#                       It costs nothing in wall time: the pre-load runs on
#                       g_num_restore_thds*2 = 16 threads, the same rate the 16
#                       workers fault pages in at, so the time simply moves
#                       from filling the pool wrongly to filling it correctly.
#   g_pgbuf_census      shutdown residency census WITHOUT g_pgbuf_debug's
#                       full-pool linear scan per miss (which would tax only
#                       the page baseline -- the quantity being measured).
# The third knob, g_sim_warmup_no_latency, is deliberately NOT set: it gates
# SimTableClient::InjectUs and is a no-op against real Cosmos. Its real-machine
# equivalent is the warm-up sizing above.
knobs_of() {
  case "$1" in
    nemo) echo '"g_index_type": "BTREE", "g_buf_type": "OBJBUF",
        "g_generative_warmup": true, "g_evict_write_once_first": true,
        "g_warmup_skip_log": true, "g_pgbuf_debug": true,
        "g_batch_eviction": true, "g_num_evict_thds": '"$EVICT_THDS"',
        "g_evict_threshold": 0.995, "g_batch_eviction_period_ms": 10'"$EVICT_BENCH_KNOB$WR_FANOUT_KNOB$DIRTY_LAST_KNOB$LATCH_KNOB$FLUSH_KNOB" ;;
    btree-ri) echo '"g_index_type": "BTREE", "g_buf_type": "PGBUF",
        "g_idx_btree_fanout": 245, "g_retain_idx_page": true,
        "g_retain_idx_evict_cold": true,
        "g_pgbuf_no_preload": true, "g_pgbuf_census": true' ;;
    btree-ei) echo '"g_index_type": "BTREE", "g_buf_type": "PGBUF",
        "g_idx_btree_fanout": 245, "g_retain_idx_page": false' ;;
    *) echo "unknown system $1" >&2; exit 1 ;;
  esac
}
dataset_of() { case "$1" in nemo) echo otpcc34w_123 ;; *) echo ptpcc34w_123 ;; esac; }
shards_of()  { case "$1" in nemo) echo "$HOME/dumps/shards_o34w" ;;
                            *)    echo "$HOME/dumps/shards_p34w" ;; esac; }

case "$MIX" in
  std) MIXCFG='"perc_neworder_": 0.45, "perc_payment_": 0.43,
        "perc_orderstatus_": 0.04, "perc_delivery_": 0.04,
        "perc_stocklevel_": 0.04' ;;
  ro)  MIXCFG='"perc_neworder_": 0.0, "perc_payment_": 0.0,
        "perc_orderstatus_": 0.5, "perc_delivery_": 0.0,
        "perc_stocklevel_": 0.5' ;;
  *) echo "NEMO_MIX must be std or ro" >&2; exit 1 ;;
esac

# Warm-up window for a PGBUF point: long enough to fault the pool in by
# demand, capped at the working set. See the NEMO_PGBUF_FILL block above.
pgbuf_warmup() {  # $1 ratio -> seconds
  python3 -c "
import math
frames = min($DATA_SZ * $1 / $SLOT_B, $WSET_FRAMES)
rate   = $WORKERS / ($PAGE_RD_MS / 1000.0)
print(max($WARMUP, int(math.ceil($WARM_SAFETY * frames / rate))))"
}

write_cfg() {  # $1 name  $2 ratio  $3 knobs  $4 warmup_s
  local BUF
  BUF=$(python3 -c "print(int($DATA_SZ*$2))")
  cat > "configs/$1.cfg" <<EOF
{
    "config_name": "$1",
    "tpcc_config": {
        "num_warehouses_": 34,
        "order_cap_per_district_": 15000,
        "history_cap_per_district_": 20000,
        "warmup_time_": $4, "runtime_": $RUNTIME,
        $MIXCFG,
        "by_name_perc_": 0.6, "payment_remote_perc_": 0.15,
        "remote_ol_perc_": 0.01, "neworder_rollback_perc_": 0.01
    },
    "db_config": {
        $3,
        "g_tpcc_blind_inserts": true,
        "g_dataset_suffix": "_123",
        "g_restore_from_remote": true,
        "g_load_to_remote_only": false,
        "g_partition_sz": 10000,
        "g_record_size": 108,
        "g_total_buf_sz": $BUF,
        "g_num_worker_threads": $WORKERS,
        "g_num_restore_thds": 8,
        "g_remote_req_retries": 2000,
        $LOG_WAIT_KNOB
        "g_enable_logging": $([ "$ENABLE_LOG" = 1 ] && echo true || echo false),
        "g_log_freq_us": $LOG_FREQ_US,
        "g_commit_group_sz": $COMMIT_GROUP,
        "g_enable_group_commit": true,
        "g_early_lock_release": true,
        "g_check_loaded": false,
        "g_scan_materialize": true,
        "g_save_output": true,
        "g_out_fname": "$OUT"$DUMP_KNOB$SCAN_GAP_KNOB$SCAN_CPP_KNOB$DUMP_WR_LAT_KNOB$LOG_LAT_KNOB$WARMUP_BUDGET_KNOB$EGT_KNOB
    }
}
EOF
}

# Split the measurement budget across a dataset's tables. NOT by row count:
# remote-read traffic tracks BYTES (how much of a table cannot stay resident),
# and rows are wildly uneven in width -- CUSTOMER is 2.3% of the rows but 13.7%
# of the bytes and is read by 43% of the transactions, so a row-weighted split
# left it at 400 RU/s and the first smoke run collapsed to 1.6 txn/s with every
# worker stuck in the engine's 429 retry loop (the retries are invisible in
# remote_rd_time_, which is why the counters looked "fast").
# Split: every table gets the 400 RU/s floor it must have anyway, and the
# REMAINDER goes purely by bytes. A uniform component sounds safer but at a
# 40k aggregate it hands ~24k to the 19 barely-read page tables and leaves the
# two hot ones (ORDER_LINE data + index, ~63% of the bytes and nearly all of
# the reads) starved at 15.8k -- measured 15.8 ms per page read there versus
# 2.6 ms for Nemo's unthrottled row reads.
scaled_plan() {  # $1 shard root  $2 budget -> "table<TAB>ru" lines
  local root=$1 budget=$2 d name items width
  for d in "$root"/*/; do
    name=$(basename "$d")
    items=$(awk -F'\t' '{n+=$2} END{print n+0}' "$d/index.txt")
    # every item in a table has the same width (fixed-width tuples; pages are
    # always 8192 B), so one dump line gives it. Uncompressed bytes are the
    # weight -- compressed size would lie: half-empty data pages compress far
    # better than dense index pages, which handed the ORDER_LINE index table
    # more RU than the data table it indexes.
    width=$(zcat "$(ls "$d"/s*.tsv.gz 2>/dev/null | head -1)" 2>/dev/null \
            | head -1 | awk -F'\t' '{print int(length($4)/2)}')
    [ -n "$width" ] || width=1
    echo "$name $items $width"
  done | awk -v B="$budget" '
    {n++; nm[n]=$1; b[n]=$2*$3; tot+=$2*$3}
    END {
      rest = B - 400*n; if (rest < 0) rest = 0
      for (i=1; i<=n; i++) {
        ru = 400 + int(rest*(b[i]/tot)/100)*100
        if (ru > 40000) ru = 40000
        print nm[i]"\t"ru
      }
    }'
}

# Write-demand split for the record dataset (NEMO_RU_SPLIT=write). Weight is
# rows written per transaction x RU per row write; the RU model is the linear
# fit through the two load-time calibration points (80 B -> 9.4 RU/row,
# 784 B -> 30), which over-predicts the middle and so errs toward giving a
# table too much rather than too little. Rows per transaction come from the
# standard mix 45/43/4/4/4 with Delivery touching 10 districts per call:
#   WAREHOUSE  Payment updates W_YTD                            0.43
#   DISTRICT   NewOrder takes the order slot + Payment D_YTD    0.88
#   CUSTOMER   Payment updates + Delivery updates 10            0.83
#   HISTORY    Payment inserts 1                                0.43
#   NEW_ORDER  NewOrder inserts 1 + Delivery deletes 10         0.85
#   ORDER      NewOrder inserts 1 + Delivery updates 10         0.85
#   ORDER_LINE NewOrder inserts ~10 + Delivery updates ~100     8.5
#   ITEM       read-only                                        0
#   STOCK      NewOrder updates 10                              4.5
#   9, 10      secondary indexes, maintained on insert          0.45
write_aware_plan() {  # $1 shard root  $2 budget -> "table<TAB>ru" lines
  local root=$1 budget=$2 d name items width
  for d in "$root"/*/; do
    name=$(basename "$d")
    items=$(awk -F'\t' '{n+=$2} END{print n+0}' "$d/index.txt")
    width=$(zcat "$(ls "$d"/s*.tsv.gz 2>/dev/null | head -1)" 2>/dev/null \
            | head -1 | awk -F'\t' '{print int(length($4)/2)}')
    [ -n "$width" ] || width=1
    echo "$name $items $width"
  done | python3 -c '
import sys
WPT = {0:0.43, 1:0.88, 2:0.83, 3:0.43, 4:0.85, 5:0.85, 6:8.5,
       7:0.0, 8:4.5, 9:0.45, 10:0.45}
budget = int(sys.argv[1]); FLOOR = 400
rows = []
for line in sys.stdin:
    name, items, width = line.split()
    try:
        tid = int(name.rsplit("-", 1)[1])
    except (IndexError, ValueError):
        tid = -1
    ruw = 9.4 + 0.0293 * max(0, int(width) - 80)
    rows.append((name, WPT.get(tid, 0.45) * ruw))
tot = sum(w for _, w in rows)
rest = max(0, budget - FLOOR * len(rows))
for name, w in rows:
    ru = FLOOR + (int(rest * (w / tot) / 100) * 100 if tot else 0)
    print("%s\t%d" % (name, min(ru, 40000)))
' "$budget"
}

# Per-point reset of the district cursors. NewOrder takes its order slot from
# D_NEXT_O_ID, which the loader sets to 3000 and the transaction refuses past
# order_cap_per_district_ (15000 since the o_cap rebuild) -- the dataset
# carries o_cap - 3000 inserts
# per district and, on a PERSISTENT store, every run spends them for good.
# Generative warmup spends them at memory speed. Re-uploading the tiny DISTRICT
# table from the pristine dump puts every point back on the same logical
# starting line: 340 rows for the record dataset, 10 pages for the page one.
# (This helper was once wiped by an edit to scaled_plan and the whole grid ran
# without it -- the 25% point then died on an exhausted district.)
reset_districts() {  # $1 dataset  $2 shard root
  local ds=$1 root=$2 t
  for t in "${ds}-1" "${ds}-0"; do   # DISTRICT, WAREHOUSE
    [ -d "$root/$t" ] || continue
    "$PY" "$SCRIPTS/upload_pages.py" --conn-file "$CONN" --table "$t" \
      --shard-dir "$root/$t" --manifest "$(mktemp -u /tmp/reset_XXXXXX.txt)" \
      --ru-budget 400 --ru-per-page 20 --workers 2 --batch 100 \
      2>&1 | tail -1 | sed "s/^/  reset $t: /"
  done
}

for sys in ${SYSTEMS//,/ }; do
  DS=$(dataset_of "$sys")
  SHARDS=$(shards_of "$sys")
  echo "=== phase $sys on $DS: bumping to ${RU_BUDGET} RU/s aggregate ==="
  if [ "$RU_SPLIT" = write ] && [ "$sys" = nemo ]; then
    echo "  [$sys] RU split: write-demand (NEMO_RU_SPLIT=write)"
    write_aware_plan "$SHARDS" "$RU_BUDGET" > "$BASE/logs/grid_${sys}_ru_$STAMP.tsv"
  else
    scaled_plan "$SHARDS" "$RU_BUDGET" > "$BASE/logs/grid_${sys}_ru_$STAMP.tsv"
  fi
  # NEMO_RU_OVERRIDE="<table>=<ru>,..." replaces the computed RU for named
  # tables. Needed because both split rules size a table from its READ demand
  # (bytes) or from a per-row write model (write), and the page dataset obeys
  # neither: it writes whole 8 KB pages on dirty eviction, so its RU demand
  # follows which tables the clock evicts dirty, which no static model knows.
  # In practice one table (e.g. CUSTOMER) can take nearly all the throttling
  # while another sits on budget it never uses; the override redirects the
  # budget by observed demand. Applied to the PLAN, so the per-table cap and
  # the aggregate report below cover overridden values too.
  if [ -n "${NEMO_RU_OVERRIDE:-}" ]; then
    awk -F'\t' -v OV="$NEMO_RU_OVERRIDE" 'BEGIN {
        n = split(OV, kv, ","); for (i = 1; i <= n; i++) {
          eq = index(kv[i], "="); if (eq) o[substr(kv[i], 1, eq-1)] = substr(kv[i], eq+1) } }
      { if ($1 in o) { printf "  [override] %s: %s -> %s RU/s\n", $1, $2, o[$1] > "/dev/stderr"
                       $2 = o[$1] } print $1"\t"$2 }' \
      "$BASE/logs/grid_${sys}_ru_$STAMP.tsv" > "$BASE/logs/grid_${sys}_ru_$STAMP.tsv.new" \
      && mv "$BASE/logs/grid_${sys}_ru_$STAMP.tsv.new" "$BASE/logs/grid_${sys}_ru_$STAMP.tsv"
  fi
  # STANDING RULE: no single table may exceed 40000 RU/s -- the 400 RU/s floor
  # is max(400, storageGB, highest-ever RU / 100), so one table crossing 40k
  # raises its floor for good. RU_BUDGET is an AGGREGATE spread over 11/21
  # tables; this is the per-table audit.
  awk -F'\t' -v S="$sys" -v B="$RU_BUDGET" '{if ($2 > mx) {mx=$2; who=$1} tot+=$2}
    END {printf "  [%s] %d tables, aggregate %d RU/s, largest single table %s = %d RU/s\n",
         S, NR, tot, who, mx
         if (mx > 40000) {print "  REFUSING: " who " exceeds the 40000 per-table cap" > "/dev/stderr"; exit 1}
         if (tot > B) {printf "  REFUSING: aggregate %d exceeds the %d budget\n", tot, B > "/dev/stderr"; exit 1}}' \
    "$BASE/logs/grid_${sys}_ru_$STAMP.tsv" || exit 1
  while IFS=$'\t' read -r tbl ru; do
    NEMO_AZURE_CONF=$CONN "$PY" "$SCRIPTS/set_ru.py" set "$ru" "$tbl" --exact \
      2>&1 | grep -E "OK|already" | sed 's/^/  /'
  done < "$BASE/logs/grid_${sys}_ru_$STAMP.tsv"

  if [ "$ENABLE_LOG" = 1 ]; then
    "$PY" -c 'import sys
from azure.data.tables import TableServiceClient
conn = open(sys.argv[1]).read().strip()
TableServiceClient.from_connection_string(conn).create_table_if_not_exists(sys.argv[2])
print("  log table %s ensured" % sys.argv[2])' "$CONN" "${DS}-log"
    NEMO_AZURE_CONF=$CONN "$PY" "$SCRIPTS/set_ru.py" set "$LOG_RU" "${DS}-log" --exact \
      2>&1 | grep -E "OK|already" | sed 's/^/  /'
  fi

  for r in ${RATIOS//,/ }; do
    reset_districts "$DS" "$SHARDS"
    NAME="tpcc_cosmos_${sys}_${r}"
    if [ "$DUMP_WRITES" = 1 ]; then
      DUMP_DIR="$BASE/dumpwrites/${sys}_${r}"
      rm -rf "$DUMP_DIR"; mkdir -p "$DUMP_DIR"
      DUMP_KNOB=',
        "g_dump_load_dir": "'"$DUMP_DIR"'"'
    fi
    # Nemo's warm-up is already latency-free, so it keeps the flat window; only
    # the page baseline gets a window sized to its pool.
    if [ "$sys" = "nemo" ] || [ "$PGBUF_FILL" != 1 ]; then
      WARM_R=$WARMUP
    else
      WARM_R=$(pgbuf_warmup "$r")
    fi
    write_cfg "$NAME" "$r" "$(knobs_of "$sys")" "$WARM_R"
    echo "--- running $sys ratio=$r (mix=$MIX ${WARM_R}s+${RUNTIME}s) ---"
    # full per-point engine log, not | tail -25: the startup census, the
    # generative-warmup counts and the "warm-up ended after X s" line all sit
    # above the last 25 lines and are needed to explain a point after the fact.
    PLOG="$BASE/logs/engine_${NAME}_$(date +%H%M%S).log"
    # NEMO_RUNNER wraps the engine invocation, e.g.
    #   NEMO_RUNNER="gdb -q -batch -ex run -ex 'thread apply all bt 15' --args"
    # for capturing a failure that only reproduces under the driver's exact
    # environment (RU plan applied, same config file), so the debugger has to
    # attach here, inside the driver, not outside it.
    # Unquoted on purpose: word-splitting is what makes the wrapper a prefix,
    # and an unset NEMO_RUNNER expands to nothing.
    ${NEMO_RUNNER:-} ./ExpTPCC "configs/$NAME.cfg" > "$PLOG" 2>&1
    tail -25 "$PLOG"
    if [ "$DUMP_WRITES" = 1 ]; then
      echo "  [$sys $r] writes diverted to disk: $(du -sh "$DUMP_DIR" 2>/dev/null | cut -f1)"
    fi
  done

  echo "=== phase $sys done: restoring 400 on $DS ==="
  NEMO_AZURE_CONF=$CONN "$PY" "$SCRIPTS/set_ru.py" set 400 "$DS" | tail -1
  if [ "$ENABLE_LOG" = 1 ] && [ "$DROP_LOG" = 1 ]; then
    "$PY" -c 'import sys
from azure.data.tables import TableServiceClient
name = sys.argv[2]
assert name.endswith("-log"), "refusing to delete %s: not a log table" % name
svc = TableServiceClient.from_connection_string(open(sys.argv[1]).read().strip())
try:
    svc.delete_table(name)
    print("  log table %s deleted" % name)
except Exception as e:
    print("  log table %s not deleted (%s)" % (name, type(e).__name__))' \
      "$CONN" "${DS}-log"
  fi
done

echo "=== grid done; final readback ==="
NEMO_AZURE_CONF=$CONN "$PY" "$SCRIPTS/set_ru.py" list | grep tpcc34w_123
# trap restores again
