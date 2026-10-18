#!/usr/bin/env bash
# Full simulated Fig-5 grid on VM-C: Nemo (OBJBUF) and B+Tree-RI (PGBUF),
# TPC-C 34 W at order_cap 15000, buffer ratios {1,5,10,25,50,75,100}%.
#
#   vmc_sim_grid.sh [phase]      phase = nemo | btree | all (default all)
#   env: WARM (60) RUN (300) RATIOS
#
# The two snapshots do not fit on the 64 GB root disk together (records
# ~11 GB, pages ~38 GB at the new caps), so each phase builds its own and
# deletes it when done.
set -u
SRC=$HOME/NemoRun
cd "$SRC/build" || exit 1
export LD_LIBRARY_PATH=$HOME/mydata/jemalloc/lib
export XDG_RUNTIME_DIR=/run/user/$(id -u)
PHASE=${1:-all}
WARM=${WARM:-30}
RUN=${RUN:-30}
SKIP_BUILD=${SKIP_BUILD:-0}
RATIOS=${RATIOS:-"0.01 0.10 0.25 0.50 0.75 1.00"}
DATA_SZ=4930000000
OUT=${OUT:-$SRC/output/sim34w_oc15k_fig5.txt}
mkdir -p "$SRC/output"

mkcfg() {  # $1 name  $2 ratio  $3 knobs  $4 snapdir  $5 extra-tpcc
  local BUF
  BUF=$(python3 -c "print(int($DATA_SZ*$2))")
  cat > "configs/$1.cfg" <<EOF
{
    "config_name": "$1",
    "tpcc_config": {
        "num_warehouses_": 34,
        "order_cap_per_district_": 15000,
        "history_cap_per_district_": 20000,
        "warmup_time_": $WARM, "runtime_": $RUN,
        "perc_neworder_": 0.45, "perc_payment_": 0.43,
        "perc_orderstatus_": 0.04, "perc_delivery_": 0.04,
        "perc_stocklevel_": 0.04,
        "by_name_perc_": 0.6, "payment_remote_perc_": 0.15,
        "remote_ol_perc_": 0.01, "neworder_rollback_perc_": 0.01$5
    },
    "db_config": {
        $3,
        "g_tpcc_blind_inserts": true,
        "g_restore_from_remote": true, "g_load_to_remote_only": false,
        "g_partition_sz": 10000, "g_record_size": 108,
        "g_total_buf_sz": $BUF,
        "g_num_worker_threads": 16, "g_num_restore_thds": 8,
        "g_enable_logging": true, "g_enable_group_commit": true,
        "g_early_lock_release": true, "g_check_loaded": false,
        "g_scan_materialize": true, "g_save_output": true,
        "g_out_fname": "$OUT",
        "g_sim_snapshot_dir": "$4",
        $LAT
    }
}
EOF
}

# Cosmos-calibrated latency injection. rd/wr/scan/batch/log come from
# CalibrateSimLatency.py over 1145 historical runs (min per-op average =
# least-throttled estimate); today's unthrottled 40k-RU Cosmos smoke measured
# 2.6 ms per record read, confirming the 2.8 ms figure. The page baseline
# reads 8 KB units, not 100 B rows, so it gets today's measured 12.3 ms.
LAT_REC='"g_sim_rd_latency_us": 2800, "g_sim_wr_latency_us": 8000,
        "g_sim_scan_latency_us": 12000, "g_sim_batch_latency_us": 5400,
        "g_sim_log_latency_us": 5400'
LAT_PAGE='"g_sim_rd_latency_us": 12300, "g_sim_wr_latency_us": 8000,
        "g_sim_scan_latency_us": 12000, "g_sim_batch_latency_us": 5400,
        "g_sim_log_latency_us": 5400'

NEMO_KNOBS='"g_index_type": "BTREE", "g_buf_type": "OBJBUF",
        "g_generative_warmup": true, "g_evict_write_once_first": true,
        "g_warmup_skip_log": true, "g_pgbuf_debug": true,
        "g_batch_eviction": true, "g_num_evict_thds": 32,
        "g_evict_threshold": 0.995, "g_batch_eviction_period_ms": 10'
# The page phase needs a residency census too -- it is the only way to see how
# the pool is divided between index and data pages. It was missing because
# g_pgbuf_debug sat on the Nemo phase only. Use g_pgbuf_census here, not
# g_pgbuf_debug: the latter also turns on a per-miss full-pool linear scan that
# is unusable at these pool sizes (580k slots x 35k misses).
BTREE_KNOBS='"g_index_type": "BTREE", "g_buf_type": "PGBUF",
        "g_idx_btree_fanout": 245, "g_retain_idx_page": true,
        "g_retain_idx_evict_cold": true, "g_pgbuf_census": true'

run_phase() {  # $1 nemo|btree  $2 knobs  $3 snapdir  $4 loadcfg  $5 latency
  LAT=$5
  if [ "$SKIP_BUILD" = 1 ] && [ -d "$3" ] && [ -n "$(ls -A "$3" 2>/dev/null)" ]; then
    echo "########## phase $1: reusing snapshot in $3 ##########"
  else
    echo "########## phase $1: building snapshot in $3 ##########"
    rm -rf "$3"; mkdir -p "$3"
    /usr/bin/time -f "  snapshot build: %e s, peak RSS %M KB" \
      ./LoadTPCC "configs/$4" > /tmp/${1}_load.log 2>&1
    tail -1 /tmp/${1}_load.log
  fi
  du -sh "$3"; df -h / | tail -1
  for r in $RATIOS; do
    mkcfg "fig5_$1_$r" "$r" "$2" "$3" ""
    echo "--- $1 ratio=$r (${WARM}s + ${RUN}s) ---"
    ./ExpTPCC "configs/fig5_$1_$r.cfg" > /tmp/fig5_${1}_$r.log 2>&1
    echo "  rc=$? $(grep -E 'Throughput' /tmp/fig5_${1}_$r.log | tail -1)"
    grep -c "order slots exhausted" /tmp/fig5_${1}_$r.log | xargs -I{} echo "  order slots exhausted: {}"
  done
  echo "########## phase $1 done; freeing $3 ##########"
  [ "${KEEP_SNAP:-0}" = 1 ] || rm -rf "$3"
  df -h / | tail -1
}

[ "$PHASE" = all ] || [ "$PHASE" = nemo ] && \
  run_phase nemo "$NEMO_KNOBS" "$SRC/build/snap_rec_oc15k" load_tpcc_sim_34w_oc15k.cfg "$LAT_REC"
[ "$PHASE" = all ] || [ "$PHASE" = btree ] && \
  run_phase btree "$BTREE_KNOBS" "$SRC/build/snap_pag_oc15k" load_pages_tpcc_sim_34w_oc15k.cfg "$LAT_PAGE"
echo "########## all done -> $OUT ##########"
