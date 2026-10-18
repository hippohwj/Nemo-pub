#!/usr/bin/env bash
# How a single B+Tree (PGBUF) point was run against real Cosmos DB.
# One invocation per ratio, each carrying its own per-table RU plan
# (the plan files live under
# experiments/data/cosmos_34w_btree_20260819_logs/).
env NEMO_MIX=std NEMO_WARMUP=60 NEMO_RUNTIME=240 NEMO_RU=39600 \
    NEMO_LOG=1 NEMO_DROP_LOG=1 NEMO_LOG_LAT_US=5860 NEMO_LOG_WAIT=0 \
    NEMO_PGBUF_FILL=1 NEMO_RU_SETTLE=60 \
    NEMO_RU_OVERRIDE="<table>=<ru>,..." \
  bash ~/NemoRun/experiments/tpcc_cosmos_grid.sh <ratio> btree-ri
