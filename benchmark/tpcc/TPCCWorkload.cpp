#include <cmath>
#include <chrono>
#include <fstream>
#include <memory>
#include "TPCCWorkload.h"
#include "common/BenchWorker.h"
#include "ITxn.h"
#include "local/ITable.h"
#include "remote/IDataStore.h"

namespace arboretum {

using namespace tpcc;

static const char * kTblNames[T_TABLE_CNT] = {
    "WAREHOUSE", "DISTRICT", "CUSTOMER", "HISTORY", "NEW_ORDER",
    "ORDER", "ORDER_LINE", "ITEM", "STOCK", "CUST_BYNAME", "OCUST_IDX"};

std::string TPCCWorkload::BuildSchemaString() {
  std::ostringstream ss;
  ss << "//size, type, name, pkey\n";
  ss << "TABLE=WAREHOUSE\n"
     << "    8,int64_t,W_KEY,1\n    8,int64_t,W_TAX,0\n    8,int64_t,W_YTD,0\n"
     << "    10,string,W_NAME,0\n    40,string,W_STREET,0\n    20,string,W_CITY,0\n"
     << "    2,string,W_STATE,0\n    9,string,W_ZIP,0\n\n";
  ss << "INDEX=IDX_WAREHOUSE\nWAREHOUSE,0\n";
  ss << "TABLE=DISTRICT\n"
     << "    8,int64_t,D_KEY,1\n    8,int64_t,D_TAX,0\n    8,int64_t,D_YTD,0\n"
     << "    8,int64_t,D_NEXT_O_ID,0\n    8,int64_t,D_NEXT_DEL_O_ID,0\n"
     << "    8,int64_t,D_NEXT_H_ID,0\n"
     << "    10,string,D_NAME,0\n    40,string,D_STREET,0\n    20,string,D_CITY,0\n"
     << "    2,string,D_STATE,0\n    9,string,D_ZIP,0\n\n";
  ss << "INDEX=IDX_DISTRICT\nDISTRICT,0\n";
  ss << "TABLE=CUSTOMER\n"
     << "    8,int64_t,C_KEY,1\n    8,int64_t,C_CREDIT_LIM,0\n    8,int64_t,C_DISCOUNT,0\n"
     << "    8,int64_t,C_BALANCE,0\n    8,int64_t,C_YTD_PAYMENT,0\n"
     << "    8,int64_t,C_PAYMENT_CNT,0\n    8,int64_t,C_DELIVERY_CNT,0\n"
     << "    8,int64_t,C_SINCE,0\n    8,int64_t,C_LAST_ID,0\n"
     << "    16,string,C_FIRST,0\n    2,string,C_MIDDLE,0\n    2,string,C_CREDIT,0\n"
     << "    40,string,C_STREET,0\n    20,string,C_CITY,0\n    2,string,C_STATE,0\n"
     << "    9,string,C_ZIP,0\n    16,string,C_PHONE,0\n    500,string,C_DATA,0\n\n";
  ss << "INDEX=IDX_CUSTOMER\nCUSTOMER,0\n";
  ss << "TABLE=HISTORY\n"
     << "    8,int64_t,H_KEY,1\n    8,int64_t,H_C_KEY,0\n    8,int64_t,H_DATE,0\n"
     << "    8,int64_t,H_AMOUNT,0\n    24,string,H_DATA,0\n\n";
  ss << "INDEX=IDX_HISTORY\nHISTORY,0\n";
  ss << "TABLE=NEW_ORDER\n"
     << "    8,int64_t,NO_KEY,1\n    8,int64_t,NO_VALID,0\n\n";
  ss << "INDEX=IDX_NEW_ORDER\nNEW_ORDER,0\n";
  ss << "TABLE=ORDER\n"
     << "    8,int64_t,O_KEY,1\n    8,int64_t,O_C_ID,0\n    8,int64_t,O_ENTRY_D,0\n"
     << "    8,int64_t,O_CARRIER_ID,0\n    8,int64_t,O_OL_CNT,0\n"
     << "    8,int64_t,O_ALL_LOCAL,0\n    8,int64_t,O_VALID,0\n\n";
  ss << "INDEX=IDX_ORDER\nORDER,0\n";
  ss << "TABLE=ORDER_LINE\n"
     << "    8,int64_t,OL_KEY,1\n    8,int64_t,OL_I_ID,0\n    8,int64_t,OL_SUPPLY_W_ID,0\n"
     << "    8,int64_t,OL_DELIVERY_D,0\n    8,int64_t,OL_QUANTITY,0\n"
     << "    8,int64_t,OL_AMOUNT,0\n    8,int64_t,OL_VALID,0\n"
     << "    24,string,OL_DIST_INFO,0\n\n";
  ss << "INDEX=IDX_ORDER_LINE\nORDER_LINE,0\n";
  ss << "TABLE=ITEM\n"
     << "    8,int64_t,I_KEY,1\n    8,int64_t,I_IM_ID,0\n    8,int64_t,I_PRICE,0\n"
     << "    24,string,I_NAME,0\n    50,string,I_DATA,0\n\n";
  ss << "INDEX=IDX_ITEM\nITEM,0\n";
  ss << "TABLE=STOCK\n"
     << "    8,int64_t,S_KEY,1\n    8,int64_t,S_QUANTITY,0\n    8,int64_t,S_YTD,0\n"
     << "    8,int64_t,S_ORDER_CNT,0\n    8,int64_t,S_REMOTE_CNT,0\n"
     << "    50,string,S_DATA,0\n";
  for (int i = 1; i <= 10; i++) {
    ss << "    24,string,S_DIST_" << (i < 10 ? "0" : "") << i << ",0\n";
  }
  ss << "\nINDEX=IDX_STOCK\nSTOCK,0\n";
  ss << "TABLE=CUST_BYNAME\n"
     << "    8,int64_t,CB_KEY,1\n    8,int64_t,CB_CNT,0\n";
  for (size_t i = 0; i < BYNAME_SLOTS; i++) {
    ss << "    8,int64_t,CB_C_KEY_" << i << ",0\n";
  }
  ss << "\nINDEX=IDX_CUST_BYNAME\nCUST_BYNAME,0\n";
  ss << "TABLE=OCUST_IDX\n"
     << "    8,int64_t,OC_KEY,1\n    8,int64_t,OC_LAST_O_KEY,0\n\n";
  ss << "INDEX=IDX_OCUST_IDX\nOCUST_IDX,0";
  return ss.str();
}

TPCCWorkload::TPCCWorkload(ARDB *db, TPCCConfig *config) : Workload(db) {
  config_ = config;
  g_num_wh = config->num_warehouses_;
  g_o_cap = config->order_cap_per_district_;
  g_h_cap = config->history_cap_per_district_;
  if (g_o_cap <= INIT_ORDERS_PER_DIST || g_h_cap <= INIT_ORDERS_PER_DIST) {
    LOG_ERROR("order/history capacity must exceed the initial 3000 rows per district");
  }
  // OBJBUF row keys are encoded as 8-digit decimal strings in remote storage
  auto max_key = NumDists() * g_o_cap * MAX_OL_PER_ORDER;
  if (max_key >= 100000000UL) {
    LOG_ERROR("ORDER_LINE key domain %lu exceeds the 8-digit storage key limit; "
              "reduce num_warehouses_ or order_cap_per_district_", max_key);
  }

  if (g_buf_type == HYBRBUF) {
    // Range scans are implemented for the two-tree buffer (bottom page-tree
    // coverage + top-tree dirty overlay, see ITxn::GetTuples HYBRBUF
    // branch), so the standard five-transaction mix is allowed. Scans
    // consume materialized rows; g_scan_materialize is forced on below.
    LOG_INFO("TPCC on HYBRBUF/2-Tree: full mix allowed (scans = bottom "
             "coverage + top-dirty overlay)");
  }
  // TPCC transactions consume range-scan results field by field
  if (!g_scan_materialize) {
    g_scan_materialize = true;
    LOG_INFO("TPCC enables g_scan_materialize");
  }

  // warehouse-level skew: wh_zipf_theta_ > 0 replaces the spec's worker-bound
  // home warehouse with a per-txn Zipf(theta) draw over all warehouses (hot =
  // low w_id, deterministic; all five txn types follow the same distribution)
  if (config_->wh_zipf_theta_ > 0) {
    M_ASSERT(config_->wh_zipf_theta_ < 1, "wh_zipf_theta_ must be in [0, 1)");
    wh_zipf_.Init(g_num_wh, config_->wh_zipf_theta_);
    LOG_INFO("TPCC warehouse zipf skew enabled: theta %.3f over %lu warehouses",
             config_->wh_zipf_theta_, g_num_wh);
  }
  // record-level skew: rec_zipf_theta_ > 0 replaces the spec's NURand for
  // item and by-id customer selection with Zipf(theta) (hot = low ids);
  // warehouses stay worker-bound, d_id uniform, lastname path per spec
  if (config_->rec_zipf_theta_ > 0) {
    M_ASSERT(config_->rec_zipf_theta_ < 1, "rec_zipf_theta_ must be in [0, 1)");
    item_zipf_.Init(MAX_ITEMS, config_->rec_zipf_theta_);
    cust_zipf_.Init(CUST_PER_DIST, config_->rec_zipf_theta_);
    LOG_INFO("TPCC record zipf skew enabled: theta %.3f over %d items and %d "
             "customers/district", config_->rec_zipf_theta_, MAX_ITEMS,
             CUST_PER_DIST);
  }
  // global-zipf customer span (declared deviation): Payment's paying
  // customer is drawn from one zipf over the WHOLE database instead of the
  // 3000-customer district domain, so mid-rank recurrence tracks the buffer
  // axis the way YCSB's global zipf does; ranks are striped round-robin
  // over districts, keeping the hot head spread across the cluster
  if (config_->global_cust_zipf_) {
    M_ASSERT(config_->rec_zipf_theta_ > 0,
             "global_cust_zipf_ requires rec_zipf_theta_ > 0");
    global_cust_zipf_gen_.Init(NumDists() * CUST_PER_DIST,
                               config_->rec_zipf_theta_);
    LOG_INFO("TPCC global customer zipf enabled: theta %.3f over %lu "
             "customers (district-striped ranks)",
             config_->rec_zipf_theta_, NumDists() * CUST_PER_DIST);
  }
  if (config_->cust_hot_scatter_) {
    LOG_INFO("TPCC customer hot-set dispersion enabled (bijective stride-1471"
             " permutation per district: access heat decoupled from key "
             "adjacency)");
  }
  if (config_->order_hot_scatter_) {
    LOG_INFO("TPCC order hot-set dispersion enabled (bijective stride-1471 "
             "permutation per district on the rand-order pick)");
  }
  // declared spec deviation (YCSB-style read surface): rand_order_reads_
  // makes OrderStatus read a random historical order (order -> its customer
  // -> its order-line scan; the by-name/OCUST last-order path is not
  // exercised) and StockLevel scan a random 20-order window instead of the
  // last 20 — the read-only working set then scales with the dataset.
  // rec_zipf_theta_ doubles as the order-pick skew axis (hot = low o_id).
  if (config_->rand_order_reads_) {
    if (config_->rec_zipf_theta_ > 0)
      order_zipf_.Init(INIT_ORDERS_PER_DIST, config_->rec_zipf_theta_);
    LOG_INFO("TPCC rand-order-reads variant enabled (%s order pick over %d "
             "initial orders/district%s)",
             config_->rec_zipf_theta_ > 0 ? "zipf" : "uniform",
             INIT_ORDERS_PER_DIST,
             config_->rand_order_reads_ == 2
                 ? "; point-only mode, order-line scan skipped" : "");
  }
  // declared spec deviation (YCSB-style RW surface): cust_payment_ replaces
  // Payment's footprint with one customer RMW (zipf/uniform pick) plus one
  // blind HISTORY slot from a process-wide sequence striped over all
  // districts; the WAREHOUSE/DISTRICT hot-row updates and the by-name path
  // are not exercised, so transactions never conflict and the RW row space
  // is the whole customer table
  if (config_->cust_payment_) {
    LOG_INFO("TPCC cust-payment variant enabled (customer RMW + striped "
             "history slots; W/D updates and by-name path skipped)");
  }
  if (config_->redraw_on_abort_) {
    LOG_INFO("TPCC redraw-on-abort enabled (a conflict-aborted transaction "
             "is replaced by a freshly generated query of the same type)");
  }

  auto schema_str = BuildSchemaString();
  std::istringstream in(schema_str);
  InitSchema(in);
  M_ASSERT(schemas_.size() == T_TABLE_CNT, "expect %d tpcc tables, got %zu",
           T_TABLE_CNT, schemas_.size());
  for (OID t = 0; t < T_TABLE_CNT; t++) {
    tbl_oid_[t] = tables_[kTblNames[t]];
    schema_of_[t] = schemas_[t];
    // the assigned index ids are global; resolve each table's primary index
    idx_oid_[t] = db_->GetTable(tbl_oid_[t])->GetPrimaryIdxId();
  }
  arboretum::ARDB::InitRand(config->num_workers_ * 2);
  neworder_by_dist_ = std::vector<std::atomic<uint64_t>>(NumDists());

  if (g_generative_warmup) {
    hook_self_ = this;
    g_gen_row_hook = &TPCCWorkload::GenRowHook;
    LOG_INFO("TPCC generative warmup enabled: record-buffer misses during warmup are "
             "regenerated from the deterministic loader (no remote reads); "
             "warmup workers %zu, skip log %d", (size_t) g_warmup_workers,
             (int) g_warmup_skip_log);
  }

  if (g_restore_from_remote) {
    WarmupCache();
    for (OID t = 0; t < T_TABLE_CNT; t++) {
      if (IsSkippedTable(tbl_oid_[t])) continue;
      db_->RestoreTable(tbl_oid_[t], TableRowCnt(t), TableKeyDomain(t));
    }
    LOG_INFO("Finished warming up cache for TPCC workload");
  } else {
    LoadData();
  }
}

// -------------------- data generation --------------------

// fill every column of table `tbl` for key `key`; pure function of (tbl, key)
static void GenRowData(ISchema *schema, OID tbl, uint64_t key, char *data) {
  memset(data, 0, schema->GetTupleSz());
  schema->SetNumericField<int64_t>(0, data, (int64_t) key);
  auto h = Hash64((uint64_t) tbl * 0x517cc1b727220a95ULL + key);
  switch (tbl) {
    case T_WAREHOUSE: {
      schema->SetNumericField<int64_t>(W_TAX, data, (int64_t)(h % 2001));       // 0-0.2, basis points
      schema->SetNumericField<int64_t>(W_YTD, data, 30000000);                  // $300k in cents
      break;
    }
    case T_DISTRICT: {
      schema->SetNumericField<int64_t>(D_TAX, data, (int64_t)(h % 2001));
      schema->SetNumericField<int64_t>(D_YTD, data, 3000000);
      schema->SetNumericField<int64_t>(D_NEXT_O_ID, data, (int64_t) INIT_ORDERS_PER_DIST);
      schema->SetNumericField<int64_t>(D_NEXT_DEL_O_ID, data, (int64_t) INIT_DELIVERED_PER_DIST);
      schema->SetNumericField<int64_t>(D_NEXT_H_ID, data, (int64_t) INIT_ORDERS_PER_DIST);
      break;
    }
    case T_CUSTOMER: {
      auto did = key / CUST_PER_DIST;
      auto c = key % CUST_PER_DIST;
      schema->SetNumericField<int64_t>(C_CREDIT_LIM, data, 5000000);
      schema->SetNumericField<int64_t>(C_DISCOUNT, data, (int64_t)(h % 5001));
      schema->SetNumericField<int64_t>(C_BALANCE, data, -1000);
      schema->SetNumericField<int64_t>(C_YTD_PAYMENT, data, 1000);
      schema->SetNumericField<int64_t>(C_PAYMENT_CNT, data, 1);
      schema->SetNumericField<int64_t>(C_LAST_ID, data, (int64_t) LastnameOf(did, c));
      // 10% bad credit per spec
      const char * credit = (h % 10 == 0) ? "BC" : "GC";
      memcpy(&data[schema->GetFieldOffset(C_CREDIT)], credit, 2);
      break;
    }
    case T_HISTORY: {
      auto did = key / tpcc::g_h_cap;
      auto hh = key % tpcc::g_h_cap;
      if (hh < INIT_ORDERS_PER_DIST) {
        schema->SetNumericField<int64_t>(H_C_KEY, data, (int64_t)(did * CUST_PER_DIST + hh));
        schema->SetNumericField<int64_t>(H_AMOUNT, data, 1000);
      }
      break;
    }
    case T_NEW_ORDER: {
      auto o = key % tpcc::g_o_cap;
      int64_t valid = (o >= INIT_DELIVERED_PER_DIST && o < INIT_ORDERS_PER_DIST) ? 1 : 0;
      schema->SetNumericField<int64_t>(NO_VALID, data, valid);
      break;
    }
    case T_ORDER: {
      auto did = key / tpcc::g_o_cap;
      auto o = key % tpcc::g_o_cap;
      if (o < INIT_ORDERS_PER_DIST) {
        schema->SetNumericField<int64_t>(O_C_ID, data, (int64_t) InitOrderCust(did, o));
        schema->SetNumericField<int64_t>(O_CARRIER_ID, data,
                                         o < INIT_DELIVERED_PER_DIST ? (int64_t)(1 + h % 10) : 0);
        schema->SetNumericField<int64_t>(O_OL_CNT, data, (int64_t) OlCnt(did, o));
        schema->SetNumericField<int64_t>(O_ALL_LOCAL, data, 1);
        schema->SetNumericField<int64_t>(O_VALID, data, 1);
      }
      break;
    }
    case T_ORDER_LINE: {
      auto okey = key / MAX_OL_PER_ORDER;
      auto j = key % MAX_OL_PER_ORDER;
      auto did = okey / tpcc::g_o_cap;
      auto o = okey % tpcc::g_o_cap;
      if (o < INIT_ORDERS_PER_DIST && j < OlCnt(did, o)) {
        schema->SetNumericField<int64_t>(OL_I_ID, data, (int64_t)(h % MAX_ITEMS));
        schema->SetNumericField<int64_t>(OL_SUPPLY_W_ID, data, (int64_t)(did / DIST_PER_WH));
        schema->SetNumericField<int64_t>(OL_DELIVERY_D, data, o < INIT_DELIVERED_PER_DIST ? 1 : 0);
        schema->SetNumericField<int64_t>(OL_QUANTITY, data, 5);
        schema->SetNumericField<int64_t>(OL_AMOUNT, data,
                                         o < INIT_DELIVERED_PER_DIST ? 0 : (int64_t)(h % 999999));
        schema->SetNumericField<int64_t>(OL_VALID, data, 1);
      }
      break;
    }
    case T_ITEM: {
      schema->SetNumericField<int64_t>(I_IM_ID, data, (int64_t)(h % 10000));
      schema->SetNumericField<int64_t>(I_PRICE, data, (int64_t)(100 + h % 9901));
      break;
    }
    case T_STOCK: {
      schema->SetNumericField<int64_t>(S_QUANTITY, data, (int64_t)(10 + h % 91));
      break;
    }
    case T_CUST_BYNAME: {
      // generated via FillBynameRow (one bucket pass per district); the pure
      // per-key form would rescan all 3000 customers for each of the 1000 keys
      M_ASSERT(false, "CUST_BYNAME rows are generated via FillBynameRow");
      break;
    }
    case T_OCUST_IDX: {
      auto did = key / CUST_PER_DIST;
      auto c = key % CUST_PER_DIST;
      auto o = InitCustLastOrder(did, c);
      schema->SetNumericField<int64_t>(OC_LAST_O_KEY, data, (int64_t) OrderKey(did, o));
      break;
    }
    default: M_ASSERT(false, "unknown tpcc table %d", tbl);
  }
}

// remote record loads batch rows per (table, partition): entity-group
// batches amortize the client stack's per-request overhead, which made
// per-row loads crawl (~30 ops/s box-wide on the Cosmos VM)
struct TpccLoadBuf {
  uint64_t part = UINT64_MAX;
  std::multimap<std::string, std::string> rows;
};
static thread_local TpccLoadBuf tl_load_bufs[T_TABLE_CNT];
constexpr size_t kLoadBatchRows = 100;  // Cosmos entity-group txn cap

void TPCCWorkload::FlushLoadBatch(OID tbl) {
  auto &buf = tl_load_bufs[tbl];
  if (buf.rows.empty()) return;
  db_->BatchInitInsert(tbl_oid_[tbl], (OID) buf.part, buf.rows);
  db_->AddLoadedRows(tbl_oid_[tbl], buf.rows.size());
  buf.rows.clear();
}

void TPCCWorkload::FlushLoadBatches() {
  for (OID t = 0; t < T_TABLE_CNT; t++) FlushLoadBatch(t);
}

void TPCCWorkload::LoadRow(OID tbl, uint64_t key, char *data) {
  if (g_buf_type == NOBUF && g_load_to_remote_only && g_dump_load_dir[0]) {
    // dump mode: hand the row to IDataStore::Write, whose g_dump_load_dir
    // hook diverts it to the per-thread gzip TSV. Going through the store
    // adapter (instead of writing our own file) keeps ONE dump format for
    // both datasets -- table NAME in column 1, gzip'd, and the metadata
    // rows FinishLoadingData() writes ride the same pipe -- so the sharder
    // and uploader are shared between the record and page datasets.
    SearchKey skey(key);
    g_data_store->Write(ITable::GetStorageId(tbl_oid_[tbl]), skey, data,
                        schema_of_[tbl]->GetTupleSz());
    db_->AddLoadedRows(tbl_oid_[tbl], 1);
    return;
  }
  if (g_buf_type == NOBUF && g_load_to_remote_only) {
    auto part = key / g_partition_sz;
    auto &buf = tl_load_bufs[tbl];
    if (buf.part != part) FlushLoadBatch(tbl);  // batches span one partition
    buf.part = part;
    buf.rows.insert({SearchKey(key).ToString(),
                     std::string(data, schema_of_[tbl]->GetTupleSz())});
    if (buf.rows.size() >= kLoadBatchRows) FlushLoadBatch(tbl);
    return;
  }
  db_->InitInsertWithPK(tbl_oid_[tbl], data, schema_of_[tbl]->GetTupleSz(), key);
}

// one district's last-name buckets, built in a single pass over its customers
// (heap-allocated: ~770 KB) and reused for all 1000 CUST_BYNAME keys of the
// district; both load orders (warehouse-major and table-major) visit the keys
// in ascending did order, so the cache hits
struct BynameBuckets {
  uint64_t did = UINT64_MAX;
  int64_t cnt[NUM_LASTNAMES];
  int64_t slots[NUM_LASTNAMES][BYNAME_SLOTS];
};

static void FillBynameRow(ISchema *schema, uint64_t key, char *data,
                          BynameBuckets &cache) {
  auto did = key / NUM_LASTNAMES;
  auto ln = key % NUM_LASTNAMES;
  if (cache.did != did) {
    memset(cache.cnt, 0, sizeof(cache.cnt));
    for (uint64_t c = 0; c < CUST_PER_DIST; c++) {
      auto name = LastnameOf(did, c);
      if (cache.cnt[name] < (int64_t) BYNAME_SLOTS) {
        cache.slots[name][cache.cnt[name]++] = (int64_t)(did * CUST_PER_DIST + c);
      }
    }
    cache.did = did;
  }
  memset(data, 0, schema->GetTupleSz());
  schema->SetNumericField<int64_t>(0, data, (int64_t) key);
  schema->SetNumericField<int64_t>(CB_CNT, data, cache.cnt[ln]);
  for (int64_t i = 0; i < cache.cnt[ln]; i++) {
    schema->SetNumericField<int64_t>(CB_FIRST_SLOT + i, data, cache.slots[ln][i]);
  }
}

TPCCWorkload *TPCCWorkload::hook_self_ = nullptr;

static std::atomic<uint64_t> g_genhook_fail_self{0}, g_genhook_fail_tbl{0},
    g_genhook_fail_sz{0}, g_genhook_fail_dom{0}, g_genhook_ok{0};
void TPCCWorkload::PrintGenHookStats() {
  LOG_INFO("gen-hook: ok %lu, fail(no self) %lu, fail(tbl map) %lu, fail(size) %lu, fail(domain) %lu",
           g_genhook_ok.load(), g_genhook_fail_self.load(), g_genhook_fail_tbl.load(),
           g_genhook_fail_sz.load(), g_genhook_fail_dom.load());
}

bool TPCCWorkload::GenRowHook(uint32_t tbl_id, uint64_t key, char *buf, size_t sz) {
  auto self = hook_self_;
  if (!self) { g_genhook_fail_self++; return false; }
  // engine table id -> tpcc table
  int t = -1;
  for (OID i = 0; i < T_TABLE_CNT; i++) if (self->tbl_oid_[i] == tbl_id) { t = (int) i; break; }
  if (t < 0) { g_genhook_fail_tbl++; return false; }
  auto schema = self->schema_of_[t];
  if (schema->GetTupleSz() != sz) { g_genhook_fail_sz++; return false; }
  if (key >= TableKeyDomain((OID) t)) { g_genhook_fail_dom++; return false; }
  g_genhook_ok++;
  if (t == T_CUST_BYNAME) {
    // per-district bucket build (~770 KB scratch); thread-local cache so
    // consecutive keys of the same district reuse one pass
    static thread_local BynameBuckets *cache = nullptr;
    if (!cache) cache = new BynameBuckets();
    FillBynameRow(schema, key, buf, *cache);
    return true;
  }
  GenRowData(schema, (OID) t, key, buf);
  return true;
}

void TPCCWorkload::LoadItemRange(uint64_t lo, uint64_t hi) {
  char data[schema_of_[T_ITEM]->GetTupleSz()];
  for (uint64_t i = lo; i < hi; i++) {
    GenRowData(schema_of_[T_ITEM], T_ITEM, i, data);
    LoadRow(T_ITEM, i, data);
  }
}

void TPCCWorkload::LoadStockRange(uint64_t w, uint64_t lo, uint64_t hi) {
  char data[schema_of_[T_STOCK]->GetTupleSz()];
  for (uint64_t i = lo; i < hi; i++) {
    auto key = StockKey(w, i);
    GenRowData(schema_of_[T_STOCK], T_STOCK, key, data);
    LoadRow(T_STOCK, key, data);
  }
}

void TPCCWorkload::LoadDistrict(uint64_t w, uint64_t d) {
  size_t max_sz = 0;
  for (OID t = 0; t < T_TABLE_CNT; t++) {
    max_sz = std::max(max_sz, (size_t) schema_of_[t]->GetTupleSz());
  }
  char data[max_sz];
  auto byname_cache = std::make_unique<BynameBuckets>();
  auto did = Did(w, d);
  GenRowData(schema_of_[T_DISTRICT], T_DISTRICT, did, data);
  LoadRow(T_DISTRICT, did, data);
  for (uint64_t c = 0; c < CUST_PER_DIST; c++) {
    auto key = did * CUST_PER_DIST + c;
    GenRowData(schema_of_[T_CUSTOMER], T_CUSTOMER, key, data);
    LoadRow(T_CUSTOMER, key, data);
    GenRowData(schema_of_[T_OCUST_IDX], T_OCUST_IDX, key, data);
    LoadRow(T_OCUST_IDX, key, data);
  }
  for (uint64_t ln = 0; ln < NUM_LASTNAMES; ln++) {
    auto key = NameKey(did, ln);
    FillBynameRow(schema_of_[T_CUST_BYNAME], key, data, *byname_cache);
    LoadRow(T_CUST_BYNAME, key, data);
  }
  for (uint64_t hh = 0; hh < g_h_cap; hh++) {
    auto key = HistKey(did, hh);
    GenRowData(schema_of_[T_HISTORY], T_HISTORY, key, data);
    LoadRow(T_HISTORY, key, data);
  }
  for (uint64_t o = 0; o < g_o_cap; o++) {
    auto okey = OrderKey(did, o);
    GenRowData(schema_of_[T_ORDER], T_ORDER, okey, data);
    LoadRow(T_ORDER, okey, data);
    GenRowData(schema_of_[T_NEW_ORDER], T_NEW_ORDER, okey, data);
    LoadRow(T_NEW_ORDER, okey, data);
    for (uint64_t j = 0; j < MAX_OL_PER_ORDER; j++) {
      auto olkey = OlKey(okey, j);
      GenRowData(schema_of_[T_ORDER_LINE], T_ORDER_LINE, olkey, data);
      LoadRow(T_ORDER_LINE, olkey, data);
    }
  }
  LOG_DEBUG("loaded warehouse %lu district %lu", w, d);
}

void TPCCWorkload::LoadWarehouse(uint64_t w) {
  char data[schema_of_[T_WAREHOUSE]->GetTupleSz()];
  GenRowData(schema_of_[T_WAREHOUSE], T_WAREHOUSE, w, data);
  LoadRow(T_WAREHOUSE, w, data);
  for (uint64_t d = 0; d < DIST_PER_WH; d++) {
    LoadDistrict(w, d);
  }
  LoadStockRange(w, 0, MAX_ITEMS);
  LOG_INFO("Finished loading warehouse %lu", w);
}

void TPCCWorkload::ParallelLoad(int thd_id, int num_thds, int64_t start_wh,
                                int64_t end_wh) {
  Worker::SetThdId(thd_id);
  // items are global; the first load job (starting at warehouse 0) loads them
  if (start_wh == 0) {
    auto chunk = MAX_ITEMS / num_thds + 1;
    auto lo = std::min((uint64_t)(thd_id * chunk), MAX_ITEMS);
    auto hi = std::min(lo + chunk, MAX_ITEMS);
    LoadItemRange(lo, hi);
  }
  // work units are districts and per-warehouse stock stripes, not whole
  // warehouses: remote loads are latency-bound per thread, and
  // warehouse-major partitioning left all threads but one idle at small
  // warehouse counts (a 1w load ran effectively single-threaded)
  for (int64_t w = start_wh; w < end_wh; w++) {
    auto chunk = MAX_ITEMS / num_thds + 1;
    auto lo = std::min((uint64_t)(thd_id * chunk), MAX_ITEMS);
    auto hi = std::min(lo + chunk, MAX_ITEMS);
    LoadStockRange((uint64_t) w, lo, hi);
    if (w % num_thds == thd_id) {
      char data[schema_of_[T_WAREHOUSE]->GetTupleSz()];
      GenRowData(schema_of_[T_WAREHOUSE], T_WAREHOUSE, (uint64_t) w, data);
      LoadRow(T_WAREHOUSE, (uint64_t) w, data);
    }
    for (uint64_t d = 0; d < DIST_PER_WH; d++) {
      if (((uint64_t) w * DIST_PER_WH + d) % (uint64_t) num_thds
          == (uint64_t) thd_id) {
        LoadDistrict((uint64_t) w, d);
      }
    }
  }
  // thread-local batch buffers die with this thread: drain them
  FlushLoadBatches();
}

void TPCCWorkload::LoadData() {
  LOG_INFO("Start loading TPCC data");
  auto start_time = std::chrono::high_resolution_clock::now();
  int64_t start_wh = config_->loading_start_wh_;
  int64_t end_wh = config_->loading_end_wh_ < 0 ? (int64_t) g_num_wh
                                                : config_->loading_end_wh_;
  if (g_buf_type == PGBUF) {
    // page/index bulk build requires ascending keys per table and the page
    // cursor is not thread-safe: load table-major, single-threaded
    auto byname_cache = std::make_unique<BynameBuckets>();
    for (OID t = 0; t < T_TABLE_CNT; t++) {
      char data[schema_of_[t]->GetTupleSz()];
      for (uint64_t key = 0; key < TableKeyDomain(t); key++) {
        if (t == T_CUST_BYNAME) {
          FillBynameRow(schema_of_[t], key, data, *byname_cache);
        } else {
          GenRowData(schema_of_[t], t, key, data);
        }
        LoadRow(t, key, data);
      }
      LOG_INFO("Finished generating pages for table %s", kTblNames[t]);
    }
  } else {
    if (!(g_buf_type == NOBUF && g_load_to_remote_only)) {
      LOG_ERROR("TPCC data loading expects NOBUF + g_load_to_remote_only "
                "(record datasets) or PGBUF (page datasets)");
    }
    std::vector<std::thread> threads;
    auto num_thds = (int) g_num_load_thds;
    for (int i = 0; i < num_thds; i++) {
      threads.emplace_back(TPCCWorkload::ExecuteLoad, this, i, num_thds,
                           start_wh, end_wh);
    }
    for (auto &thd : threads) thd.join();
  }
  for (OID t = 0; t < T_TABLE_CNT; t++) {
    db_->FinishLoadingData(tbl_oid_[t]);
  }
  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::seconds>(
      end_time - start_time).count();
  LOG_INFO("Finished loading TPCC data in %lld seconds", duration);
}

// -------------------- warmup --------------------

void TPCCWorkload::WarmupCache() {
  if (g_buf_type != OBJBUF && g_buf_type != HYBRBUF) {
    // page buffers warm lazily while the workload runs during the warmup window
    return;
  }
  // Bootstrap each table's object tree with its domain boundary records so
  // range scans have anchors; then pre-fill the tiny always-hot tables
  // (warehouse + district). Everything else warms lazily under the real
  // workload during the warmup window, consistent with Section 9.2.
  auto fetch_insert = [&](OID t, uint64_t key) {
    std::string data;
    SearchKey k(key);
    auto rc = g_data_store->Read(
        db_->GetTable(tbl_oid_[t])->GetStorageId(), k, data);
    if (rc != RC::OK || data.size() < schema_of_[t]->GetTupleSz()) {
      LOG_ERROR("warmup fetch failed for table %s key %lu (dataset not loaded?)",
                kTblNames[t], key);
    }
    // pin the boundary anchors: range scans assume a cached entry exists at
    // or below any start key, so the anchors must survive eviction
    db_->InitInsertWithPK(tbl_oid_[t], const_cast<char *>(data.data()),
                          schema_of_[t]->GetTupleSz(), key,
                          false, false, /*pin=*/true);
  };
  // fetch a whole dense key range in one remote scan instead of per-key reads
  auto fetch_insert_range = [&](OID t, uint64_t lo, uint64_t hi) {
    auto tbl = db_->GetTable(tbl_oid_[t]);
    auto tuple_sz = tbl->GetTotalTupleSize();
    char *addrs = nullptr;
    size_t cnt = 0;
    g_data_store->ReadRange(tbl->GetStorageId(), SearchKey(lo), SearchKey(hi),
                            &addrs, tuple_sz, cnt);
    if (cnt != hi - lo + 1) {
      LOG_ERROR("warmup range fetch for table %s [%lu, %lu] returned %zu rows "
                "(dataset not loaded?)", kTblNames[t], lo, hi, cnt);
    }
    for (size_t i = 0; i < cnt; i++) {
      auto src = &addrs[i * tuple_sz];
      auto pk = *((uint64_t *) (src + sizeof(ITuple)));
      db_->InitInsertWithPK(tbl_oid_[t], src + sizeof(ITuple),
                            schema_of_[t]->GetTupleSz(), pk,
                            false, false, /*pin=*/pk == lo || pk == hi);
    }
    if (addrs) DEALLOC(addrs);
  };
  for (OID t = 0; t < T_TABLE_CNT; t++) {
    if (t == T_WAREHOUSE || t == T_DISTRICT) continue; // fully prefilled below
    if (IsSkippedTable(tbl_oid_[t])) continue; // no remote table to anchor
    fetch_insert(t, 0);
    fetch_insert(t, TableKeyDomain(t) - 1);
  }
  if (!IsSkippedTable(tbl_oid_[T_WAREHOUSE])) {
    fetch_insert_range(T_WAREHOUSE, 0, g_num_wh - 1);
  }
  fetch_insert_range(T_DISTRICT, 0, NumDists() - 1);
  db_->FinishWarmupCache();
  LOG_INFO("TPCC warmup bootstrap done (%lu warehouses, %lu districts cached)",
           g_num_wh, NumDists());
}

// -------------------- query generation --------------------

// Gray et al. Zipf generator (same formulation as YCSBWorkload::zipf)
void TPCCWorkload::ZipfGen::Init(uint64_t n_arg, double theta_arg) {
  n = n_arg;
  theta = theta_arg;
  zetan = 0;
  for (uint64_t i = 1; i <= n; i++) zetan += 1.0 / pow((double) i, theta);
  zeta2 = 1.0 + pow(0.5, theta);
}

uint64_t TPCCWorkload::ZipfGen::Next() const {
  double alpha = 1 / (1 - theta);
  double eta = (1 - pow(2.0 / n, 1 - theta)) / (1 - zeta2 / zetan);
  double u = ARDB::RandDouble();
  double uz = u * zetan;
  if (uz < 1) return 0;
  if (uz < 1 + pow(0.5, theta)) return 1;
  auto idx = (uint64_t)(n * pow(eta * u - eta + 1, alpha));
  return idx >= n ? n - 1 : idx;
}

uint64_t TPCCWorkload::PickItemId() {
  return config_->rec_zipf_theta_ > 0 ? item_zipf_.Next() : RandItemId();
}

uint64_t TPCCWorkload::PickCustId() {
  return config_->rec_zipf_theta_ > 0 ? cust_zipf_.Next() : RandCustId();
}

uint64_t TPCCWorkload::PickOrderId() {
  return config_->rec_zipf_theta_ > 0
             ? order_zipf_.Next()
             : (uint64_t) ARDB::RandIntegerInclusive(0, INIT_ORDERS_PER_DIST - 1);
}

void TPCCWorkload::GenQuery(TPCCQuery &q, int force_type) {
  memset(&q, 0, sizeof(q));
  if (force_type >= 0) {
    // redraw_on_abort_: the replacement query keeps the aborted one's type
    // so the completed-transaction mix stays at the configured ratios
    q.type = (TPCCTxnType) force_type;
  } else {
    auto r = ARDB::RandDouble();
    if (r < config_->perc_neworder_) q.type = TPCC_NEW_ORDER;
    else if (r < config_->perc_neworder_ + config_->perc_payment_) q.type = TPCC_PAYMENT;
    else if (r < config_->perc_neworder_ + config_->perc_payment_ +
                 config_->perc_orderstatus_) q.type = TPCC_ORDER_STATUS;
    else if (r < config_->perc_neworder_ + config_->perc_payment_ +
                 config_->perc_orderstatus_ + config_->perc_delivery_) q.type = TPCC_DELIVERY;
    else q.type = TPCC_STOCK_LEVEL;
  }

  q.w_id = config_->wh_zipf_theta_ > 0 ? wh_zipf_.Next()
                                       : Worker::GetThdId() % g_num_wh;
  q.d_id = (uint64_t) ARDB::RandIntegerInclusive(0, DIST_PER_WH - 1);
  switch (q.type) {
    case TPCC_NEW_ORDER: {
      q.c_id = PickCustId();
      for (int j = 0; j < 15; j++) {
        // regenerate duplicate item ids: the NO_WAIT row lock is not
        // reentrant, so ordering the same item twice would deterministically
        // self-abort (and the retry loop would replay the same query forever)
        bool dup = true;
        while (dup) {
          q.items[j].i_id = PickItemId();
          dup = false;
          for (int k = 0; k < j; k++) {
            if (q.items[k].i_id == q.items[j].i_id) { dup = true; break; }
          }
        }
        q.items[j].quantity = (uint64_t) ARDB::RandIntegerInclusive(1, 10);
        if (g_num_wh > 1 && ARDB::RandDouble() < config_->remote_ol_perc_) {
          auto rw = (uint64_t) ARDB::RandIntegerInclusive(0, (int) g_num_wh - 2);
          q.items[j].supply_w_id = rw >= q.w_id ? rw + 1 : rw;
        } else {
          q.items[j].supply_w_id = q.w_id;
        }
      }
      q.rollback = ARDB::RandDouble() < config_->neworder_rollback_perc_;
      break;
    }
    case TPCC_PAYMENT: {
      q.amount = ARDB::RandIntegerInclusive(100, 500000);
      q.by_last_name = ARDB::RandDouble() < config_->by_name_perc_;
      q.c_last_id = RandLastname();
      if (config_->global_cust_zipf_) {
        // global rank -> (district, customer): did = r % NumDists spreads
        // consecutive (hot) ranks across districts; the W/D updates keep
        // the independently drawn home warehouse q.w_id
        auto r = global_cust_zipf_gen_.Next();
        auto did = r % NumDists();
        q.c_w_id = did / DIST_PER_WH;
        q.c_d_id = did % DIST_PER_WH;
        q.c_id = r / NumDists();
      } else {
        if (g_num_wh > 1 && ARDB::RandDouble() < config_->payment_remote_perc_) {
          auto rw = (uint64_t) ARDB::RandIntegerInclusive(0, (int) g_num_wh - 2);
          q.c_w_id = rw >= q.w_id ? rw + 1 : rw;
          q.c_d_id = (uint64_t) ARDB::RandIntegerInclusive(0, DIST_PER_WH - 1);
        } else {
          q.c_w_id = q.w_id;
          q.c_d_id = q.d_id;
        }
        q.c_id = PickCustId();
      }
      if (config_->cust_hot_scatter_) {
        // hot-set dispersion (declared deviation): bijective per-district
        // permutation rank -> (1471*rank + phase(did)) % CUST_PER_DIST
        // decouples access heat from key adjacency, so hot customers no
        // longer share leaf pages. 1471 is coprime with CUST_PER_DIST, so
        // the zipf mass is exactly preserved; record-granular caching is
        // invariant to this map by construction, page-granular is not.
        auto gdid = Did(q.c_w_id, q.c_d_id);
        q.c_id = (1471 * q.c_id + Hash64(gdid) % CUST_PER_DIST)
                 % CUST_PER_DIST;
      }
      break;
    }
    case TPCC_ORDER_STATUS: {
      q.c_w_id = q.w_id;
      q.c_d_id = q.d_id;
      q.by_last_name = ARDB::RandDouble() < config_->by_name_perc_;
      q.c_last_id = RandLastname();
      q.c_id = PickCustId();
      if (config_->rand_order_reads_) {
        q.o_id = PickOrderId();
        if (config_->order_hot_scatter_) {
          // hot-set dispersion for the order pick (mirrors
          // cust_hot_scatter_): bijective per-district stride-1471
          // permutation decouples order heat from o_id adjacency, so hot
          // orders stop sharing leaf pages (1471 coprime with 3000, zipf
          // mass exactly preserved). The derived customer read (OCustToC)
          // inherits the dispersion.
          q.o_id = (1471 * q.o_id +
                    Hash64(Did(q.w_id, q.d_id)) % INIT_ORDERS_PER_DIST)
                   % INIT_ORDERS_PER_DIST;
        }
      }
      break;
    }
    case TPCC_DELIVERY: {
      q.carrier_id = (uint64_t) ARDB::RandIntegerInclusive(1, 10);
      break;
    }
    case TPCC_STOCK_LEVEL: {
      q.threshold = ARDB::RandIntegerInclusive(10, 20);
      if (config_->rand_order_reads_) {
        auto o = PickOrderId();
        q.o_id = o > (uint64_t)(INIT_ORDERS_PER_DIST - 20)
                     ? (uint64_t)(INIT_ORDERS_PER_DIST - 20) : o;
      }
      break;
    }
    default: break;
  }
}

// -------------------- execution loop --------------------

void TPCCWorkload::Execute(TPCCWorkload *workload, BenchWorker *worker) {
  Worker::SetThdId(worker->worker_id_);
  Worker::SetMaxThdTxnId(0);
  ARDB::InitRand(worker->worker_id_);
  LOG_INFO("Thread-%u starts TPCC execution!", Worker::GetThdId());
  TPCCQuery query;
  ITxn *txn = nullptr;
  RC rc = RC::OK;
  bool user_rollback = false;
  uint64_t starttime = 0;
  uint32_t retry = 0;
  bool need_new_query = true;
  int redraw_type = -1;
  while (!g_terminate_exec && !(worker->warmup_only_ && g_warmup_finished)) {
    if (need_new_query) {
      retry = 0;
      // keep the committed txn pointer: the next StartTxn reclaims it
      // (dtor + free). Nulling it here leaked the whole ITxn and its
      // vector buffers every commit (~2 KB/txn, OOM at all-hit rates).
      workload->GenQuery(query, redraw_type);
      redraw_type = -1;
    }
    txn = workload->db_->StartTxn(txn, /*retry=*/!need_new_query);
    if (need_new_query) {
      starttime = txn->GetCurrrentStartTime();
    }
    user_rollback = false;
    switch (query.type) {
      case TPCC_NEW_ORDER:
        txn->txn_tpe = TxnType::RW_TXN;
        txn->SetReadOnly(false);
        rc = workload->NewOrderTxn(&query, txn, user_rollback);
        break;
      case TPCC_PAYMENT:
        txn->txn_tpe = TxnType::RW_TXN;
        txn->SetReadOnly(false);
        rc = workload->PaymentTxn(&query, txn);
        break;
      case TPCC_ORDER_STATUS:
        txn->txn_tpe = TxnType::SCAN_TXN;
        txn->SetReadOnly(true);
        rc = workload->OrderStatusTxn(&query, txn);
        break;
      case TPCC_DELIVERY:
        txn->txn_tpe = TxnType::RW_TXN;
        txn->SetReadOnly(false);
        rc = workload->DeliveryTxn(&query, txn);
        break;
      case TPCC_STOCK_LEVEL:
        txn->txn_tpe = TxnType::SCAN_TXN;
        txn->SetReadOnly(true);
        rc = workload->StockLevelTxn(&query, txn);
        break;
      default: M_ASSERT(false, "unknown tpcc txn type");
    }
    workload->db_->CommitTxn(txn, rc);
    // a user-initiated rollback (1% of NewOrder per spec) counts as a
    // completed transaction and is not retried
    need_new_query = (rc == RC::OK) || user_rollback;
    if (rc != RC::OK && !user_rollback) {
      retry++;
      if (workload->config_->redraw_on_abort_) {
        // redraw_on_abort_: a conflict-aborted transaction is replaced by a
        // freshly drawn query of the same type instead of replaying the same
        // keys, so NO_WAIT losers do not convoy behind the lock holder (and
        // the backoff sleep is skipped: the replacement is independent work)
        need_new_query = true;
        redraw_type = (int) query.type;
      } else {
        double sleep_ms = pow(2, retry - 1) * 2;
        int sleep_ms_int = static_cast<int>((sleep_ms > 10) ? 10 : sleep_ms);
        usleep(sleep_ms_int * 1000);
      }
    }
    if (g_warmup_finished) {
      auto endtime = GetSystemClock();
      auto latency = endtime - starttime;
      worker->bench_stats_.incr_thd_runtime_(latency);
      if (rc == RC::OK || user_rollback) {
        g_stats->db_stats_[Worker::GetThdId()].incr_txn_latency_(latency);
        g_stats->db_stats_[Worker::GetThdId()].incr_txn_cnt_(1);
        g_stats->db_stats_[Worker::GetThdId()].incr_cc_time_(txn->GetCCTime());
        g_stats->db_stats_[Worker::GetThdId()].incr_log_time_(txn->GetLogTime());
        if (retry != 0) {
          g_stats->db_stats_[Worker::GetThdId()].incr_abort_time_(
              txn->GetCurrrentStartTime() - starttime);
        }
        switch (query.type) {
          case TPCC_NEW_ORDER:
            worker->bench_stats_.incr_neworder_commit_cnt_(1);
            worker->bench_stats_.incr_neworder_latency_(latency);
            if (user_rollback) worker->bench_stats_.incr_neworder_rollback_cnt_(1);
            break;
          case TPCC_PAYMENT:
            worker->bench_stats_.incr_payment_commit_cnt_(1);
            worker->bench_stats_.incr_payment_latency_(latency);
            break;
          case TPCC_ORDER_STATUS:
            worker->bench_stats_.incr_orderstatus_commit_cnt_(1);
            worker->bench_stats_.incr_orderstatus_latency_(latency);
            break;
          case TPCC_DELIVERY:
            worker->bench_stats_.incr_delivery_commit_cnt_(1);
            worker->bench_stats_.incr_delivery_latency_(latency);
            break;
          case TPCC_STOCK_LEVEL:
            worker->bench_stats_.incr_stocklevel_commit_cnt_(1);
            worker->bench_stats_.incr_stocklevel_latency_(latency);
            break;
          default: break;
        }
      } else {
        switch (query.type) {
          case TPCC_NEW_ORDER: worker->bench_stats_.incr_neworder_abort_cnt_(1); break;
          case TPCC_PAYMENT: worker->bench_stats_.incr_payment_abort_cnt_(1); break;
          case TPCC_ORDER_STATUS: worker->bench_stats_.incr_orderstatus_abort_cnt_(1); break;
          case TPCC_DELIVERY: worker->bench_stats_.incr_delivery_abort_cnt_(1); break;
          case TPCC_STOCK_LEVEL: worker->bench_stats_.incr_stocklevel_abort_cnt_(1); break;
          default: break;
        }
      }
    }
  }
  LOG_INFO("Thread-%u finishes TPCC execution!", Worker::GetThdId());
  if (worker->warmup_only_) return; // never contributes to measured stats
  worker->SumUp();
}

} // namespace arboretum
