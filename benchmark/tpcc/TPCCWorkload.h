#ifndef ARBORETUM_BENCHMARK_TPCC_TPCCWORKLOAD_H_
#define ARBORETUM_BENCHMARK_TPCC_TPCCWORKLOAD_H_

#include <atomic>
#include "common/Workload.h"
#include "TPCCConfig.h"
#include "TPCCConst.h"

namespace arboretum {

class ARDB;
class BenchWorker;

enum TPCCTxnType {
  TPCC_NEW_ORDER = 0, TPCC_PAYMENT, TPCC_ORDER_STATUS, TPCC_DELIVERY,
  TPCC_STOCK_LEVEL, TPCC_TXN_TYPES
};

// column ids; must match the schema built in BuildSchemaString()
enum WarehouseCol { W_KEY = 0, W_TAX, W_YTD, W_NAME, W_STREET, W_CITY, W_STATE, W_ZIP };
enum DistrictCol  { D_KEY = 0, D_TAX, D_YTD, D_NEXT_O_ID, D_NEXT_DEL_O_ID,
                    D_NEXT_H_ID, D_NAME, D_STREET, D_CITY, D_STATE, D_ZIP };
enum CustomerCol  { C_KEY = 0, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT,
                    C_PAYMENT_CNT, C_DELIVERY_CNT, C_SINCE, C_LAST_ID, C_FIRST,
                    C_MIDDLE, C_CREDIT, C_STREET, C_CITY, C_STATE, C_ZIP,
                    C_PHONE, C_DATA };
enum HistoryCol   { H_KEY = 0, H_C_KEY, H_DATE, H_AMOUNT, H_DATA };
enum NewOrderCol  { NO_KEY = 0, NO_VALID };
enum OrderCol     { O_KEY = 0, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT,
                    O_ALL_LOCAL, O_VALID };
enum OrderLineCol { OL_KEY = 0, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D,
                    OL_QUANTITY, OL_AMOUNT, OL_VALID, OL_DIST_INFO };
enum ItemCol      { I_KEY = 0, I_IM_ID, I_PRICE, I_NAME, I_DATA };
enum StockCol     { S_KEY = 0, S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT,
                    S_DATA, S_DIST_01 /* .. S_DIST_10 follow */ };
enum CustBynameCol { CB_KEY = 0, CB_CNT, CB_FIRST_SLOT /* .. 64 slots follow */ };
enum OCustIdxCol  { OC_KEY = 0, OC_LAST_O_KEY };

struct TPCCQuery {
  TPCCTxnType type;
  uint64_t w_id;
  uint64_t d_id;
  // customer selection (Payment / OrderStatus)
  uint64_t c_w_id;
  uint64_t c_d_id;
  uint64_t c_id;
  bool by_last_name;
  uint64_t c_last_id;
  // NewOrder
  struct {
    uint64_t i_id;
    uint64_t supply_w_id;
    uint64_t quantity;
  } items[15];
  bool rollback;      // 1% NewOrder aborts by spec (invalid item)
  // Payment
  int64_t amount;     // cents
  // Delivery
  uint64_t carrier_id;
  // StockLevel
  int64_t threshold;
  // rand_order_reads_ variant: the order picked for OrderStatus / the
  // 20-order window start for StockLevel (within the 3000 initial orders)
  uint64_t o_id;
};

class TPCCWorkload : public Workload {
 public:
  explicit TPCCWorkload(ARDB *db, TPCCConfig *config);
  void LoadData();
  void WarmupCache();
  // post-run TPC-C consistency validation against remote storage state
  // (g_tpcc_consistency_check; PGBUF only, see TPCCCheck.cpp)
  void CheckConsistency();
  void GenQuery(TPCCQuery &query, int force_type = -1);
  static void Execute(TPCCWorkload *workload, BenchWorker *worker);
  // generative warmup: regenerate a row's load-time value from the
  // deterministic loader (registered as g_gen_row_hook). Returns false for
  // tables/keys that cannot be generated (caller fetches instead).
  static bool GenRowHook(uint32_t tbl_id, uint64_t key, char *buf, size_t sz);
  static TPCCWorkload *hook_self_;
  static void PrintGenHookStats();

  static void ExecuteLoad(TPCCWorkload *wl, int thd_id, int num_thds,
                          int64_t start_wh, int64_t end_wh) {
    wl->ParallelLoad(thd_id, num_thds, start_wh, end_wh);
  };
  void ParallelLoad(int thd_id, int num_thds, int64_t start_wh, int64_t end_wh);

 private:
  RC NewOrderTxn(TPCCQuery *query, ITxn *txn, bool &user_rollback);
  RC PaymentTxn(TPCCQuery *query, ITxn *txn);
  RC OrderStatusTxn(TPCCQuery *query, ITxn *txn);
  RC DeliveryTxn(TPCCQuery *query, ITxn *txn);
  RC StockLevelTxn(TPCCQuery *query, ITxn *txn);

  // customer resolution shared by Payment / OrderStatus
  RC ResolveCustomer(TPCCQuery *query, ITxn *txn, uint64_t &c_key);

  // data generation
  std::string BuildSchemaString();
  void LoadItemRange(uint64_t lo, uint64_t hi);
  void LoadWarehouse(uint64_t w);
  void LoadDistrict(uint64_t w, uint64_t d);
  void LoadStockRange(uint64_t w, uint64_t lo, uint64_t hi);

  // typed access helpers over raw tuple data
  static int64_t GetNum(ISchema *schema, OID col, char *data) {
    return *((int64_t *) &data[schema->GetFieldOffset(col)]);
  }
  static void SetNum(ISchema *schema, OID col, char *data, int64_t val) {
    *((int64_t *) &data[schema->GetFieldOffset(col)]) = val;
  }
  // insert one row into whichever target the current load/warmup mode uses
  void LoadRow(OID tbl, uint64_t key, char *data);
  void FlushLoadBatch(OID tbl);
  void FlushLoadBatches();

  // blind: full overwrite of a never-written pre-allocated slot (simulated
  // insert); with g_tpcc_blind_inserts the engine skips the base fetch
  RC ReadRow(ITxn *txn, OID tbl, uint64_t key, AccessType ac, char *&data,
             bool blind = false);

  // Gray et al. Zipf over [0, n), hot = low ids; zeta terms precomputed once.
  // Two orthogonal skew knobs build on it (both default 0 = spec behavior):
  //   wh_zipf_theta_  — per-txn draw over warehouses (hot-store contention
  //                     scenario; replaces the worker-bound terminal model)
  //   rec_zipf_theta_ — item + by-id customer selection (record-space
  //                     locality axis, YCSB-comparable; d_id and the by-name
  //                     lastname path stay per spec)
  struct ZipfGen {
    uint64_t n{0};
    double theta{0};
    double zetan{0};
    double zeta2{0};
    void Init(uint64_t n_arg, double theta_arg);
    uint64_t Next() const;
  };
  ZipfGen wh_zipf_;
  ZipfGen item_zipf_;
  ZipfGen cust_zipf_;
  ZipfGen order_zipf_;
  // global_cust_zipf_: one zipf over ALL NumDists*CUST_PER_DIST customers
  // (district-striped ranks) instead of the per-district 3000-customer domain
  ZipfGen global_cust_zipf_gen_;
  uint64_t PickItemId();
  uint64_t PickCustId();
  uint64_t PickOrderId();

  // cust_payment_ variant: process-wide history-slot sequence, striped
  // round-robin over all districts so concurrent transactions never share a
  // row (slot did = seq % NumDists, h = INIT_ORDERS_PER_DIST + seq / NumDists)
  std::atomic<uint64_t> cust_pay_seq_{0};
  // diag: NewOrder cursor advances (attempts incl. aborted) per district
  std::vector<std::atomic<uint64_t>> neworder_by_dist_;

  TPCCConfig *config_;
  OID tbl_oid_[tpcc::T_TABLE_CNT];
  OID idx_oid_[tpcc::T_TABLE_CNT];
  ISchema *schema_of_[tpcc::T_TABLE_CNT];
};

}

#endif //ARBORETUM_BENCHMARK_TPCC_TPCCWORKLOAD_H_
