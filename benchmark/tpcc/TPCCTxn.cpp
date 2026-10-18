#include <unordered_set>
#include "TPCCWorkload.h"
#include "common/BenchWorker.h"
#include "ITxn.h"

namespace arboretum {

using namespace tpcc;

RC TPCCWorkload::ReadRow(ITxn *txn, OID tbl, uint64_t key, AccessType ac,
                         char *&data, bool blind) {
  size_t sz;
  return db_->GetTuple(tbl_oid_[tbl], idx_oid_[tbl], SearchKey(key), data, sz,
                       ac, txn, blind && g_tpcc_blind_inserts);
}

RC TPCCWorkload::ResolveCustomer(TPCCQuery *q, ITxn *txn, uint64_t &c_key) {
  if (!q->by_last_name) {
    c_key = CustKey(q->c_w_id, q->c_d_id, q->c_id);
    return RC::OK;
  }
  char *data;
  auto key = NameKey(Did(q->c_w_id, q->c_d_id), q->c_last_id);
  auto rc = ReadRow(txn, T_CUST_BYNAME, key, AccessType::READ, data);
  if (rc != RC::OK) return rc;
  auto schema = schema_of_[T_CUST_BYNAME];
  auto cnt = GetNum(schema, CB_CNT, data);
  M_ASSERT(cnt > 0, "last name bucket %lu is empty", key);
  // spec: 1-based position ceil(n/2) in the name's customer list
  c_key = (uint64_t) GetNum(schema, CB_FIRST_SLOT + (cnt - 1) / 2, data);
  return RC::OK;
}

RC TPCCWorkload::NewOrderTxn(TPCCQuery *q, ITxn *txn, bool &user_rollback) {
  RC rc;
  char *data;
  auto did = Did(q->w_id, q->d_id);

  rc = ReadRow(txn, T_WAREHOUSE, q->w_id, AccessType::READ, data);
  if (rc != RC::OK) return rc;
  auto w_tax = GetNum(schema_of_[T_WAREHOUSE], W_TAX, data);

  rc = ReadRow(txn, T_DISTRICT, did, AccessType::UPDATE, data);
  if (rc != RC::OK) return rc;
  auto d_schema = schema_of_[T_DISTRICT];
  auto d_tax = GetNum(d_schema, D_TAX, data);
  auto o = (uint64_t) GetNum(d_schema, D_NEXT_O_ID, data);
  if (o >= g_o_cap) {
    // must abort the experiment: the next slot would alias another district
    static std::atomic<int> once{0};
    if (once.fetch_add(1) == 0) {
      std::string top;
      uint64_t tot = 0;
      for (uint64_t d = 0; d < NumDists(); d++) {
        auto c = neworder_by_dist_[d].load();
        tot += c;
        if (c > 1500) top += " d" + std::to_string(d) + "=" + std::to_string(c);
      }
      LOG_INFO("slot exhaustion diag: district %lu; NewOrder cursor advances per "
               "district (total %lu, mean %.0f):%s; warmup_finished=%d", did, tot,
               tot * 1.0 / NumDists(), top.c_str(), (int) g_warmup_finished);
    }
    LOG_ERROR("district %lu order slots exhausted (o=%lu); increase "
              "order_cap_per_district_", did, o);
    // Aliasing the next district's keys would corrupt the dataset (and
    // segfault a few rows later), so give up on this transaction instead.
    // Against a persistent store the slots are consumed for good, so a run
    // that reaches this needs the district cursors reset
    // (experiments/tpcc_cosmos_grid.sh re-uploads the DISTRICT table from the
    // pristine dump before every point).
    return RC::ABORT;
  }
  SetNum(d_schema, D_NEXT_O_ID, data, (int64_t)(o + 1));
  // first district to reach the budget ends the warm-up (the driver polls this
  // in tpcc.h). Generative warm-up runs the full mix at CPU speed, so a
  // time-bounded warm-up can consume nearly all of a district's NewOrder
  // slots while the measured window needs only a few.
  if (g_warmup_slot_budget > 0 && !g_warmup_finished &&
      o >= INIT_ORDERS_PER_DIST + g_warmup_slot_budget) {
    g_warmup_budget_hit.store(true, std::memory_order_release);
  }
  neworder_by_dist_[did].fetch_add(1, std::memory_order_relaxed);

  auto c_key = CustKey(q->w_id, q->d_id, q->c_id);
  rc = ReadRow(txn, T_CUSTOMER, c_key, AccessType::READ, data);
  if (rc != RC::OK) return rc;
  auto c_discount = GetNum(schema_of_[T_CUSTOMER], C_DISCOUNT, data);

  auto okey = OrderKey(did, o);
  auto ol_cnt = OlCnt(did, o);
  bool all_local = true;
  for (uint64_t j = 0; j < ol_cnt; j++) {
    if (q->items[j].supply_w_id != q->w_id) all_local = false;
  }

  // simulated inserts: fill the pre-allocated slots at the new order id.
  // slots at o >= INIT_ORDERS_PER_DIST are never-written (zeros + key) and
  // fully overwritten here, so they qualify as blind inserts
  rc = ReadRow(txn, T_ORDER, okey, AccessType::UPDATE, data, /*blind=*/true);
  if (rc != RC::OK) return rc;
  auto o_schema = schema_of_[T_ORDER];
  SetNum(o_schema, O_C_ID, data, (int64_t) q->c_id);
  SetNum(o_schema, O_ENTRY_D, data, (int64_t) GetSystemClock());
  SetNum(o_schema, O_CARRIER_ID, data, 0);
  SetNum(o_schema, O_OL_CNT, data, (int64_t) ol_cnt);
  SetNum(o_schema, O_ALL_LOCAL, data, all_local ? 1 : 0);
  SetNum(o_schema, O_VALID, data, 1);

  rc = ReadRow(txn, T_NEW_ORDER, okey, AccessType::UPDATE, data, /*blind=*/true);
  if (rc != RC::OK) return rc;
  SetNum(schema_of_[T_NEW_ORDER], NO_VALID, data, 1);

  // secondary index maintenance: customer -> latest order
  rc = ReadRow(txn, T_OCUST_IDX, c_key, AccessType::UPDATE, data);
  if (rc != RC::OK) return rc;
  SetNum(schema_of_[T_OCUST_IDX], OC_LAST_O_KEY, data, (int64_t) okey);

  for (uint64_t j = 0; j < ol_cnt; j++) {
    if (q->rollback && j == ol_cnt - 1) {
      // spec: 1% of NewOrder transactions roll back on an unused item id
      user_rollback = true;
      return RC::ABORT;
    }
    rc = ReadRow(txn, T_ITEM, q->items[j].i_id, AccessType::READ, data);
    if (rc != RC::OK) return rc;
    auto i_price = GetNum(schema_of_[T_ITEM], I_PRICE, data);

    auto s_key = StockKey(q->items[j].supply_w_id, q->items[j].i_id);
    rc = ReadRow(txn, T_STOCK, s_key, AccessType::UPDATE, data);
    if (rc != RC::OK) return rc;
    auto s_schema = schema_of_[T_STOCK];
    auto qty = GetNum(s_schema, S_QUANTITY, data);
    auto order_qty = (int64_t) q->items[j].quantity;
    qty = (qty >= order_qty + 10) ? qty - order_qty : qty - order_qty + 91;
    SetNum(s_schema, S_QUANTITY, data, qty);
    SetNum(s_schema, S_YTD, data, GetNum(s_schema, S_YTD, data) + order_qty);
    SetNum(s_schema, S_ORDER_CNT, data, GetNum(s_schema, S_ORDER_CNT, data) + 1);
    if (q->items[j].supply_w_id != q->w_id) {
      SetNum(s_schema, S_REMOTE_CNT, data, GetNum(s_schema, S_REMOTE_CNT, data) + 1);
    }

    rc = ReadRow(txn, T_ORDER_LINE, OlKey(okey, j), AccessType::UPDATE, data,
                 /*blind=*/true);
    if (rc != RC::OK) return rc;
    auto ol_schema = schema_of_[T_ORDER_LINE];
    auto amount = order_qty * i_price;
    amount = amount * (10000 + w_tax + d_tax) / 10000;
    amount = amount * (10000 - c_discount) / 10000;
    SetNum(ol_schema, OL_I_ID, data, (int64_t) q->items[j].i_id);
    SetNum(ol_schema, OL_SUPPLY_W_ID, data, (int64_t) q->items[j].supply_w_id);
    SetNum(ol_schema, OL_DELIVERY_D, data, 0);
    SetNum(ol_schema, OL_QUANTITY, data, order_qty);
    SetNum(ol_schema, OL_AMOUNT, data, amount);
    SetNum(ol_schema, OL_VALID, data, 1);
  }
  return RC::OK;
}

RC TPCCWorkload::PaymentTxn(TPCCQuery *q, ITxn *txn) {
  RC rc;
  char *data;
  auto did = Did(q->w_id, q->d_id);
  uint64_t h;
  uint64_t c_key;

  if (config_->cust_payment_) {
    // declared spec deviation (YCSB-style RW surface, "cust_payment"): the
    // WAREHOUSE/DISTRICT hot-row updates and the by-name path are not
    // exercised; the write set is one customer RMW plus one blind history
    // slot from a process-wide sequence striped over all districts, so
    // concurrent transactions never touch the same row
    auto seq = cust_pay_seq_.fetch_add(1, std::memory_order_relaxed);
    did = seq % NumDists();
    h = INIT_ORDERS_PER_DIST + seq / NumDists();
    if (h >= g_h_cap) {
      // must abort the experiment: the next slot would alias another district
      LOG_ERROR("cust_payment history slots exhausted (h=%lu); increase "
                "history_cap_per_district_", h);
    }
    c_key = CustKey(q->c_w_id, q->c_d_id, q->c_id);
  } else {
    rc = ReadRow(txn, T_WAREHOUSE, q->w_id, AccessType::UPDATE, data);
    if (rc != RC::OK) return rc;
    auto w_schema = schema_of_[T_WAREHOUSE];
    SetNum(w_schema, W_YTD, data, GetNum(w_schema, W_YTD, data) + q->amount);

    rc = ReadRow(txn, T_DISTRICT, did, AccessType::UPDATE, data);
    if (rc != RC::OK) return rc;
    auto d_schema = schema_of_[T_DISTRICT];
    SetNum(d_schema, D_YTD, data, GetNum(d_schema, D_YTD, data) + q->amount);
    h = (uint64_t) GetNum(d_schema, D_NEXT_H_ID, data);
    if (h >= g_h_cap) {
      // must abort the experiment: the next slot would alias another district
      LOG_ERROR("district %lu history slots exhausted (h=%lu); increase "
                "history_cap_per_district_", did, h);
    }
    SetNum(d_schema, D_NEXT_H_ID, data, (int64_t)(h + 1));
    if (g_warmup_slot_budget > 0 && !g_warmup_finished &&
        h >= INIT_ORDERS_PER_DIST + g_warmup_slot_budget) {
      g_warmup_budget_hit.store(true, std::memory_order_release);
    }

    rc = ResolveCustomer(q, txn, c_key);
    if (rc != RC::OK) return rc;
  }

  rc = ReadRow(txn, T_CUSTOMER, c_key, AccessType::UPDATE, data);
  if (rc != RC::OK) return rc;
  auto c_schema = schema_of_[T_CUSTOMER];
  SetNum(c_schema, C_BALANCE, data, GetNum(c_schema, C_BALANCE, data) - q->amount);
  SetNum(c_schema, C_YTD_PAYMENT, data,
         GetNum(c_schema, C_YTD_PAYMENT, data) + q->amount);
  SetNum(c_schema, C_PAYMENT_CNT, data, GetNum(c_schema, C_PAYMENT_CNT, data) + 1);
  auto credit_off = c_schema->GetFieldOffset(C_CREDIT);
  if (data[credit_off] == 'B') {
    // bad credit: rewrite c_data per spec
    memset(&data[c_schema->GetFieldOffset(C_DATA)], 'x',
           c_schema->GetFieldSize(C_DATA));
  }

  // simulated insert of the history record at the home district's next slot
  // (never-written slot, fully overwritten: blind insert)
  rc = ReadRow(txn, T_HISTORY, HistKey(did, h), AccessType::UPDATE, data,
               /*blind=*/true);
  if (rc != RC::OK) return rc;
  auto h_schema = schema_of_[T_HISTORY];
  SetNum(h_schema, H_C_KEY, data, (int64_t) c_key);
  SetNum(h_schema, H_DATE, data, (int64_t) GetSystemClock());
  SetNum(h_schema, H_AMOUNT, data, q->amount);
  return RC::OK;
}

RC TPCCWorkload::OrderStatusTxn(TPCCQuery *q, ITxn *txn) {
  RC rc;
  char *data;
  uint64_t okey;
  if (config_->rand_order_reads_) {
    // declared spec deviation (YCSB-style read surface): query a random
    // historical order directly — order -> its customer -> its order lines;
    // the by-name/OCUST_IDX last-order path is not exercised in this mode
    auto did = Did(q->w_id, q->d_id);
    okey = OrderKey(did, q->o_id);
    rc = ReadRow(txn, T_ORDER, okey, AccessType::READ, data);
    if (rc != RC::OK) return rc;
    auto c = (uint64_t) GetNum(schema_of_[T_ORDER], O_C_ID, data);
    rc = ReadRow(txn, T_CUSTOMER, did * CUST_PER_DIST + c,
                 AccessType::READ, data);
    if (rc != RC::OK) return rc;
  } else {
    uint64_t c_key;
    rc = ResolveCustomer(q, txn, c_key);
    if (rc != RC::OK) return rc;

    rc = ReadRow(txn, T_CUSTOMER, c_key, AccessType::READ, data);
    if (rc != RC::OK) return rc;

    rc = ReadRow(txn, T_OCUST_IDX, c_key, AccessType::READ, data);
    if (rc != RC::OK) return rc;
    okey = (uint64_t) GetNum(schema_of_[T_OCUST_IDX], OC_LAST_O_KEY, data);

    rc = ReadRow(txn, T_ORDER, okey, AccessType::READ, data);
    if (rc != RC::OK) return rc;
  }

  // point-only mode (rand_order_reads_ == 2): the order-line scan is skipped
  // too — the read surface is exactly two point reads (ORDER + its CUSTOMER),
  // the closest TPC-C analog of a YCSB point lookup
  if (config_->rand_order_reads_ == 2) return RC::OK;

  // range scan: all order lines of the customer's latest order
  rc = db_->GetTuples(tbl_oid_[T_ORDER_LINE], idx_oid_[T_ORDER_LINE],
                      SearchKey(OlKey(okey, 0)),
                      SearchKey(OlKey(okey, MAX_OL_PER_ORDER - 1)), txn);
  if (rc != RC::OK) {
    txn->ReleaseScanArray();
    return rc;
  }
  auto ol_schema = schema_of_[T_ORDER_LINE];
  int64_t total_amount = 0;
  for (auto &row : txn->scan_rows_) {
    if (GetNum(ol_schema, OL_VALID, row.second) == 1) {
      total_amount += GetNum(ol_schema, OL_AMOUNT, row.second);
    }
  }
  (void) total_amount;
  txn->ReleaseScanArray();
  return RC::OK;
}

RC TPCCWorkload::DeliveryTxn(TPCCQuery *q, ITxn *txn) {
  RC rc;
  char *data;
  for (uint64_t d = 0; d < DIST_PER_WH; d++) {
    auto did = Did(q->w_id, d);
    rc = ReadRow(txn, T_DISTRICT, did, AccessType::UPDATE, data);
    if (rc != RC::OK) return rc;
    auto d_schema = schema_of_[T_DISTRICT];
    auto del = (uint64_t) GetNum(d_schema, D_NEXT_DEL_O_ID, data);
    auto next = (uint64_t) GetNum(d_schema, D_NEXT_O_ID, data);
    if (del >= next) continue; // no undelivered order in this district
    SetNum(d_schema, D_NEXT_DEL_O_ID, data, (int64_t)(del + 1));

    auto okey = OrderKey(did, del);
    // simulated delete: consume the NEW_ORDER row
    rc = ReadRow(txn, T_NEW_ORDER, okey, AccessType::UPDATE, data);
    if (rc != RC::OK) return rc;
    SetNum(schema_of_[T_NEW_ORDER], NO_VALID, data, 0);

    rc = ReadRow(txn, T_ORDER, okey, AccessType::UPDATE, data);
    if (rc != RC::OK) return rc;
    auto o_schema = schema_of_[T_ORDER];
    auto c = (uint64_t) GetNum(o_schema, O_C_ID, data);
    auto ol_cnt = (uint64_t) GetNum(o_schema, O_OL_CNT, data);
    SetNum(o_schema, O_CARRIER_ID, data, (int64_t) q->carrier_id);

    int64_t amount_sum = 0;
    auto ol_schema = schema_of_[T_ORDER_LINE];
    for (uint64_t j = 0; j < ol_cnt; j++) {
      rc = ReadRow(txn, T_ORDER_LINE, OlKey(okey, j), AccessType::UPDATE, data);
      if (rc != RC::OK) return rc;
      SetNum(ol_schema, OL_DELIVERY_D, data, (int64_t) GetSystemClock());
      amount_sum += GetNum(ol_schema, OL_AMOUNT, data);
    }

    rc = ReadRow(txn, T_CUSTOMER, did * CUST_PER_DIST + c, AccessType::UPDATE, data);
    if (rc != RC::OK) return rc;
    auto c_schema = schema_of_[T_CUSTOMER];
    SetNum(c_schema, C_BALANCE, data, GetNum(c_schema, C_BALANCE, data) + amount_sum);
    SetNum(c_schema, C_DELIVERY_CNT, data,
           GetNum(c_schema, C_DELIVERY_CNT, data) + 1);
  }
  return RC::OK;
}

RC TPCCWorkload::StockLevelTxn(TPCCQuery *q, ITxn *txn) {
  RC rc;
  char *data;
  auto did = Did(q->w_id, q->d_id);

  rc = ReadRow(txn, T_DISTRICT, did, AccessType::READ, data);
  if (rc != RC::OK) return rc;
  auto next = (uint64_t) GetNum(schema_of_[T_DISTRICT], D_NEXT_O_ID, data);
  if (next == 0) return RC::OK;
  uint64_t lo_o, hi_o;
  if (config_->rand_order_reads_) {
    // declared spec deviation: scan a random 20-order window of the
    // district's history instead of the last 20 orders
    lo_o = q->o_id;
    hi_o = lo_o + 20;
  } else {
    hi_o = next;
    lo_o = next >= 20 ? next - 20 : 0;
  }

  // range scan: order lines of the selected 20-order window
  rc = db_->GetTuples(tbl_oid_[T_ORDER_LINE], idx_oid_[T_ORDER_LINE],
                      SearchKey(OlKey(OrderKey(did, lo_o), 0)),
                      SearchKey(OlKey(OrderKey(did, hi_o - 1), MAX_OL_PER_ORDER - 1)),
                      txn);
  if (rc != RC::OK) {
    txn->ReleaseScanArray();
    return rc;
  }
  auto ol_schema = schema_of_[T_ORDER_LINE];
  std::unordered_set<uint64_t> items;
  for (auto &row : txn->scan_rows_) {
    if (GetNum(ol_schema, OL_VALID, row.second) == 1) {
      items.insert((uint64_t) GetNum(ol_schema, OL_I_ID, row.second));
    }
  }
  txn->ReleaseScanArray();

  int64_t low_stock = 0;
  auto s_schema = schema_of_[T_STOCK];
  for (auto i : items) {
    rc = ReadRow(txn, T_STOCK, StockKey(q->w_id, i), AccessType::READ, data);
    if (rc != RC::OK) return rc;
    if (GetNum(s_schema, S_QUANTITY, data) < q->threshold) low_stock++;
  }
  (void) low_stock;
  return RC::OK;
}

} // namespace arboretum
