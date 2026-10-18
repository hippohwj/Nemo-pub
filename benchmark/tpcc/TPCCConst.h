#ifndef ARBORETUM_BENCHMARK_TPCC_TPCCCONST_H_
#define ARBORETUM_BENCHMARK_TPCC_TPCCCONST_H_

#include "common/Common.h"
#include "db/ARDB.h"

namespace arboretum {
namespace tpcc {

// scale constants (fixed by the TPC-C specification)
constexpr uint64_t DIST_PER_WH = 10;
constexpr uint64_t CUST_PER_DIST = 3000;
constexpr uint64_t MAX_ITEMS = 100000;
constexpr uint64_t NUM_LASTNAMES = 1000;
constexpr uint64_t MAX_OL_PER_ORDER = 16;   // slot capacity; real ol_cnt in [5, 15]
constexpr uint64_t INIT_ORDERS_PER_DIST = 3000;
constexpr uint64_t INIT_DELIVERED_PER_DIST = 2100;
// customer slots per last-name bucket; the deterministic NURand assignment
// peaks at 78 customers for one name across the full 104-warehouse domain
constexpr uint64_t BYNAME_SLOTS = 96;

// runtime scale parameters, set by TPCCWorkload from TPCCConfig before any
// key is encoded (C++17 inline variables: single definition across all TUs)
inline uint64_t g_num_wh = 10;  // number of warehouses
inline uint64_t g_o_cap = 6000; // order slots per district (initial 3000 + headroom)
inline uint64_t g_h_cap = 8000; // history slots per district (initial 3000 + headroom)

// logical tables; order must match the schema built in TPCCWorkload
enum TableID : OID {
  T_WAREHOUSE = 0, T_DISTRICT, T_CUSTOMER, T_HISTORY, T_NEW_ORDER,
  T_ORDER, T_ORDER_LINE, T_ITEM, T_STOCK, T_CUST_BYNAME, T_OCUST_IDX,
  T_TABLE_CNT
};

// ---------------- key encoding (all ids 0-based) ----------------
// SearchKey is one uint64 whose decimal form must fit 8 digits (< 10^8);
// with g_o_cap = 6000 the largest key (ORDER_LINE) is
// (g_num_wh*10*g_o_cap)*16, so g_num_wh must stay <= 104.
inline uint64_t Did(uint64_t w, uint64_t d) { return w * DIST_PER_WH + d; }
inline uint64_t CustKey(uint64_t w, uint64_t d, uint64_t c) {
  return Did(w, d) * CUST_PER_DIST + c;
}
inline uint64_t OrderKey(uint64_t did, uint64_t o) { return did * g_o_cap + o; }
inline uint64_t OlKey(uint64_t okey, uint64_t j) { return okey * MAX_OL_PER_ORDER + j; }
inline uint64_t StockKey(uint64_t w, uint64_t i) { return w * MAX_ITEMS + i; }
inline uint64_t HistKey(uint64_t did, uint64_t h) { return did * g_h_cap + h; }
inline uint64_t NameKey(uint64_t did, uint64_t ln) { return did * NUM_LASTNAMES + ln; }

inline uint64_t Hash64(uint64_t x) {
  x ^= x >> 33; x *= 0xff51afd7ed558ccdULL;
  x ^= x >> 33; x *= 0xc4ceb9fe1a85ec53ULL;
  x ^= x >> 33; return x;
}

// deterministic order-line count of order (did, o): uniform in [5, 15].
// Load time pre-decides it so the executed NewOrder for slot o matches the
// pre-allocated ORDER_LINE rows.
inline uint64_t OlCnt(uint64_t did, uint64_t o) {
  return 5 + Hash64(did * 1000003 + o + 1) % 11;
}

// deterministic last name id of customer (did, c): the first 1000 customers
// cover every name once (per spec), the rest follow NURand(255).
inline uint64_t LastnameOf(uint64_t did, uint64_t c) {
  if (c < NUM_LASTNAMES) return c;
  auto h = Hash64(did * 0x9E3779B97F4A7C15ULL + c);
  auto a = h & 255;
  auto b = (h >> 8) % NUM_LASTNAMES;
  return ((a | b) + 173) % NUM_LASTNAMES;
}

// deterministic affine permutation of initial orders to customers:
// perm(o) = (7*o + b(did)) mod 3000, inverse uses 7^-1 mod 3000 = 2143
inline uint64_t InitOrderCust(uint64_t did, uint64_t o) {
  return (7 * o + Hash64(did) % CUST_PER_DIST) % CUST_PER_DIST;
}
inline uint64_t InitCustLastOrder(uint64_t did, uint64_t c) {
  auto b = Hash64(did) % CUST_PER_DIST;
  return (2143 * ((c + CUST_PER_DIST - b) % CUST_PER_DIST)) % CUST_PER_DIST;
}

// run-time NURand (uses the worker's thread-local generator)
inline uint64_t NURand(uint64_t A, uint64_t x, uint64_t y) {
  constexpr uint64_t C = 173;
  auto a = (uint64_t) ARDB::RandIntegerInclusive(0, (int) A);
  auto b = (uint64_t) ARDB::RandIntegerInclusive((int) x, (int) y);
  return (((a | b) + C) % (y - x + 1)) + x;
}
inline uint64_t RandCustId() { return NURand(1023, 0, CUST_PER_DIST - 1); }
inline uint64_t RandItemId() { return NURand(8191, 0, MAX_ITEMS - 1); }
inline uint64_t RandLastname() { return NURand(255, 0, NUM_LASTNAMES - 1); }

// per-table row counts and key domains for the current scale
inline uint64_t NumDists() { return g_num_wh * DIST_PER_WH; }
inline uint64_t TableRowCnt(OID tbl) {
  switch (tbl) {
    case T_WAREHOUSE:   return g_num_wh;
    case T_DISTRICT:    return NumDists();
    case T_CUSTOMER:    return NumDists() * CUST_PER_DIST;
    case T_HISTORY:     return NumDists() * g_h_cap;
    case T_NEW_ORDER:   return NumDists() * g_o_cap;
    case T_ORDER:       return NumDists() * g_o_cap;
    case T_ORDER_LINE:  return NumDists() * g_o_cap * MAX_OL_PER_ORDER;
    case T_ITEM:        return MAX_ITEMS;
    case T_STOCK:       return g_num_wh * MAX_ITEMS;
    case T_CUST_BYNAME: return NumDists() * NUM_LASTNAMES;
    case T_OCUST_IDX:   return NumDists() * CUST_PER_DIST;
    default: M_ASSERT(false, "unknown tpcc table %d", tbl);
  }
  return 0;
}
// all TPC-C key domains are dense: every key in [0, TableRowCnt) exists
inline uint64_t TableKeyDomain(OID tbl) { return TableRowCnt(tbl); }

} // namespace tpcc
} // namespace arboretum

#endif // ARBORETUM_BENCHMARK_TPCC_TPCCCONST_H_
