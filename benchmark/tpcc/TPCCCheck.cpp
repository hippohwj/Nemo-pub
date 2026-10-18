// Post-run TPC-C consistency validation (g_tpcc_consistency_check).
//
// Re-reads the final database state directly from remote storage through
// g_data_store (after db->Terminate) and verifies a subset of the TPC-C
// consistency conditions adapted to the pre-allocated-slot methodology:
//   C1  per district: D_NEXT_DEL_O_ID <= D_NEXT_O_ID <= O_CAP
//   C2  per district: ORDER slots o < D_NEXT_O_ID all have O_VALID=1,
//       slots o >= D_NEXT_O_ID all have O_VALID=0 (max valid == next_o-1)
//   C3  per district: NEW_ORDER valid window is exactly
//       [D_NEXT_DEL_O_ID, D_NEXT_O_ID) (so NO_VALID=0 below the delivery
//       cursor and above the order cursor)
//   C4  per warehouse: W_YTD == sum of its districts' D_YTD
//   C5  sampled delivered orders: O_OL_CNT matches the deterministic
//       OlCnt(did, o), carrier is set, and exactly the first O_OL_CNT
//       ORDER_LINE slots have OL_VALID=1 with a nonzero OL_DELIVERY_D
//
// Buffer-type scope: PGBUF and OBJBUF. Before reading, all buffered state
// is pushed to remote so it holds the final committed state: PGBUF via
// PageBufferManager::FlushAll() (every resident page), OBJBUF via
// ITable::FlushDirtyObjects() (leaf-chain walk of each table's object
// index writing dirty cached records). HYBRBUF is skipped (TPC-C does not
// run on it yet).

#include <functional>
#include <memory>
#include <unordered_map>
#include <vector>
#include "TPCCWorkload.h"
#include "db/ARDB.h"
#include "db/buffer/PageBufferManager.h"
#include "local/IPage.h"
#include "local/ITable.h"
#include "local/ITuple.h"
#include "remote/IDataStore.h"

namespace arboretum {

using namespace tpcc;

namespace {

int64_t FieldNum(ISchema *schema, OID col, char *data) {
  return *((int64_t *) &data[schema->GetFieldOffset(col)]);
}

// Uniform view over one table's remote state, independent of how the
// dataset is materialized (8 KB pages for PGBUF, raw record rows for
// OBJBUF).
class TableStateReader {
 public:
  virtual ~TableStateReader() = default;
  // visit every stored row in key order; returns rows visited
  virtual size_t Scan(const std::function<void(uint64_t, char *)> &fn) = 0;
  // keyed point read; nullptr if unreadable (pointer valid until next call)
  virtual char *ReadRow(uint64_t key) = 0;
  size_t NumRemoteReads() const { return reads_; }

 protected:
  size_t reads_{0};
};

// PGBUF page datasets: pages are stored as whole BasePage values under key
// (tbl_id << 32) + pg_id; the single-threaded PGBUF loader fills them in
// ascending key order over a dense key domain, so full pages all hold the
// same number of tuples and a key maps to
// (first_pg + key / items_per_pg, key % items_per_pg). Every keyed read
// verifies the tuple's embedded primary key against the requested key.
class RemotePageReader : public TableStateReader {
 public:
  RemotePageReader(ITable *tbl, ISchema *schema)
      : tbl_(tbl), schema_(schema), storage_id_(tbl->GetStorageId()) {
    pg_cnt_ = (size_t) g_data_store->Read(storage_id_, "metadata", "pg_cnt_");
    page_ = std::make_unique<BasePage>();
    M_ASSERT(pg_cnt_ > 0, "no pg_cnt_ metadata for table %s",
             storage_id_.c_str());
  }

  size_t Scan(const std::function<void(uint64_t, char *)> &fn) override {
    size_t rows = 0;
    for (size_t pg = 0; pg < pg_cnt_; pg++) {
      if (!LoadPage(pg)) continue;  // pg ids start at 1 (pg_cnt_ init is 1)
      for (int64_t i = 0; i < page_->header_.num_items; i++) {
        auto tuple = (ITuple *) page_->GetData((size_t) i);
        auto payload = tuple->GetData();
        fn((uint64_t) FieldNum(schema_, 0, payload), payload);
        rows++;
      }
    }
    return rows;
  }

  // keyed point read via dense-slot arithmetic; nullptr if the page is
  // missing or the slot holds a different key (reported by the caller)
  char *ReadRow(uint64_t key) override {
    if (items_per_pg_ == 0 && !InitSlotArithmetic()) return nullptr;
    auto pg = first_pg_ + key / items_per_pg_;
    auto slot = key % items_per_pg_;
    if (!LoadPage(pg)) return nullptr;
    if ((int64_t) slot >= page_->header_.num_items) return nullptr;
    auto tuple = (ITuple *) page_->GetData(slot);
    auto payload = tuple->GetData();
    if ((uint64_t) FieldNum(schema_, 0, payload) != key) return nullptr;
    return payload;
  }

 private:
  bool InitSlotArithmetic() {
    // probe for the first data page (the loader starts at pg id 1) and take
    // its item count; all non-tail pages are filled to the same capacity
    for (size_t pg = 0; pg < 2; pg++) {
      if (LoadPage(pg)) {
        first_pg_ = pg;
        items_per_pg_ = (size_t) page_->header_.num_items;
        return items_per_pg_ > 0;
      }
    }
    return false;
  }

  bool LoadPage(size_t pg_id) {
    if (loaded_pg_ == (int64_t) pg_id) return true;
    SearchKey key(PageTag(tbl_->GetTableId(), (OID) pg_id).ToUInt64());
    std::string data;
    reads_++;
    auto rc = g_data_store->Read(storage_id_, key, data);
    if (rc != RC::OK) return false;
    M_ASSERT(data.size() >= sizeof(BasePage),
             "short page read from %s (%zu bytes)", storage_id_.c_str(),
             data.size());
    page_->SetFromLoaded(const_cast<char *>(data.data()), sizeof(BasePage));
    loaded_pg_ = (int64_t) pg_id;
    return true;
  }

  ITable *tbl_;
  ISchema *schema_;
  std::string storage_id_;
  size_t pg_cnt_{0};
  size_t first_pg_{0};
  size_t items_per_pg_{0};
  int64_t loaded_pg_{-1};
  std::unique_ptr<BasePage> page_;
};

// OBJBUF record datasets: one raw-payload row per key. Full scans go
// through IDataStore::ReadRange partition by partition (same buffer
// contract as the warmup: rows at i * total_tuple_sz + sizeof(ITuple),
// only existing rows returned); keyed reads are plain point reads with the
// embedded primary key verified.
class RemoteRecordReader : public TableStateReader {
 public:
  RemoteRecordReader(ITable *tbl, ISchema *schema, uint64_t key_domain)
      : tbl_(tbl), schema_(schema), storage_id_(tbl->GetStorageId()),
        domain_(key_domain) {}

  size_t Scan(const std::function<void(uint64_t, char *)> &fn) override {
    size_t rows = 0;
    auto total_sz = tbl_->GetTotalTupleSize();
    for (uint64_t lo = 0; lo < domain_; lo += g_partition_sz) {
      auto hi = std::min(domain_ - 1, lo + g_partition_sz - 1);
      char *addrs = nullptr;
      size_t cnt = 0;
      g_data_store->ReadRange(storage_id_, SearchKey(lo), SearchKey(hi),
                              &addrs, total_sz, cnt);
      reads_++;
      for (size_t i = 0; i < cnt; i++) {
        auto payload = &addrs[i * total_sz] + sizeof(ITuple);
        fn((uint64_t) FieldNum(schema_, 0, payload), payload);
        rows++;
      }
      if (addrs) DEALLOC(addrs);
    }
    return rows;
  }

  char *ReadRow(uint64_t key) override {
    SearchKey k(key);
    reads_++;
    auto rc = g_data_store->Read(storage_id_, k, row_);
    if (rc != RC::OK || row_.size() < schema_->GetTupleSz()) return nullptr;
    auto payload = const_cast<char *>(row_.data());
    if ((uint64_t) FieldNum(schema_, 0, payload) != key) return nullptr;
    return payload;
  }

 private:
  ITable *tbl_;
  ISchema *schema_;
  std::string storage_id_;
  uint64_t domain_;
  std::string row_;
};

struct DistrictState {
  int64_t next_o{-1}, next_del{-1}, ytd{0};
  // ORDER scan aggregates
  int64_t o_seen{0}, o_valid_cnt{0}, o_max_valid{-1};
  int64_t o_min_invalid{INT64_MAX};
  // NEW_ORDER scan aggregates
  int64_t no_seen{0}, no_valid_cnt{0}, no_max_valid{-1};
  int64_t no_min_valid{INT64_MAX};
};

struct SampledOrder {
  bool seen{false};
  int64_t valid{0}, ol_cnt{0}, carrier{0};
};

} // anonymous namespace

void TPCCWorkload::CheckConsistency() {
  if (!g_tpcc_consistency_check) return;
  if (g_buf_type != PGBUF && g_buf_type != OBJBUF) {
    LOG_INFO("TPCC consistency check is only implemented for PGBUF/OBJBUF; "
             "skipping");
    return;
  }
  auto starttime = GetSystemClock();
  // push all buffered state so remote storage holds the final committed state
  if (g_buf_type == PGBUF) {
    LOG_INFO("TPCC consistency check: flushing all buffered pages");
    ((PageBufferManager *) g_buf_mgr)->FlushAll();
  } else {
    size_t flushed = 0;
    for (OID t = 0; t < T_TABLE_CNT; t++) {
      flushed += db_->GetTable(tbl_oid_[t])->FlushDirtyObjects();
    }
    LOG_INFO("TPCC consistency check: flushed %zu dirty cached records",
             flushed);
  }

  size_t violations = 0;
#define CHECK_COND(cond, ...) do { if (!(cond)) { violations++;               \
    if (violations <= 50) {                                                   \
      std::printf("[TPCC-CHECK VIOLATION] " __VA_ARGS__);                     \
      std::printf("\n"); } } } while (0)

  auto num_dists = NumDists();
  std::vector<DistrictState> dists(num_dists);
  std::vector<int64_t> w_ytd(g_num_wh, -1);
  // sampled delivered orders per district, filled during the ORDER scan
  std::unordered_map<uint64_t, SampledOrder> samples;

  auto reader_of = [&](OID t) -> std::unique_ptr<TableStateReader> {
    auto tbl = db_->GetTable(tbl_oid_[t]);
    if (g_buf_type == PGBUF) {
      return std::make_unique<RemotePageReader>(tbl, schema_of_[t]);
    }
    return std::make_unique<RemoteRecordReader>(tbl, schema_of_[t],
                                                TableKeyDomain(t));
  };
  size_t remote_reads = 0;

  // ---- DISTRICT ----
  auto d_schema = schema_of_[T_DISTRICT];
  auto d_reader = reader_of(T_DISTRICT);
  auto d_rows = d_reader->Scan([&](uint64_t key, char *payload) {
    if (key >= num_dists) { CHECK_COND(false, "district key %lu out of range", key); return; }
    auto &d = dists[key];
    d.next_o = FieldNum(d_schema, D_NEXT_O_ID, payload);
    d.next_del = FieldNum(d_schema, D_NEXT_DEL_O_ID, payload);
    d.ytd = FieldNum(d_schema, D_YTD, payload);
  });
  CHECK_COND(d_rows == num_dists, "DISTRICT scan saw %zu rows, expect %lu",
             d_rows, num_dists);
  remote_reads += d_reader->NumRemoteReads();

  // C1 + pick per-district samples of delivered orders
  for (uint64_t did = 0; did < num_dists; did++) {
    auto &d = dists[did];
    CHECK_COND(d.next_o >= 0, "district %lu never seen in scan", did);
    CHECK_COND(d.next_del >= 0 && d.next_del <= d.next_o,
               "district %lu: D_NEXT_DEL_O_ID %ld > D_NEXT_O_ID %ld",
               did, d.next_del, d.next_o);
    CHECK_COND(d.next_o <= (int64_t) g_o_cap,
               "district %lu: D_NEXT_O_ID %ld exceeds O_CAP %lu",
               did, d.next_o, g_o_cap);
    if (d.next_del > 0) {
      int64_t picks[3] = {0, d.next_del / 2, d.next_del - 1};
      for (auto o : picks) {
        if (o >= 0 && o < d.next_del) samples[OrderKey(did, (uint64_t) o)];
      }
    }
  }

  // ---- WAREHOUSE ----
  auto w_schema = schema_of_[T_WAREHOUSE];
  auto w_reader = reader_of(T_WAREHOUSE);
  auto w_rows = w_reader->Scan([&](uint64_t key, char *payload) {
    if (key >= g_num_wh) { CHECK_COND(false, "warehouse key %lu out of range", key); return; }
    w_ytd[key] = FieldNum(w_schema, W_YTD, payload);
  });
  CHECK_COND(w_rows == g_num_wh, "WAREHOUSE scan saw %zu rows, expect %lu",
             w_rows, g_num_wh);
  remote_reads += w_reader->NumRemoteReads();

  // C4: W_YTD == sum(D_YTD)
  for (uint64_t w = 0; w < g_num_wh; w++) {
    int64_t sum = 0;
    for (uint64_t dd = 0; dd < DIST_PER_WH; dd++) sum += dists[Did(w, dd)].ytd;
    CHECK_COND(w_ytd[w] == sum,
               "warehouse %lu: W_YTD %ld != sum(D_YTD) %ld", w, w_ytd[w], sum);
  }

  // ---- ORDER ----
  auto o_schema = schema_of_[T_ORDER];
  auto o_reader = reader_of(T_ORDER);
  auto o_rows = o_reader->Scan([&](uint64_t key, char *payload) {
    auto did = key / g_o_cap;
    auto o = (int64_t)(key % g_o_cap);
    if (did >= num_dists) { CHECK_COND(false, "order key %lu out of range", key); return; }
    auto &d = dists[did];
    d.o_seen++;
    if (FieldNum(o_schema, O_VALID, payload) == 1) {
      d.o_valid_cnt++;
      if (o > d.o_max_valid) d.o_max_valid = o;
    } else if (o < d.o_min_invalid) {
      d.o_min_invalid = o;
    }
    auto it = samples.find(key);
    if (it != samples.end()) {
      it->second.seen = true;
      it->second.valid = FieldNum(o_schema, O_VALID, payload);
      it->second.ol_cnt = FieldNum(o_schema, O_OL_CNT, payload);
      it->second.carrier = FieldNum(o_schema, O_CARRIER_ID, payload);
    }
  });
  CHECK_COND(o_rows == num_dists * g_o_cap,
             "ORDER scan saw %zu rows, expect %lu", o_rows, num_dists * g_o_cap);
  remote_reads += o_reader->NumRemoteReads();

  // C2: valid orders are exactly the slots below the order cursor
  for (uint64_t did = 0; did < num_dists; did++) {
    auto &d = dists[did];
    CHECK_COND(d.o_seen == (int64_t) g_o_cap,
               "district %lu: ORDER scan saw %ld slots, expect %lu",
               did, d.o_seen, g_o_cap);
    CHECK_COND(d.o_valid_cnt == d.next_o,
               "district %lu: %ld valid ORDER slots, expect D_NEXT_O_ID %ld",
               did, d.o_valid_cnt, d.next_o);
    CHECK_COND(d.o_max_valid == d.next_o - 1,
               "district %lu: max valid ORDER slot %ld, expect %ld",
               did, d.o_max_valid, d.next_o - 1);
    CHECK_COND(d.o_min_invalid == d.next_o ||
               (d.next_o == (int64_t) g_o_cap && d.o_min_invalid == INT64_MAX),
               "district %lu: first invalid ORDER slot %ld, expect %ld",
               did, d.o_min_invalid, d.next_o);
  }

  // ---- NEW_ORDER ----
  auto no_schema = schema_of_[T_NEW_ORDER];
  auto no_reader = reader_of(T_NEW_ORDER);
  auto no_rows = no_reader->Scan([&](uint64_t key, char *payload) {
    auto did = key / g_o_cap;
    auto o = (int64_t)(key % g_o_cap);
    if (did >= num_dists) { CHECK_COND(false, "new-order key %lu out of range", key); return; }
    auto &d = dists[did];
    d.no_seen++;
    if (FieldNum(no_schema, NO_VALID, payload) == 1) {
      d.no_valid_cnt++;
      if (o > d.no_max_valid) d.no_max_valid = o;
      if (o < d.no_min_valid) d.no_min_valid = o;
    }
  });
  CHECK_COND(no_rows == num_dists * g_o_cap,
             "NEW_ORDER scan saw %zu rows, expect %lu",
             no_rows, num_dists * g_o_cap);
  remote_reads += no_reader->NumRemoteReads();

  // C3: NEW_ORDER valid window == [next_del, next_o)
  for (uint64_t did = 0; did < num_dists; did++) {
    auto &d = dists[did];
    CHECK_COND(d.no_seen == (int64_t) g_o_cap,
               "district %lu: NEW_ORDER scan saw %ld slots, expect %lu",
               did, d.no_seen, g_o_cap);
    auto expect_cnt = d.next_o - d.next_del;
    CHECK_COND(d.no_valid_cnt == expect_cnt,
               "district %lu: %ld valid NEW_ORDER slots, expect %ld",
               did, d.no_valid_cnt, expect_cnt);
    if (expect_cnt > 0) {
      CHECK_COND(d.no_min_valid == d.next_del,
                 "district %lu: min valid NEW_ORDER slot %ld, expect "
                 "D_NEXT_DEL_O_ID %ld (undelivered below the cursor)",
                 did, d.no_min_valid, d.next_del);
      CHECK_COND(d.no_max_valid == d.next_o - 1,
                 "district %lu: max valid NEW_ORDER slot %ld, expect %ld",
                 did, d.no_max_valid, d.next_o - 1);
    }
  }

  // ---- C5: sampled delivered orders and their ORDER_LINE rows ----
  auto ol_schema = schema_of_[T_ORDER_LINE];
  auto ol_reader = reader_of(T_ORDER_LINE);
  for (auto &pair : samples) {
    auto okey = pair.first;
    auto &s = pair.second;
    auto did = okey / g_o_cap;
    auto o = okey % g_o_cap;
    CHECK_COND(s.seen && s.valid == 1,
               "delivered order (did %lu, o %lu) missing or invalid", did, o);
    if (!s.seen) continue;
    CHECK_COND(s.ol_cnt == (int64_t) OlCnt(did, o),
               "order (did %lu, o %lu): O_OL_CNT %ld != deterministic %lu",
               did, o, s.ol_cnt, OlCnt(did, o));
    CHECK_COND(s.carrier != 0,
               "delivered order (did %lu, o %lu) has no carrier", did, o);
    for (uint64_t j = 0; j < MAX_OL_PER_ORDER; j++) {
      auto payload = ol_reader->ReadRow(OlKey(okey, j));
      if (payload == nullptr) {
        CHECK_COND(false, "order-line (did %lu, o %lu, line %lu) unreadable",
                   did, o, j);
        continue;
      }
      auto valid = FieldNum(ol_schema, OL_VALID, payload);
      auto delivery_d = FieldNum(ol_schema, OL_DELIVERY_D, payload);
      if ((int64_t) j < s.ol_cnt) {
        CHECK_COND(valid == 1 && delivery_d != 0,
                   "order-line (did %lu, o %lu, line %lu): valid %ld, "
                   "delivery_d %ld on a delivered order", did, o, j, valid,
                   delivery_d);
      } else {
        CHECK_COND(valid == 0,
                   "order-line (did %lu, o %lu, line %lu): valid beyond "
                   "O_OL_CNT %ld", did, o, j, s.ol_cnt);
      }
    }
  }
  remote_reads += ol_reader->NumRemoteReads();
#undef CHECK_COND

  auto secs = (GetSystemClock() - starttime) / 1000000000.0;
  if (violations > 0) {
    LOG_ERROR("TPCC consistency check FAILED: %zu violations "
              "(%zu sampled orders, %zu remote reads, %.1f s)",
              violations, samples.size(), remote_reads, secs);
  }
  LOG_INFO("TPCC consistency check PASSED (%lu districts, %zu sampled "
           "delivered orders, %zu remote reads, %.1f s)",
           num_dists, samples.size(), remote_reads, secs);
}

} // namespace arboretum
