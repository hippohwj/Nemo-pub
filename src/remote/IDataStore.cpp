#include "IDataStore.h"
#include "common/Worker.h"
#include "common/Stats.h"
#include <cstdio>
#include <unistd.h>   // usleep, for g_dump_write_latency_us
#include <mutex>
#include <vector>
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_SIMULATED
#include "SimNetClient.h"
#include "SimTableClient.h"
#endif

namespace arboretum {

// two-tree (HYBRBUF) touches both datasets, whose on-store key formats
// differ: the top (object) dataset is written PADDED by the NOBUF/OBJBUF
// loader, the bottom (page) dataset NUMERIC by the PGBUF loader, while
// SearchKey::ToString() alone cannot know the target. Pick per table:
// tables of the declared top dataset use the padded format, everything
// else keeps the buffer-type default.
static inline std::string KeyStr(const std::string &tbl_name,
                                 const SearchKey &key) {
  if (g_buf_type == HYBRBUF &&
      strncmp(tbl_name.c_str(), g_top_dataset_id,
              strlen(g_top_dataset_id)) == 0) {
    return key.ToPaddedString();
  }
  return key.ToString();
}

// g_dump_load_dir mode: every remote write is diverted to per-thread
// gzip'd TSV files that a python uploader replays against Cosmos. This
// single choke point covers data pages, index pages, metapage (binary)
// and pg_cnt_/row_cnt_ (numeric, dumped as INT:<v>).
// Line format: tbl \t part \t rowkey \t hex.
static std::mutex dump_pipes_mutex;
static std::vector<FILE *> dump_pipes;
static void CloseDumpPipes() {
  std::lock_guard<std::mutex> guard(dump_pipes_mutex);
  for (auto f : dump_pipes) pclose(f);
  dump_pipes.clear();
}
static FILE *GetDumpPipe() {
  static thread_local FILE *pipe = nullptr;
  if (!pipe) {
    std::lock_guard<std::mutex> guard(dump_pipes_mutex);
    if (dump_pipes.empty()) atexit(CloseDumpPipes);
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "gzip -1 >> %s/dump_%03zu.tsv.gz",
             g_dump_load_dir, dump_pipes.size());
    pipe = popen(cmd, "w");
    M_ASSERT(pipe, "cannot start gzip dump pipe in %s", g_dump_load_dir);
    dump_pipes.push_back(pipe);
  }
  return pipe;
}
static void DumpWriteRaw(const std::string &tbl, const std::string &part,
                         const std::string &row, const std::string &field) {
  fprintf(GetDumpPipe(), "%s\t%s\t%s\t%s\n", tbl.c_str(), part.c_str(),
          row.c_str(), field.c_str());
}
static void DumpWrite(const std::string &tbl, const std::string &part,
                      const std::string &row, const char *data, size_t sz) {
  auto f = GetDumpPipe();
  fprintf(f, "%s\t%s\t%s\t", tbl.c_str(), part.c_str(), row.c_str());
  static const char kHex[] = "0123456789abcdef";
  char buf[4096];
  size_t o = 0;
  for (size_t i = 0; i < sz; i++) {
    buf[o++] = kHex[(unsigned char) data[i] >> 4];
    buf[o++] = kHex[(unsigned char) data[i] & 0xf];
    if (o == sizeof(buf)) { fwrite(buf, 1, o, f); o = 0; }
  }
  if (o) fwrite(buf, 1, o, f);
  fputc('\n', f);
}

int IDataStore::ReadHistSlot(const std::string &tbl_name) {
  return RemoteTableSlot(tbl_name);
}

void IDataStore::PrintReadHist() {
  uint64_t rds = 0, scans = 0, rws = 0, wrs = 0;
  for (int i = 0; i < kReadHistSlots; i++) {
    auto r = read_hist_rds_[i].load(std::memory_order_relaxed);
    auto s = read_hist_scans_[i].load(std::memory_order_relaxed);
    auto w = read_hist_rws_[i].load(std::memory_order_relaxed);
    auto p = read_hist_wrs_[i].load(std::memory_order_relaxed);
    if (r || s || w || p) {
      LOG_INFO("read hist: %-10s %2d rds %10lu scans %8lu rws %8lu wrs %8lu",
               i >= 32 ? "IDX of tbl" : "tbl", i >= 32 ? i - 32 : i, r, s, w, p);
      rds += r; scans += s; rws += w; wrs += p;
    }
  }
  LOG_INFO("read hist: total rds %lu scans %lu rws %lu wrs %lu (post-warmup; "
           "should match remote_rds_/remote_scans_/remote_rws_/remote_wrs_)",
           rds, scans, rws, wrs);
}

IDataStore::IDataStore() {
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  std::ifstream in(redis_config_fname);
  std::string line;
  auto cnt = 0;
  std::vector<std::string> lines;
  while (getline(in, line)) {
    if (line[0] == '#') continue;
    cnt++;
    lines.push_back(line);
  }
  client_ = NEW(RedisClient)(lines[1], std::stol(lines[2]), lines[0]);
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_TIKV
  client_ = NEW(TiKVClient)();
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE
  std::ifstream in(azure_config_fname);
  std::string azure_conn_str;
  azure_conn_str.resize(500);
  getline(in, azure_conn_str);
  client_ = NEW(AzureTableClient)(azure_conn_str);
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_SIMULATED
  if (SimNetEnabled()) {
    client_ = NEW(SimNetClient)();
  } else {
    client_ = NEW(SimTableClient)();
  }
#endif
}

int64_t IDataStore::CheckSize() {
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  return client_->CheckSize(g_granule_id);
#endif
  return -1;
}

RC IDataStore::Read(const std::string &tbl_name, SearchKey &key, std::string &data) {
  auto starttime = GetSystemClock();
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  client_->LoadSync(tbl_name, key.ToString(), data);
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_TIKV
  client_->LoadSync(tbl_name, key.ToString(), data);
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE
  auto partition = std::to_string(key.ToUInt64() / g_partition_sz);
  // LOG_INFO("Read from Azure Table: tbl_name: %s, partition: %s, key: %s", tbl_name.c_str(), partition.c_str(), key.ToString().c_str());
  // std::cout.flush();
  auto rc = client_->LoadSync(tbl_name, partition, key.ToString(), data);
  // LOG_INFO("Read Finished from Azure Table: tbl_name: %s, partition: %s, key: %s, data: %s", tbl_name.c_str(), partition.c_str(), key.ToString().c_str(), data.c_str());
  // std::cout.flush();
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_SIMULATED
  auto partition = std::to_string(key.ToUInt64() / g_partition_sz);
  auto rc = client_->LoadSync(tbl_name, partition, KeyStr(tbl_name, key), data);
#endif
  if (g_warmup_finished) {
    auto latency = GetSystemClock() - starttime;
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_io_time_(latency);
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_rd_time_(latency);
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_rds_(1);
    read_hist_rds_[ReadHistSlot(tbl_name)].fetch_add(1, std::memory_order_relaxed);
  }
  return rc;
}


// only used in page-based B+Tree for range experiment
RC IDataStore::ReadQuery(const std::string &tbl_name, SearchKey &key, std::string &data) {
  //TODO: change tbl id to str for all functions and change accordingly in redis & tikv.
  auto starttime = GetSystemClock();
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  client_->LoadSync(tbl_name, key.ToString(), data);
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_TIKV
  client_->LoadSync(tbl_name, key.ToString(), data);
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE
  auto partition = std::to_string(key.ToUInt64() / g_partition_sz);
  // LOG_INFO("Read from Azure Table: tbl_name: %s, partition: %s, key: %s", tbl_name.c_str(), partition.c_str(), key.ToString().c_str());
  // std::cout.flush();
  auto rc = client_->LoadSyncQuery(tbl_name, partition,  key.ToString(), data);
  // LOG_INFO("Read Finished from Azure Table: tbl_name: %s, partition: %s, key: %s, data: %s", tbl_name.c_str(), partition.c_str(), key.ToString().c_str(), data.c_str());
  // std::cout.flush();
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_SIMULATED
  auto partition = std::to_string(key.ToUInt64() / g_partition_sz);
  auto rc = client_->LoadSyncQuery(tbl_name, partition, KeyStr(tbl_name, key), data);
#endif
  if (g_warmup_finished) {
    auto latency = GetSystemClock() - starttime;
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_io_time_(latency);
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_rd_time_(latency);
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_rds_(1);
    read_hist_rds_[ReadHistSlot(tbl_name)].fetch_add(1, std::memory_order_relaxed);
  }
  return rc;
}

int64_t IDataStore::Read(const std::string &tbl_name, std::string part_key, std::string key) {
  return client_->LoadNumericSync(tbl_name, part_key, key);
}

// only used in page-based B+Tree
RC IDataStore::Read(const std::string &tbl_name, std::string part_key, std::string key, std::string &data) {
  return client_->LoadSync(tbl_name, part_key, key, data);
}


void IDataStore::Write(const std::string &tbl_name, std::string part_key, std::string key, int64_t num) {
  if (g_dump_load_dir[0]) {
    DumpWriteRaw(tbl_name, part_key, key, "INT:" + std::to_string(num));
    if (g_dump_write_latency_us) usleep(g_dump_write_latency_us);
    return;
  }
  client_->StoreNumericSync(tbl_name, part_key, key, num);
}

void IDataStore::Write(const std::string &tbl_name, std::string part_key, std::string key, char *data, size_t sz) {
  if (g_dump_load_dir[0]) {
    DumpWrite(tbl_name, part_key, key, data, sz);
    if (g_dump_write_latency_us) usleep(g_dump_write_latency_us);
    return;
  }
  client_->StoreSync(tbl_name, part_key, key, data, sz);
}

void IDataStore::Write(const std::string &tbl_name, SearchKey &key, char *data, size_t sz) {
  if (g_dump_load_dir[0]) {
    auto dump_part = std::to_string(key.ToUInt64() / g_partition_sz);
    DumpWrite(tbl_name, dump_part, KeyStr(tbl_name, key), data, sz);
    if (g_dump_write_latency_us) usleep(g_dump_write_latency_us);
    return;
  }
  auto starttime = GetSystemClock();
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  client_->StoreSync(tbl_name, key.ToString(), data, sz);
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_TIKV
  std::string value(data, sz);
  client_->StoreSync(tbl_name, key.ToString(), value);
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE || \
      REMOTE_STORAGE_TYPE == REMOTE_STORAGE_SIMULATED
  auto partition = std::to_string(key.ToUInt64() / g_partition_sz);
  client_->StoreSync(tbl_name, partition, KeyStr(tbl_name, key), data, sz);
#endif
  if (g_warmup_finished) {
    auto latency = GetSystemClock() - starttime;
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_io_time_(latency);
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_wr_time_(latency);
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_wrs_(1);
    read_hist_wrs_[ReadHistSlot(tbl_name)].fetch_add(1, std::memory_order_relaxed);
  }
}

void IDataStore::WriteAndRead(const std::string &tbl_name, SearchKey &key, char *data, size_t sz,
                              SearchKey &load_key, std::string &load_data) {
  // g_dump_load_dir: divert only the WRITE half to the local gzip TSV; the
  // read half still has to go to the store, because the read path is what the
  // run is measuring. Without this hook the setting is not neutral between the
  // two systems: Nemo's eviction write-back goes through
  // WriteBatchAsyncCallback (hooked), but PageBufferManager's steady-state
  // eviction takes this call -- evict one dirty page and load its replacement
  // in a single round trip, PageBufferManager.cpp:630 -- so the page baseline
  // would keep paying full price for its writes while Nemo paid none.
  if (g_dump_load_dir[0]) {
    auto store_part = std::to_string(key.ToUInt64() / g_partition_sz);
    DumpWrite(tbl_name, store_part, KeyStr(tbl_name, key), data, sz);
    if (g_dump_write_latency_us) usleep(g_dump_write_latency_us);
    Read(tbl_name, load_key, load_data);  // charges its own remote_rd_ stats
    return;
  }
  auto starttime = GetSystemClock();
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  client_->StoreAndLoadSync(tbl_id, key, data, sz, load_key, load_data);
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_TIKV
  std::string value(data, sz);
  client_->StoreSync(tbl_id, key, value);
  client_->LoadSync(load_tbl_id, load_key, load_data);
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE || \
      REMOTE_STORAGE_TYPE == REMOTE_STORAGE_SIMULATED
  auto store_part = std::to_string(key.ToUInt64() / g_partition_sz);
  auto load_part = std::to_string(load_key.ToUInt64() / g_partition_sz);
  client_->StoreAndLoadSync(tbl_name, store_part, KeyStr(tbl_name, key), data,
                            sz, load_part, KeyStr(tbl_name, load_key),
                            load_data);
#endif
  if (g_warmup_finished) {
    auto latency = GetSystemClock() - starttime;
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_io_time_(latency);
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_rw_time_(latency);
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_rws_(1);
    read_hist_rws_[ReadHistSlot(tbl_name)].fetch_add(1, std::memory_order_relaxed);
  }
}

void IDataStore::WriteBatch(const std::string &tbl_name, std::string part,
                            std::multimap<std::string, std::string> &map) {
  // used only when data forcing is enabled
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  client_->BatchStoreSync(tbl_name, std::move(map));
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_TIKV
  client_->BatchStoreSync(tbl_name, std::move(map));
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE || \
      REMOTE_STORAGE_TYPE == REMOTE_STORAGE_SIMULATED
  client_->BatchStoreSync(tbl_name, part, map);
#endif
}

void IDataStore::WriteBatchAsyncCallback(const std::string &tbl_name, std::string partition, const std::multimap<std::string, std::string> &data, std::function<void()> callback) {
  // g_dump_load_dir on the measurement path: send eviction write-backs to the
  // local gzip TSV instead of the store, the same hook IDataStore::Write already has.
  // The point is to take the engineering defect out of the measurement -- the
  // wastorage entity-group txn is unusable against Cosmos, so this "batch"
  // write degrades to a serial per-row loop (110 rows x ~20 ms), and the slot
  // supply that eviction provides became the ceiling once the latch bug was
  // fixed (rw_await_evict_time_ 90 % at the 75 % point). Dumping locally makes
  // write-back disk-speed so the run measures the read path.
  // The dumped rows are NOT applied to remote storage: the dataset diverges,
  // exactly as it already does under g_evict_flush_outside_latch.
  if (g_dump_load_dir[0]) {
    for (auto &item : data) {
      DumpWrite(tbl_name, partition, item.first,
                (char *) item.second.data(), item.second.size());
    }
    // g_dump_write_latency_us models what the write WOULD have cost remotely,
    // charged per row. Local dumping alone measures a machine with free
    // writes, which deletes a real cost of record granularity; sweeping this
    // instead gives throughput as a function of per-row write cost, and the
    // measured Cosmos value can be read off that curve rather than assumed.
    // Per row, not per batch, because that is the unit both regimes are quoted
    // in: today's path pays one round trip per row (~5.9 ms p50 uncontended,
    // ~16 ms under the eviction pool's own concurrency), while a working
    // entity-group transaction would amortise one round trip over 100 rows.
    if (g_dump_write_latency_us)
      usleep(g_dump_write_latency_us * data.size());
    callback();
    return;
  }
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE || \
    REMOTE_STORAGE_TYPE == REMOTE_STORAGE_SIMULATED
  client_->WriteBatchAsync(tbl_name, std::move(partition), data, callback);
#else
  // Implementations for other storage types can be added here.
  LOG_ERROR("WriteBatchAsync is not implemented for the current storage type.");
#endif
}

void IDataStore::WriteBatchAsync(const std::string &tbl_name, std::string part,
                            std::multimap<std::string, std::string> &map,
                            volatile bool& is_done) {
  // used only when data forcing is enabled
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  client_->BatchStoreASync(tbl_name, std::move(map));
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_TIKV
  client_->BatchStoreASync(tbl_name, std::move(map));
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE || \
      REMOTE_STORAGE_TYPE == REMOTE_STORAGE_SIMULATED
  client_->BatchStoreAsync(tbl_name, std::move(part), map, is_done);
#endif
}

void IDataStore::WriteAsync(const std::string &tbl_name, SearchKey &key,
                           char *data, size_t sz, volatile RC *rc) {
  auto partition = std::to_string(key.ToUInt64() / g_partition_sz);
  return client_->StoreAsync(tbl_name, partition, KeyStr(tbl_name, key), data,
                             sz, rc);
}

// g_scan_cpp_query sends a range fetch through the C++ SDK's table query
// instead of the embedded interpreter (see AzureTableClient::LoadRangeCpp for
// the one header that makes Cosmos accept it, and for the measured gap). Only
// the Azure build has that method -- the simulated store has its own
// LoadRangePython -- so the choice has to be made at compile time.
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE
#define RANGE_FETCH(tbl, part, lo, hi, buf, sz, cnt)                    \
  do {                                                                  \
    if (g_scan_cpp_query)                                               \
      client_->LoadRangeCpp((tbl), (part), (lo), (hi), (buf), (sz), (cnt)); \
    else                                                                \
      client_->LoadRangePython((tbl), (part), (lo), (hi), (buf), (sz), (cnt)); \
  } while (0)
#else
#define RANGE_FETCH(tbl, part, lo, hi, buf, sz, cnt) \
  client_->LoadRangePython((tbl), (part), (lo), (hi), (buf), (sz), (cnt))
#endif

void IDataStore::ReadRange(std::string table_name, SearchKey low_key, SearchKey high_key,  char** tuples, size_t total_tuple_sz,
                           size_t& res_cnt) {
  auto starttime = GetSystemClock();
  auto low_part_num = low_key.ToUInt64() / g_partition_sz;
  auto high_part_num = high_key.ToUInt64() / g_partition_sz;
  if (low_part_num == high_part_num) {
    auto low_part = std::to_string(low_part_num);
    auto lo = KeyStr(table_name, low_key), hi = KeyStr(table_name, high_key);
    RANGE_FETCH(table_name, low_part, lo, hi, tuples, total_tuple_sz, res_cnt);
  } else {
    // remote scans cannot cross a partition; split at partition boundaries and merge
    std::vector<std::pair<char *, size_t>> sub_results;
    size_t total_cnt = 0;
    auto sub_low = low_key.ToUInt64();
    for (auto part = low_part_num; part <= high_part_num; part++) {
      auto sub_high = std::min(high_key.ToUInt64(), (part + 1) * g_partition_sz - 1);
      char * sub_buf = nullptr;
      size_t sub_cnt = 0;
      auto part_str = std::to_string(part);
      auto lo = KeyStr(table_name, SearchKey(sub_low));
      auto hi = KeyStr(table_name, SearchKey(sub_high));
      RANGE_FETCH(table_name, part_str, lo, hi, &sub_buf, total_tuple_sz, sub_cnt);
      if (sub_cnt > 0) {
        sub_results.emplace_back(sub_buf, sub_cnt);
        total_cnt += sub_cnt;
      }
      sub_low = sub_high + 1;
    }
    res_cnt = total_cnt;
    if (total_cnt > 0) {
      *tuples = (char *) MemoryAllocator::Alloc(total_tuple_sz * total_cnt);
      size_t offset = 0;
      for (auto & sub : sub_results) {
        memcpy(*tuples + offset, sub.first, total_tuple_sz * sub.second);
        offset += total_tuple_sz * sub.second;
        DEALLOC(sub.first);
      }
    }
  }
  if (g_warmup_finished) {
    auto latency = GetSystemClock() - starttime;
    LOG_XXX("ReadRange from %s, low_key: %s, high_key: %s, scan_sz: %d, latency: %.2f us",
              table_name.c_str(), low_key.ToString().c_str(), high_key.ToString().c_str(), res_cnt,  (latency*1.0)/1000);
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_io_time_(latency);
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_scan_time_(latency);
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_scans_(1);
    read_hist_scans_[ReadHistSlot(table_name)].fetch_add(1, std::memory_order_relaxed);
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_scan_sz_(res_cnt);
  }
}

} // arboretum