#include "SimTableClient.h"
#include "local/ITuple.h"

#include <filesystem>

namespace arboretum {

// spin margin before the deadline: sleep_until wakes ~this early instead of
// overshooting, and the remainder is spun for sub-us stability
static constexpr auto kSpinMarginNs = std::chrono::nanoseconds(100 * 1000);

void SimTableClient::InjectNs(uint64_t ns) {
  if (ns == 0) return;
  auto deadline = std::chrono::steady_clock::now() + std::chrono::nanoseconds(ns);
  auto wake = deadline - kSpinMarginNs;
  if (wake > std::chrono::steady_clock::now()) {
    std::this_thread::sleep_until(wake);
  }
  while (std::chrono::steady_clock::now() < deadline) {
#if defined(__x86_64__) || defined(__i386__)
    __asm__ __volatile__("pause");
#endif
  }
}

SimTableClient::SimTableClient(bool inject_latency) : inject_(inject_latency) {
  for (size_t i = 0; i < g_sim_async_thds; i++) {
    executors_.emplace_back(SimTableClient::ExecutorLoop, this);
  }
  LOG_INFO("Created simulated table client (rd/wr/rw/scan/batch/log latency "
           "= %lu/%lu/%lu/%lu/%lu/%lu us, %lu async threads)",
           g_sim_rd_latency_us, g_sim_wr_latency_us, RWLatencyUs(),
           g_sim_scan_latency_us, g_sim_batch_latency_us, LogLatencyUs(),
           g_sim_async_thds);
}

SimTableClient::~SimTableClient() {
  {
    std::unique_lock<std::mutex> lk(task_latch_);
    stop_ = true;
  }
  task_cv_.notify_all();
  for (auto &thd : executors_) thd.join();
}

// -------------------- table / partition access --------------------

SimTableClient::SimTable *SimTableClient::GetTable(const std::string &tbl_name) {
  {
    std::shared_lock<std::shared_mutex> lk(tables_latch_);
    auto it = tables_.find(tbl_name);
    if (it != tables_.end()) return it->second;
  }
  std::unique_lock<std::shared_mutex> lk(tables_latch_);
  auto it = tables_.find(tbl_name);
  if (it != tables_.end()) return it->second;
  auto tbl = new SimTable();
  TryRestoreTable(tbl_name, tbl);
  tables_[tbl_name] = tbl;
  return tbl;
}

SimTableClient::SimPartition *SimTableClient::GetPartition(
    SimTable *tbl, const std::string &part) {
  {
    std::shared_lock<std::shared_mutex> lk(tbl->latch_);
    auto it = tbl->parts_.find(part);
    if (it != tbl->parts_.end()) return it->second;
  }
  std::unique_lock<std::shared_mutex> lk(tbl->latch_);
  auto it = tbl->parts_.find(part);
  if (it != tbl->parts_.end()) return it->second;
  auto p = new SimPartition();
  tbl->parts_[part] = p;
  return p;
}

void SimTableClient::CreateTable(const std::string &tbl_name) {
  if (StrEndsWith(tbl_name, "log")) {
    // match AzureTableClient: log tables start empty on every run
    DeleteTable(tbl_name);
  }
  GetTable(tbl_name);
  LOG_DEBUG("Created simulated table %s", tbl_name.c_str());
}

void SimTableClient::DeleteTable(const std::string &tbl_name) {
  std::unique_lock<std::shared_mutex> lk(tables_latch_);
  auto it = tables_.find(tbl_name);
  if (it != tables_.end()) {
    for (auto &pair : it->second->parts_) delete pair.second;
    delete it->second;
    tables_.erase(it);
  }
}

// -------------------- sync point operations --------------------

RC SimTableClient::LoadSync(const std::string &tbl_name, std::string partition,
                            const std::string &key, std::string &data) {
  InjectUs(g_sim_rd_latency_us);
  auto tbl = GetTable(tbl_name);
  auto part = GetPartition(tbl, partition);
  std::shared_lock<std::shared_mutex> lk(part->latch_);
  auto it = part->rows_.find(key);
  if (it == part->rows_.end()) {
    // diagnostic (2tree TPC-C integration): dump the table state on a miss
    // during startup so key-encoding/partition mismatches are visible
    if (!g_warmup_finished) {
      std::string sample;
      size_t nparts;
      {
        std::shared_lock<std::shared_mutex> tlk(tbl->latch_);
        nparts = tbl->parts_.size();
        int shown = 0;
        for (auto &pp : tbl->parts_) {
          if (shown++ >= 4) break;
          sample += " [part " + pp.first + ": " +
                    std::to_string(pp.second->rows_.size()) + " rows";
          if (!pp.second->rows_.empty())
            sample += ", first key " + pp.second->rows_.begin()->first;
          sample += "]";
        }
      }
      LOG_INFO("sim miss: tbl=%s part=%s key=%s; table has %zu partitions:%s",
               tbl_name.c_str(), partition.c_str(), key.c_str(), nparts,
               sample.c_str());
    }
    return RC::NOT_FOUND;
  }
  data = it->second;
  return RC::OK;
}

uint64_t SimTableClient::LoadNumericSync(const std::string &tbl_name,
                                         std::string partition,
                                         const std::string &key) {
  InjectUs(g_sim_rd_latency_us);
  auto part = GetPartition(GetTable(tbl_name), partition);
  std::shared_lock<std::shared_mutex> lk(part->latch_);
  auto it = part->rows_.find(key);
  if (it == part->rows_.end() || it->second.size() != sizeof(int64_t)) {
    LOG_ERROR("Cannot load data %s from table %s", key.c_str(),
              tbl_name.c_str());
  }
  int64_t num;
  memcpy(&num, it->second.data(), sizeof(int64_t));
  return (uint64_t) num;
}

void SimTableClient::ApplyWrite(const std::string &tbl_name,
                                const std::string &part_name,
                                const std::string &key, std::string value) {
  auto part = GetPartition(GetTable(tbl_name), part_name);
  std::unique_lock<std::shared_mutex> lk(part->latch_);
  part->rows_[key] = std::move(value);
}

void SimTableClient::ApplyBatch(
    const std::string &tbl_name, const std::string &part_name,
    const std::vector<std::pair<std::string, std::string>> &rows) {
  auto part = GetPartition(GetTable(tbl_name), part_name);
  std::unique_lock<std::shared_mutex> lk(part->latch_);
  for (auto &row : rows) {
    part->rows_[row.first] = row.second;
  }
}

void SimTableClient::StoreSync(const std::string &tbl_name,
                               std::string partition, const std::string &key,
                               char *data, size_t sz) {
  InjectUs(g_sim_wr_latency_us);
  ApplyWrite(tbl_name, partition, key, std::string(data, sz));
}

void SimTableClient::StoreNumericSync(const std::string &tbl_name,
                                      std::string partition,
                                      const std::string &key, int64_t number) {
  InjectUs(g_sim_wr_latency_us);
  ApplyWrite(tbl_name, partition, key,
             std::string((char *) &number, sizeof(int64_t)));
}

void SimTableClient::StoreAndLoadSync(const std::string &tbl_name,
                                      std::string store_part,
                                      const std::string &key, char *data,
                                      size_t sz, std::string load_part,
                                      const std::string &load_key,
                                      std::string &load_data) {
  // Azure issues the write and the read concurrently and waits for both,
  // so the pair costs one round trip
  InjectUs(RWLatencyUs());
  ApplyWrite(tbl_name, store_part, key, std::string(data, sz));
  auto part = GetPartition(GetTable(tbl_name), load_part);
  std::shared_lock<std::shared_mutex> lk(part->latch_);
  auto it = part->rows_.find(load_key);
  if (it == part->rows_.end()) {
    LOG_ERROR("Cannot StoreAndLoad data %s in table %s", load_key.c_str(),
              tbl_name.c_str());
  }
  load_data = it->second;
}

void SimTableClient::BatchStoreSync(
    const std::string &tbl_name, std::string part,
    const std::multimap<std::string, std::string> &map) {
  InjectUs(g_sim_batch_latency_us);
  std::vector<std::pair<std::string, std::string>> rows(map.begin(), map.end());
  ApplyBatch(tbl_name, part, rows);
}

void SimTableClient::LogFlush(const std::string &tbl_name, const char *payload,
                              size_t payload_sz) {
  InjectUs(LogLatencyUs());
}

// -------------------- async operations --------------------

void SimTableClient::SubmitTask(uint64_t latency_us,
                                std::function<void()> apply) {
  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::microseconds(latency_us);
  {
    std::unique_lock<std::mutex> lk(task_latch_);
    tasks_.emplace(deadline, std::move(apply));
  }
  task_cv_.notify_one();
}

void SimTableClient::ExecutorLoop(SimTableClient *client) {
  std::unique_lock<std::mutex> lk(client->task_latch_);
  while (true) {
    client->task_cv_.wait(
        lk, [client] { return client->stop_ || !client->tasks_.empty(); });
    if (client->tasks_.empty()) return; // stopped and drained
    auto it = client->tasks_.begin();
    auto deadline = it->first;
    if (std::chrono::steady_clock::now() < deadline) {
      // wake early if an earlier-deadline task arrives or stop is requested
      client->task_cv_.wait_until(lk, deadline);
      continue;
    }
    auto apply = std::move(it->second);
    client->tasks_.erase(it);
    lk.unlock();
    apply();
    lk.lock();
  }
}

void SimTableClient::StoreAsync(const std::string &tbl_name,
                                std::string partition, const std::string &key,
                                char *data, size_t sz, volatile RC *rc) {
  std::string value(data, sz);
  SubmitTask(inject_ ? g_sim_wr_latency_us : 0,
             [this, tbl_name, partition, key, value = std::move(value), rc] {
               ApplyWrite(tbl_name, partition, key, value);
               *rc = RC::OK;
             });
}

void SimTableClient::BatchStoreAsync(
    const std::string &tbl_name, std::string part,
    const std::multimap<std::string, std::string> &map, volatile bool &is_done) {
  is_done = false;
  std::vector<std::pair<std::string, std::string>> rows(map.begin(), map.end());
  volatile bool *done_ptr = &is_done;
  SubmitTask(inject_ ? g_sim_batch_latency_us : 0,
             [this, tbl_name, part, rows = std::move(rows), done_ptr] {
               ApplyBatch(tbl_name, part, rows);
               *done_ptr = true;
             });
}

void SimTableClient::WriteBatchAsync(
    const std::string &tbl_name, std::string partition,
    const std::multimap<std::string, std::string> &data,
    std::function<void()> callback) {
  std::vector<std::pair<std::string, std::string>> rows(data.begin(),
                                                        data.end());
  SubmitTask(inject_ ? g_sim_batch_latency_us : 0,
             [this, tbl_name, partition, rows = std::move(rows),
              callback = std::move(callback)] {
               ApplyBatch(tbl_name, partition, rows);
               callback();
             });
}

// -------------------- range scan --------------------

void SimTableClient::LoadRangePython(std::string &tbl_name,
                                     std::string &partition,
                                     std::string low_key, std::string high_key,
                                     char **tuples, size_t total_tuple_sz,
                                     size_t &res_cnt) {
  InjectUs(g_sim_scan_latency_us);
  auto part = GetPartition(GetTable(tbl_name), partition);
  std::vector<std::pair<const std::string *, const std::string *>> hits;
  {
    std::shared_lock<std::shared_mutex> lk(part->latch_);
    auto lo = part->rows_.lower_bound(low_key);
    auto hi = part->rows_.upper_bound(high_key);
    for (auto it = lo; it != hi; ++it) {
      hits.emplace_back(&it->first, &it->second);
    }
    res_cnt = hits.size();
    if (res_cnt > 0) {
      // same buffer contract as AzurePythonHelper::ExecPyRangeSearch: one
      // total_tuple_sz slot per row, payload at offset sizeof(ITuple)
      *tuples = (char *) MemoryAllocator::Alloc(total_tuple_sz * res_cnt);
      for (size_t i = 0; i < res_cnt; i++) {
        auto &value = *hits[i].second;
        M_ASSERT(value.size() + sizeof(ITuple) <= total_tuple_sz,
                 "row %s in table %s (%zu B) exceeds the %zu B scan slot",
                 hits[i].first->c_str(), tbl_name.c_str(), value.size(),
                 total_tuple_sz);
        memcpy(*tuples + i * total_tuple_sz + sizeof(ITuple), value.data(),
               value.size());
      }
    }
  }
  if (inject_) InjectNs(res_cnt * g_sim_scan_per_row_ns);
}

void SimTableClient::LoadRangeValues(const std::string &tbl_name,
                                     const std::string &partition,
                                     const std::string &low_key,
                                     const std::string &high_key,
                                     std::vector<std::string> &values) {
  InjectUs(g_sim_scan_latency_us);
  auto part = GetPartition(GetTable(tbl_name), partition);
  {
    std::shared_lock<std::shared_mutex> lk(part->latch_);
    auto lo = part->rows_.lower_bound(low_key);
    auto hi = part->rows_.upper_bound(high_key);
    for (auto it = lo; it != hi; ++it) {
      values.push_back(it->second);
    }
  }
  if (inject_) InjectNs(values.size() * g_sim_scan_per_row_ns);
}

// -------------------- snapshot persistence --------------------

static const char kSnapshotMagic[8] = {'S', 'I', 'M', 'T', 'B', 'L', '0', '1'};

static std::string SnapshotPath(const std::string &tbl_name) {
  return std::string(g_sim_snapshot_dir) + "/" + tbl_name + ".simtbl";
}

template<typename T>
static void WriteBin(std::ofstream &out, T v) {
  out.write((const char *) &v, sizeof(T));
}

static void WriteStr(std::ofstream &out, const std::string &s) {
  WriteBin(out, (uint32_t) s.size());
  out.write(s.data(), (std::streamsize) s.size());
}

template<typename T>
static bool ReadBin(std::ifstream &in, T &v) {
  in.read((char *) &v, sizeof(T));
  return in.good();
}

static bool ReadStr(std::ifstream &in, std::string &s) {
  uint32_t sz;
  if (!ReadBin(in, sz)) return false;
  s.resize(sz);
  in.read(&s[0], sz);
  return in.good();
}

void SimTableClient::DumpTable(const std::string &tbl_name, SimTable *tbl) {
  auto path = SnapshotPath(tbl_name);
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  if (!out) {
    LOG_ERROR("cannot write snapshot %s", path.c_str());
  }
  out.write(kSnapshotMagic, sizeof(kSnapshotMagic));
  std::shared_lock<std::shared_mutex> lk(tbl->latch_);
  WriteBin(out, (uint64_t) tbl->parts_.size());
  uint64_t row_cnt = 0;
  for (auto &pair : tbl->parts_) {
    WriteStr(out, pair.first);
    std::shared_lock<std::shared_mutex> plk(pair.second->latch_);
    WriteBin(out, (uint64_t) pair.second->rows_.size());
    for (auto &row : pair.second->rows_) {
      WriteStr(out, row.first);
      WriteStr(out, row.second);
      row_cnt++;
    }
  }
  out.close();
  LOG_INFO("Snapshotted simulated table %s (%lu rows) to %s", tbl_name.c_str(),
           row_cnt, path.c_str());
}

void SimTableClient::TryRestoreTable(const std::string &tbl_name,
                                     SimTable *tbl) {
  auto path = SnapshotPath(tbl_name);
  std::ifstream in(path, std::ios::binary);
  if (!in) return; // no snapshot: fresh table
  char magic[8];
  in.read(magic, sizeof(magic));
  if (!in.good() || memcmp(magic, kSnapshotMagic, sizeof(magic)) != 0) {
    LOG_ERROR("corrupted snapshot %s", path.c_str());
  }
  uint64_t part_cnt, row_cnt = 0;
  if (!ReadBin(in, part_cnt)) LOG_ERROR("corrupted snapshot %s", path.c_str());
  for (uint64_t p = 0; p < part_cnt; p++) {
    std::string part_name;
    uint64_t rows;
    if (!ReadStr(in, part_name) || !ReadBin(in, rows)) {
      LOG_ERROR("corrupted snapshot %s", path.c_str());
    }
    auto part = new SimPartition();
    tbl->parts_[part_name] = part;
    auto insert_pos = part->rows_.end();
    for (uint64_t r = 0; r < rows; r++) {
      std::string key, value;
      if (!ReadStr(in, key) || !ReadStr(in, value)) {
        LOG_ERROR("corrupted snapshot %s", path.c_str());
      }
      // rows were dumped in key order: hint keeps the insert O(1)
      insert_pos = part->rows_.emplace_hint(insert_pos, std::move(key),
                                            std::move(value));
      row_cnt++;
    }
  }
  LOG_INFO("Restored simulated table %s (%lu rows) from %s", tbl_name.c_str(),
           row_cnt, path.c_str());
}

void SimTableClient::Checkpoint() {
  std::filesystem::create_directories(g_sim_snapshot_dir);
  std::shared_lock<std::shared_mutex> lk(tables_latch_);
  for (auto &pair : tables_) {
    if (StrEndsWith(pair.first, "log")) continue;
    DumpTable(pair.first, pair.second);
  }
}

} // namespace arboretum
