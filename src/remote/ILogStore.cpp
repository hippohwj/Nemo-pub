#include "ILogStore.h"
#include "IDataStore.h"
#include "common/Worker.h"
#include "common/Stats.h"
#include <cstdio>
#include <mutex>
#include <unistd.h>
#include <vector>
#include <string>
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_SIMULATED
#include "SimNetClient.h"
#include "SimTableClient.h"
#endif

namespace arboretum {

ILogStore::ILogStore() {
  storage_id_ = (g_index_type == TWOTREE)? g_bottom_dataset_id:g_dataset_id;
  storage_id_ += "-log";
  LOG_INFO("Log store name %s", storage_id_.c_str());
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE
  std::ifstream in(azure_config_fname);
  std::string azure_conn_str;
  azure_conn_str.resize(500);
  getline(in, azure_conn_str);
  client_ = NEW(AzureTableClient)(azure_conn_str);
  client_->CreateTable(storage_id_);
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_SIMULATED
  if (SimNetEnabled()) {
    client_ = NEW(SimNetClient)();
  } else {
    client_ = NEW(SimTableClient)();
  }
  client_->CreateTable(storage_id_);
#else
  std::ifstream in(redis_config_fname);
  std::string line;
  auto cnt = 0;
  std::vector<std::string> lines;
  while (getline(in, line)) {
    if (line[0] == '#') continue;
    cnt++;
    lines.push_back(line);
  }
  client_ = NEW(RedisClient)(lines[3], std::stol(lines[4]), lines[0]);
#endif
}

void ILogStore::PeriodicLog(ILogStore * log_store) {
  while (!g_terminate_exec) {
    std::unique_lock<std::mutex> lk(log_store->buffer_latch_);
    // LOG_DEBUG("waiting for log buffer to be filled;");
    log_store->buffer_cv_.wait_for(lk, std::chrono::microseconds(g_log_freq_us),
                 [log_store] { return log_store->txns_.size() >= g_commit_group_sz; });
    if (g_terminate_exec)
      break;
    log_store->Flush(lk);
  }
  log_store->flush_cv_.notify_all();
}



// g_log_write_latency_us: send the log group to local disk and charge it what
// one Cosmos round trip costs, instead of writing it remotely.
//
// The remote path it replaces is not a sound baseline to charge from. Every
// entity in a group is built with the SAME PartitionKey and RowKey
// (flush_lsn_ % 1000, cnt_), so an entity-group transaction is rejected for
// duplicate keys and the per-row fallback has each record overwrite the last:
// only one record per group survives. Nothing reads log payloads back, which
// is why that has gone unnoticed. Measured consequence with commits not
// waiting (g_enable_log=false): 3-8 flushes in a 30 s window at p50 2.6-4.2 s,
// against the simulator's 5394 flushes at p50 5.45 ms -- i.e. the log was
// effectively bypassed in every Cosmos run so far.
//
// So the cost modelled here is what a CORRECT log would pay: one round trip
// per commit group. g_log_write_latency_us is charged per flush, not per
// record. 5860 us is the probe's p50 for a single-entity write against this
// account (108 B row, one thread, uncontended), which is also within 8 % of
// the simulator's injected g_sim_log_latency_us = 5400.
static FILE *LogDumpPipe() {
  static FILE *pipe = nullptr;
  static std::mutex m;
  std::lock_guard<std::mutex> g(m);
  if (!pipe) {
    char cmd[256];
    // g_dump_load_dir when the data path is also local, otherwise the working
    // directory: this switch is independent of that one, so it cannot assume
    // that directory is set
    snprintf(cmd, sizeof(cmd), "gzip -1 >> %s/logflush.bin.gz",
             g_dump_load_dir[0] ? g_dump_load_dir : ".");
    pipe = popen(cmd, "w");
    M_ASSERT(pipe, "cannot start log dump pipe in %s",
             g_dump_load_dir[0] ? g_dump_load_dir : ".");
  }
  return pipe;
}

void ILogStore::Flush(std::unique_lock<std::mutex> & lk) {
  if (txns_.size() == 0)
    return;
  auto sz = txns_.size();
  size_t log_sz = 0;
  // prepare log
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE
  azure::storage::table_batch_operation batch_op;
  for (size_t i = 0; i < sz; i++) {
    azure::storage::table_entity tuple(U(std::to_string(flush_lsn_ % 1000)),
                                       U(std::to_string(cnt_)));
    auto& col = tuple.properties();
    col.reserve(1);
    col[U("Data")] = azure::storage::entity_property(
        std::vector<uint8_t>(data_[i].begin(), data_[i].end()));
    batch_op.insert_or_replace_entity(tuple);
    log_sz += data_[i].size();
  }
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_SIMULATED
  // network mode ships the real bytes so the transfer scales with the commit
  // group; the in-process client only looks at the size
  std::string sim_payload;
  for (size_t i = 0; i < sz; i++) {
    log_sz += data_[i].size();
  }
  if (SimNetEnabled()) {
    sim_payload.reserve(log_sz);
    for (size_t i = 0; i < sz; i++) {
      sim_payload.append(data_[i]);
    }
  }
#else
  for (size_t i = 0; i < sz; i++) {
    log_sz += data_[i].size();
  }
#endif
  // the dump sink writes the real bytes, so they have to outlive the erase
  std::vector<std::string> flushed_payloads;
  if (g_log_write_latency_us) {
    flushed_payloads.assign(data_.begin(), data_.begin() + sz);
  }
  // remove flushed items
  txns_.erase(txns_.begin(), txns_.begin() + sz);
  data_.erase(data_.begin(), data_.begin() + sz);
  lk.unlock();
  uint64_t starttime = GetSystemClock();
  uint64_t flush_interval = starttime - flush_ts_;
  flush_ts_ = starttime;
  // flush log
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE
  // gated on the LOG knob, not on g_dump_load_dir: the two paths have to move
  // independently. Sharing the data path's switch meant that turning data
  // writes back on for Cosmos silently returned the log to its broken remote
  // path as well, so a run meant to price real writes would have changed two
  // things at once.
  if (g_log_write_latency_us) {
    auto f = LogDumpPipe();
    for (auto &rec : flushed_payloads) fwrite(rec.data(), 1, rec.size(), f);
    usleep(g_log_write_latency_us);
  } else {
    client_->BatchStoreSync(storage_id_, batch_op);
  }
  cnt_++;
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_SIMULATED
  // log payloads are never read back: latency (and, in network mode, the
  // transfer of the real bytes) is the entire cost
  client_->LogFlush(storage_id_, sim_payload.data(), log_sz);
  cnt_++;
#endif
  flush_lsn_ += sz;
  flush_cv_.notify_all();
  // collect stats
  if (g_warmup_finished) {
    auto latency = GetSystemClock() - starttime;
    g_stats->commit_stats_[0].incr_num_flushes_(1);
    g_stats->commit_stats_[0].incr_log_group_sz_(sz);
    g_stats->commit_stats_[0].incr_log_group_payload_sz_(log_sz);
    g_stats->commit_stats_[0].incr_log_flush_interval_(flush_interval);
    g_stats->commit_stats_[0].incr_log_flush_latency_(latency);
  }
}

uint64_t ILogStore::AppendToLogBuffer(OID txn_id, std::string &log) {
  // add log to log buffer
  std::unique_lock<std::mutex> lk(buffer_latch_);
  txns_.push_back(txn_id);
  data_.push_back(log);
  auto lsn = ++log_lsn_;
  buffer_cv_.notify_all();
  return lsn;
}

bool ILogStore::WaitForFlush(uint64_t lsn) {
  if (g_enable_log) {
      std::unique_lock<std::mutex> lk2(flush_latch_);
  // check every ms in case db is terminated
      while (!g_terminate_exec) {
        if (flush_cv_.wait_for(lk2, std::chrono::microseconds(1000),
                     [this, lsn]{ return this->flush_lsn_ >= lsn; })) {
          return true;
        }
      }
      return false;
  } else {
    // if logging is disabled, we can return immediately
    return true;
  }

}

}