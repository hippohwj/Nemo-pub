#ifndef ARBORETUM_SRC_REMOTE_SIMTABLECLIENT_H_
#define ARBORETUM_SRC_REMOTE_SIMTABLECLIENT_H_

#include "common/Common.h"
#include "common/Utils.h"
#include "ISimStoreClient.h"

#include <chrono>
#include <deque>
#include <map>
#include <shared_mutex>
#include <thread>

namespace arboretum {

// In-memory stand-in for AzureTableClient: every request completes after a
// configurable deterministic latency (g_sim_*_latency_us), with unlimited
// concurrency, no request caps, and no retries. Mirrors the AzureTableClient
// call surface 1:1 so the IDataStore/ILogStore dispatch code is unchanged.
// Tables snapshot to <g_sim_snapshot_dir>/<tbl_name>.simtbl so datasets are
// loaded once and reused across runs (see Checkpoint / TryRestoreTable).
// SimStoreServer embeds this class with inject_latency=false: the server's
// deadline scheduler owns all injection there.
class SimTableClient : public ISimStoreClient {
 public:
  explicit SimTableClient(bool inject_latency = true);
  ~SimTableClient() override;

  void CreateTable(const std::string &tbl_name) override;
  void DeleteTable(const std::string &tbl_name) override;

  RC LoadSync(const std::string &tbl_name, std::string partition,
              const std::string &key, std::string &data) override;
  RC LoadSyncQuery(const std::string &tbl_name, std::string partition,
                   const std::string &key, std::string &data) override {
    return LoadSync(tbl_name, std::move(partition), key, data);
  }
  uint64_t LoadNumericSync(const std::string &tbl_name, std::string partition,
                           const std::string &key) override;
  void StoreSync(const std::string &tbl_name, std::string partition,
                 const std::string &key, char *data, size_t sz) override;
  void StoreNumericSync(const std::string &tbl_name, std::string partition,
                        const std::string &key, int64_t number) override;
  void StoreAndLoadSync(const std::string &tbl_name, std::string store_part,
                        const std::string &key, char *data, size_t sz,
                        std::string load_part, const std::string &load_key,
                        std::string &load_data) override;

  void StoreAsync(const std::string &tbl_name, std::string partition,
                  const std::string &key, char *data, size_t sz,
                  volatile RC *rc) override;
  void BatchStoreSync(const std::string &tbl_name, std::string part,
                      const std::multimap<std::string, std::string> &map) override;
  void BatchStoreAsync(const std::string &tbl_name, std::string part,
                       const std::multimap<std::string, std::string> &map,
                       volatile bool &is_done) override;
  void WriteBatchAsync(const std::string &tbl_name, std::string partition,
                       const std::multimap<std::string, std::string> &data,
                       std::function<void()> callback) override;

  // native replacement of AzurePythonHelper::ExecPyRangeSearch, replicating
  // its buffer contract: one total_tuple_sz slot per result row, payload
  // copied at offset sizeof(ITuple) within the slot
  void LoadRangePython(std::string &tbl_name, std::string &partition,
                       std::string low_key, std::string high_key,
                       char **tuples, size_t total_tuple_sz,
                       size_t &res_cnt) override;

  // group-commit log flush: latency only, log content is never read back
  // (payload is ignored in-process)
  void LogFlush(const std::string &tbl_name, const char *payload,
                size_t payload_sz) override;

  // dump every non-log table to <g_sim_snapshot_dir>/<tbl_name>.simtbl
  void Checkpoint() override;

  // raw range read for SimStoreServer: values of rows in [low, high] within
  // the partition, in key order (the client rebuilds the tuple-slot buffer)
  void LoadRangeValues(const std::string &tbl_name, const std::string &partition,
                       const std::string &low_key, const std::string &high_key,
                       std::vector<std::string> &values);

  // deterministic latency: sleep to ~100us before the deadline, spin the rest
  // (public: the SimStoreServer scheduler reuses it and the latency-class
  // helpers below)
  static void InjectNs(uint64_t ns);
  static uint64_t RWLatencyUs() {
    return g_sim_rw_latency_us != 0
               ? g_sim_rw_latency_us
               : std::max(g_sim_rd_latency_us, g_sim_wr_latency_us);
  }
  static uint64_t LogLatencyUs() {
    return g_sim_log_latency_us != 0 ? g_sim_log_latency_us
                                     : g_sim_batch_latency_us;
  }

 private:
  struct SimPartition {
    std::shared_mutex latch_;
    std::map<std::string, std::string> rows_;
  };
  struct SimTable {
    std::shared_mutex latch_;
    std::unordered_map<std::string, SimPartition *> parts_;
  };

  SimTable *GetTable(const std::string &tbl_name);
  SimPartition *GetPartition(SimTable *tbl, const std::string &part);
  void ApplyWrite(const std::string &tbl_name, const std::string &part,
                  const std::string &key, std::string value);
  void ApplyBatch(const std::string &tbl_name, const std::string &part,
                  const std::vector<std::pair<std::string, std::string>> &rows);
  // called with tables_latch_ held exclusively, before the table is visible
  void TryRestoreTable(const std::string &tbl_name, SimTable *tbl);
  void DumpTable(const std::string &tbl_name, SimTable *tbl);

  void InjectUs(uint64_t us) const {
    // g_sim_warmup_no_latency: charge the calibrated remote latency only in
    // the measurement window. Nemo's warm-up is already latency-free -- it
    // generates rows locally (g_generative_warmup) and skips the log
    // (g_warmup_skip_log) -- while the page baseline pays the full remote
    // latency for every warm-up page AND every restore pre-load page. That
    // asymmetry, not page granularity, can keep the page baseline from
    // reaching steady state inside the warm-up window. Set it on the page
    // column only; leaving it off reproduces the old behaviour exactly.
    if (g_sim_warmup_no_latency && !g_warmup_finished) return;
    if (inject_) InjectNs(us * 1000);
  }

  // async executor: tasks apply their mutation at a fixed deadline, then set
  // the caller's flag / fire its callback
  void SubmitTask(uint64_t latency_us, std::function<void()> apply);
  static void ExecutorLoop(SimTableClient *client);

  // false when embedded in SimStoreServer: the server-side deadline
  // scheduler owns all latency injection
  const bool inject_;

  std::shared_mutex tables_latch_;
  std::unordered_map<std::string, SimTable *> tables_;

  std::mutex task_latch_;
  std::condition_variable task_cv_;
  std::multimap<std::chrono::steady_clock::time_point, std::function<void()>>
      tasks_;
  std::vector<std::thread> executors_;
  bool stop_{false};
};

} // namespace arboretum

#endif // ARBORETUM_SRC_REMOTE_SIMTABLECLIENT_H_
