#ifndef ARBORETUM_SRC_REMOTE_SIMNETCLIENT_H_
#define ARBORETUM_SRC_REMOTE_SIMNETCLIENT_H_

#include "ISimStoreClient.h"

#include <atomic>
#include <mutex>
#include <thread>
#include <unordered_map>

namespace arboretum {

// Network-mode simulated-store client: translates the
// ISimStoreClient surface into binary RPCs against a SimStoreServer at
// g_sim_server_addr. All latency is injected server-side; this class never
// reads the g_sim_*_latency_us knobs.
//
// Threading model:
// - sync ops run lockstep request/reply on a lazily-created thread-local
//   connection (one per engine thread per client instance), like the Azure
//   SDK's sync calls;
// - async ops (the eviction paths) share ONE multiplexed channel: the caller
//   serializes and sends under a mutex and returns; a receiver thread matches
//   replies by req_id and fires the completion, so in-flight async requests
//   are unbounded (matching the in-process deadline-queue semantics).
//
// Any transport failure is fatal (LOG_ERROR): there is no silent fallback to
// the in-process store.
class SimNetClient : public ISimStoreClient {
 public:
  SimNetClient();
  ~SimNetClient() override;

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

  void LoadRangePython(std::string &tbl_name, std::string &partition,
                       std::string low_key, std::string high_key,
                       char **tuples, size_t total_tuple_sz,
                       size_t &res_cnt) override;

  void LogFlush(const std::string &tbl_name, const char *payload,
                size_t payload_sz) override;

  void Checkpoint() override;

 private:
  using Completion = std::function<void(uint8_t status, const std::string &)>;

  // connect + HELLO handshake, retrying briefly; LOG_ERROR on failure
  int Connect() const;
  // per-thread sync connection, created lazily; kept in an instance-owned
  // map (closed in the destructor) so instance address reuse can never hand
  // a thread a stale fd. The map lookup takes a short lock per sync op,
  // negligible against ms-scale op latencies.
  int SyncFd();
  // lockstep RPC on the calling thread's connection; returns the status
  uint8_t SyncCall(uint8_t op, const std::string &req, std::string &resp);
  // fire-and-forget send on the async channel; completion runs on the
  // receiver thread when the server replies
  void AsyncCall(uint8_t op, const std::string &req, Completion completion);
  static void ReceiverLoop(SimNetClient *client);

  static std::string EncodeBatch(const std::string &tbl_name,
                                 const std::string &part,
                                 const std::multimap<std::string, std::string> &map);

  std::string host_;
  int port_{0};

  std::mutex sync_latch_;
  std::unordered_map<std::thread::id, int> sync_fds_;

  std::atomic<uint64_t> next_req_id_{1};
  int async_fd_{-1};
  std::mutex send_latch_;
  std::mutex pending_latch_;
  std::unordered_map<uint64_t, Completion> pending_;
  std::thread receiver_;
  std::atomic<bool> stop_{false};
};

} // namespace arboretum

#endif // ARBORETUM_SRC_REMOTE_SIMNETCLIENT_H_
