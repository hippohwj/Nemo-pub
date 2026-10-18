#ifndef ARBORETUM_DISTRIBUTED_SRC_TRANSPORT_DATASTORE_H_
#define ARBORETUM_DISTRIBUTED_SRC_TRANSPORT_DATASTORE_H_

#include "common/Common.h"
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
#include "RedisClient.h"
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_TIKV
#include "TiKVClient.h"
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE
#include "AzureTableClient.h"
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_SIMULATED
#include "ISimStoreClient.h"
#endif

namespace arboretum {

class IDataStore {
 public:
  IDataStore();
  // debug metrics: per-table remote-read AND remote-write attribution, counted
  // post-warmup at the same sites as remote_rds_/remote_scans_/remote_rws_/
  // remote_wrs_ (so the histogram sums to those stats). Slots come from
  // RemoteTableSlot: data tables at 0..30, index tables at 32+t, everything
  // else in the last slot. Dumped by PrintReadHist() from ARDB::Terminate,
  // and keyed identically to the g_thr429_tbl throttle histogram so the two
  // can be read together -- which table is starved, and of what.
  static constexpr int kReadHistSlots = kThrottleSlots;
  static int ReadHistSlot(const std::string &tbl_name);
  void PrintReadHist();
  // the engine never destroys its store; unit tests do, and a net-mode
  // client must shut down before the server it is connected to
  ~IDataStore() {
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_SIMULATED
    client_->~ISimStoreClient();
    DEALLOC(client_);
#endif
  }
  RC Read(const std::string &tbl_name, SearchKey &key, std::string &data);
  RC ReadQuery(const std::string &tbl_name, SearchKey &key, std::string &data);
  void ReadRange(std::string table_name, SearchKey low_key, SearchKey high_key,  char** tuples, size_t total_tuple_sz,
    size_t& res_cnt);
  // void ReadRange(std::string table_name, SearchKey low_key, SearchKey high_key, char **data_ptrs, size_t &res_cnt);
  void Write(const std::string &tbl_name, SearchKey &key, char * data, size_t sz);
  void WriteAsync(const std::string &tbl_name, SearchKey &key, char * data, size_t sz, volatile RC *rc);
  void WriteAndRead(const std::string &tbl_name, SearchKey &key, char *data, size_t sz,
                    SearchKey &load_key, std::string &load_data);
  void WriteBatch(const std::string &tbl_name, std::string part, std::multimap<std::string, std::string> &map);
  void WriteBatchAsync(const std::string &tbl_name, std::string part, std::multimap<std::string, std::string> &map,
                       volatile bool& is_done);
  void WriteBatchAsyncCallback(const std::string &tbl_name, std::string partition, const std::multimap<std::string, std::string> &data, std::function<void()> callback);
  void CreateTable(const std::string &tbl_name) {
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE || \
    REMOTE_STORAGE_TYPE == REMOTE_STORAGE_SIMULATED
    client_->CreateTable(tbl_name);
#endif
  };
  // for populating data in remote
  void WaitForAsyncBatchLoading(int cnt) {
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE || \
    REMOTE_STORAGE_TYPE == REMOTE_STORAGE_SIMULATED
    client_->WaitForAsync(cnt);
#endif
  }
  // simulated store only: persist tables so later runs can restore; no-op on
  // real backends (their data persists on the remote service)
  void Checkpoint() {
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_SIMULATED
    client_->Checkpoint();
#endif
  }
  // for metadata
  int64_t Read(const std::string &tbl_name, std::string part_key, std::string key);
  RC Read(const std::string &tbl_name, std::string part_key, std::string key, std::string &data);
  void Write(const std::string &tbl_name, std::string part_key, std::string key, int64_t num);
  void Write(const std::string &tbl_name, std::string part_key, std::string key, char * data, size_t sz);
  int64_t CheckSize();

 private:
  std::atomic<uint64_t> read_hist_rds_[kReadHistSlots]{};
  std::atomic<uint64_t> read_hist_scans_[kReadHistSlots]{};
  std::atomic<uint64_t> read_hist_rws_[kReadHistSlots]{};
  // plain remote writes per table, beside the per-table read attribution
  // above. For a write-heavy baseline the write side is most of the remote
  // cost, and sizing a per-table RU plan needs to know WHICH table takes
  // the eviction write-backs.
  std::atomic<uint64_t> read_hist_wrs_[kReadHistSlots]{};
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  RedisClient * client_;
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_TIKV
  TiKVClient * client_;
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE
  AzureTableClient * client_;
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_SIMULATED
  // SimTableClient (in-process) or SimNetClient, by g_sim_server_addr
  ISimStoreClient * client_;
#endif
};

} // arboretum

#endif //ARBORETUM_DISTRIBUTED_SRC_TRANSPORT_DATASTORE_H_
