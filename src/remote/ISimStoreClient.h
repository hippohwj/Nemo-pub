#ifndef ARBORETUM_SRC_REMOTE_ISIMSTORECLIENT_H_
#define ARBORETUM_SRC_REMOTE_ISIMSTORECLIENT_H_

#include "common/Common.h"
// stands in for SimTableClient.h in IDataStore.h/ILogStore.h, so keep its
// transitive include surface (several .cpps pick up Utils.h through here)
#include "common/Utils.h"

#include <functional>
#include <map>
#include <string>

namespace arboretum {

// Common surface of the simulated-store clients, mirroring the
// AzureTableClient subset used by IDataStore/ILogStore. Two implementations
// selected at runtime by g_sim_server_addr:
//   ""          -> SimTableClient  (in-process store, latency injected locally)
//   "host:port" -> SimNetClient    (RPC to a SimStoreServer process, latency
//                                   injected server-side before the reply)
class ISimStoreClient {
 public:
  virtual ~ISimStoreClient() = default;

  virtual void CreateTable(const std::string &tbl_name) = 0;
  virtual void DeleteTable(const std::string &tbl_name) = 0;

  virtual RC LoadSync(const std::string &tbl_name, std::string partition,
                      const std::string &key, std::string &data) = 0;
  virtual RC LoadSyncQuery(const std::string &tbl_name, std::string partition,
                           const std::string &key, std::string &data) = 0;
  virtual uint64_t LoadNumericSync(const std::string &tbl_name,
                                   std::string partition,
                                   const std::string &key) = 0;
  virtual void StoreSync(const std::string &tbl_name, std::string partition,
                         const std::string &key, char *data, size_t sz) = 0;
  virtual void StoreNumericSync(const std::string &tbl_name,
                                std::string partition, const std::string &key,
                                int64_t number) = 0;
  virtual void StoreAndLoadSync(const std::string &tbl_name,
                                std::string store_part, const std::string &key,
                                char *data, size_t sz, std::string load_part,
                                const std::string &load_key,
                                std::string &load_data) = 0;

  virtual void StoreAsync(const std::string &tbl_name, std::string partition,
                          const std::string &key, char *data, size_t sz,
                          volatile RC *rc) = 0;
  virtual void BatchStoreSync(const std::string &tbl_name, std::string part,
                              const std::multimap<std::string, std::string> &map) = 0;
  virtual void BatchStoreAsync(const std::string &tbl_name, std::string part,
                               const std::multimap<std::string, std::string> &map,
                               volatile bool &is_done) = 0;
  virtual void WriteBatchAsync(const std::string &tbl_name,
                               std::string partition,
                               const std::multimap<std::string, std::string> &data,
                               std::function<void()> callback) = 0;
  virtual void WaitForAsync(int cnt) {}

  virtual void LoadRangePython(std::string &tbl_name, std::string &partition,
                               std::string low_key, std::string high_key,
                               char **tuples, size_t total_tuple_sz,
                               size_t &res_cnt) = 0;

  // group-commit flush; payload may be nullptr in-process (content is never
  // read back), network mode sends the real bytes so transfer scales with
  // group size
  virtual void LogFlush(const std::string &tbl_name, const char *payload,
                        size_t payload_sz) = 0;

  virtual void Checkpoint() = 0;
};

} // namespace arboretum

#endif // ARBORETUM_SRC_REMOTE_ISIMSTORECLIENT_H_
