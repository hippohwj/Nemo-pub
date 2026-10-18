#ifndef README_MD_SRC_REMOTE_AZURETABLECLIENT_H_
#define README_MD_SRC_REMOTE_AZURETABLECLIENT_H_

#include "AzurePythonHelper.h"
#include "common/Common.h"
#include "common/Worker.h"
#include "common/Utils.h"

#include <was/storage_account.h>
#include <was/table.h>

namespace arboretum {

// Slot for the per-table 429 histogram. Shared with the read/write histogram
// via RemoteTableSlot so the two are always keyed the same way.
static inline int ThrottleSlot(const std::string &tbl_name) {
  return RemoteTableSlot(tbl_name);
}

// One throttled request: count it, and charge the sleep that follows so the
// share of remote_*_time_ that is backoff rather than service can be read off.
static inline void CountThrottle(const std::string &tbl_name,
                                 std::atomic<uint64_t> &path_cnt, int usec) {
  path_cnt.fetch_add(1, std::memory_order_relaxed);
  g_thr429_tbl[ThrottleSlot(tbl_name)].fetch_add(1, std::memory_order_relaxed);
  g_thr_backoff_ns.fetch_add((uint64_t) usec * 1000, std::memory_order_relaxed);
}

class AzureTableClient {
 public:
  explicit AzureTableClient(const std::string &str) {
    const utility::string_t& storage_connection_string(U(str));
    if (storage_connection_string.find(U("REPLACE_TO_YOUR_")) != utility::string_t::npos) {
      LOG_ERROR("Storage connection string contains REPLACE_TO_YOUR_XXX in ifconfig_azure.txt, please replace it with your own storage connection string.");
    }
    storage_acct_ = azure::storage::cloud_storage_account::parse(storage_connection_string);
    table_client_ = storage_acct_.create_cloud_table_client();
    python_helper_ = NEW(AzurePythonHelper)(str);
    LOG_DEBUG("Created azure table client. ");
  }

  void CreateTable(const std::string &tbl_name) {
    tables_[tbl_name] = table_client_.get_table_reference(U(tbl_name));
    if (StrEndsWith(tbl_name, "log")) {
      try {
        tables_[tbl_name].delete_table_if_exists();
        LOG_DEBUG("deleted table %s", tbl_name.c_str());
      } catch (const std::exception &e) {
        std::wcout << U("Error: ") << e.what() << std::endl;
        LOG_ERROR("failed to delete table and exit.");
      }
    }
    try {
      tables_[tbl_name].create_if_not_exists();
      LOG_DEBUG("Created table %s", tbl_name.c_str());
    } catch (const std::exception &e) {
      std::wcout << U("Error: ") << e.what() << std::endl;
      LOG_ERROR("failed to create table %s and exit.", tbl_name.c_str());
    }
  }

  void DeleteTable(const std::string &tbl_name) {
    auto tbl = table_client_.get_table_reference(U(tbl_name));
    try {
      tbl.delete_table();
      LOG_DEBUG("deleted table %s", tbl_name.c_str());
    } catch (const std::exception &e) {
      std::wcout << U("Error: ") << e.what() << std::endl;
      LOG_ERROR("failed to delete table and exit.");
    }
  }

  RC LoadSync(const std::string& tbl_name, std::string partition, const std::string& key, std::string& data) {
      if (g_buf_type == HYBRBUF && tbl_name == "obj-10g-0" && partition != "metadata") {
        M_ASSERT(false, "shouldn't access this table");
      }

      if (tables_.find(tbl_name) == tables_.end()) {
        tables_[tbl_name] = table_client_.get_table_reference(U(tbl_name));
      }

      int retry_left = g_remote_req_retries;
      auto table = tables_[tbl_name];
      auto retrieve_operation = azure::storage::table_operation::retrieve_entity(U(partition), U(key));
      while (retry_left >= 0) {
        try {
          auto retrieve_result = table.execute(retrieve_operation);
          azure::storage::table_entity entity = retrieve_result.entity();
          if (!entity.etag().empty()) {
            const azure::storage::table_entity::properties_type& properties = entity.properties();
            auto property = properties.at(U("Data"));
            if (property.property_type() == azure::storage::edm_type::binary) {
              auto ret = property.binary_value();
              data = std::string(ret.begin(), ret.end());
            } else if (property.property_type() == azure::storage::edm_type::string) {
              data = property.string_value();
            }
            retry_left = -1;
          } else {
            retry_left = -1;
            return RC::NOT_FOUND;
          }

        } catch (const azure::storage::storage_exception& e) {

          if (e.result().http_status_code() == 404) {

            retry_left = -1;
            return RC::NOT_FOUND;
          } else {
            retry_left--;
            if (retry_left >= 0) {
              if (g_warmup_finished) {
                auto backoff = (Worker::GetThdId() % 16 + 1) * (g_remote_req_retries - retry_left);
                if (e.result().http_status_code() == 429)
                  CountThrottle(tbl_name, g_thr429_rd, backoff * 1000);
                LOG_DEBUG("[thd-%u] Cannot load from table %s partition %s for data %s: %s. %d-th retry after %u ms",
                          Worker::GetThdId(), tbl_name.c_str(), partition.c_str(), key.c_str(), e.what(),
                          g_remote_req_retries - retry_left, backoff);
                std::cout.flush();
                usleep(backoff * 1000);
              } else {
                sleep(Worker::GetThdId() % 60 + 1);
              }
            } else {
              LOG_ERROR("[thd-%u] Cannot load from table data %s in table %s after retries: %s", Worker::GetThdId(),
                        key.c_str(), tbl_name.c_str(), e.what());
              std::cout.flush();
            }
          }
        }
      }
      return RC::OK;
    // } else {
    //   data = "";
    //   LOG_DEBUG("[thd-%u] load data %s in table %s ", Worker::GetThdId(), key.c_str(), tbl_name.c_str());
    // }
  }


RC LoadSyncQuery(const std::string& tbl_name, std::string partition, const std::string& key, std::string& data) {
  bool res = python_helper_->ExecPyPointQuery(tbl_name, partition, key, data);
  if (res) {
    return RC::OK;
  } else {
    return RC::ERROR;
  }
}
  

  
  uint64_t LoadNumericSync(const std::string &tbl_name, std::string partition, const std::string &key) {
    if (tables_.find(tbl_name) == tables_.end()) {
      tables_[tbl_name] = table_client_.get_table_reference(U(tbl_name));
    }
    auto table = tables_[tbl_name];
    auto retrieve_operation = azure::storage::table_operation::retrieve_entity(
        U(partition), U(key));
    auto retrieve_result = table.execute(retrieve_operation);
    azure::storage::table_entity entity = retrieve_result.entity();
    const azure::storage::table_entity::properties_type& properties = entity.properties();
    try {
      return properties.at(U("Data")).int64_value();
    } catch (const std::exception &e) {
      LOG_ERROR("Cannot load data %s from table %s: %s", key.c_str(), tbl_name.c_str(), e.what());
      std::cout.flush();
    }
  }

  void StoreSync(const std::string &tbl_name, azure::storage::table_entity &tuple) {
    auto table = tables_[tbl_name];
    auto insert_op = azure::storage::table_operation::insert_or_replace_entity(tuple);
    try {
      auto result = table.execute(insert_op);
    } catch (const std::exception &e) {
      // LOG_ERROR("Failed to StoreSync: %s", e.what());
    }
  }

  void StoreSync(const std::string &tbl_name, std::string partition, const std::string &key, char * data, size_t sz) {
    auto table = tables_[tbl_name];
    azure::storage::table_entity tuple(U(partition), U(key));
    auto& col = tuple.properties();
    col.reserve(1);
    col[U("Data")] = azure::storage::entity_property(
        std::vector<uint8_t>(data, data + sz));
    auto insert_op = azure::storage::table_operation::insert_or_replace_entity(tuple);
    bool should_retry = true;
    int retry = 0;
    while (should_retry) {
     try {
      auto result = table.execute(insert_op);

      should_retry = false;
     } catch (const std::exception &e) {
      const char* subStr = "Too Many Requests";
      if (strstr(e.what(), subStr)!= nullptr) {
        if (retry > 0 && ((retry % 100) == 0)) {
          LOG_DEBUG("Failed to StoreSync: %s, retry: %d, table: %s", e.what(), retry, tbl_name.c_str());
        }
        // don't hot-spin the CPU (or the RU) while throttled
        auto backoff_us = std::min(1000000, 10000 * (retry + 1));
        if (g_warmup_finished) CountThrottle(tbl_name, g_thr429_wr, backoff_us);
        usleep(backoff_us);
      } else {
        should_retry =false;
        LOG_ERROR("Failed to StoreSync for data len %ld: %s, retry: %s, table: %s", sz, e.what(), retry, tbl_name.c_str());
      }
      std::cout.flush();
     }
     retry++;
    }
  }

  void StoreAsync(const std::string &tbl_name, std::string partition,
      const std::string &key, char * data, size_t sz, volatile RC *rc) {
    auto table = tables_[tbl_name];
    azure::storage::table_entity tuple(U(partition), U(key));
    auto& col = tuple.properties();
    col.reserve(1);
    col[U("Data")] = azure::storage::entity_property(
        std::vector<uint8_t>(data, data + sz));
    auto insert_op = azure::storage::table_operation::insert_or_replace_entity(tuple);
    try {
      auto resp = table.execute_async(insert_op);
      resp.then([=] (azure::storage::table_result result) {
        *rc = RC::OK;
      });
    } catch (const std::exception &e) {
      LOG_ERROR("Failed to StoreSync: %s", e.what());
    }
  }

  void StoreNumericSync(const std::string &tbl_name, std::string partition, const std::string &key, int64_t number) {
    auto table = tables_[tbl_name];
    azure::storage::table_entity tuple(U(partition), U(key));
    auto& col = tuple.properties();
    col.reserve(1);
    col[U("Data")] = azure::storage::entity_property(number);
    auto insert_op = azure::storage::table_operation::insert_or_replace_entity(tuple);
    try {
      auto result = table.execute(insert_op);
    } catch (const std::exception &e) {
      LOG_ERROR("Failed to StoreSync: %s", e.what());
    }
  }

  void StoreAndLoadSync(const std::string &tbl_name,
                        std::string store_part, const std::string &key,
                        char *data, size_t sz, std::string load_part,
                        const std::string &load_key, std::string &load_data) {
    auto table = tables_[tbl_name];
    azure::storage::table_entity tuple(U(store_part), U(key));
    auto& col = tuple.properties();
    col.reserve(1);
    col[U("Data")] = azure::storage::entity_property(std::vector<uint8_t>(data, data + sz));
    auto insert_op = azure::storage::table_operation::insert_or_replace_entity(tuple);
    auto retrieve_operation = azure::storage::table_operation::retrieve_entity(
        U(load_part), U(load_key));

    int retry_left = g_remote_req_retries;
    while (retry_left >= 0) {
      try {
        auto result = table.execute_async(insert_op);
        auto retrieve_result = table.execute(retrieve_operation);
        azure::storage::table_entity entity = retrieve_result.entity();
        const azure::storage::table_entity::properties_type& properties = entity.properties();
        auto ret = properties.at(U("Data")).binary_value();
        load_data = std::string(ret.begin(), ret.end());
        // wait for both requests to complete.
        result.wait();
        retry_left = -1;
      } catch (const std::exception &e) {
        retry_left--;
        if (retry_left >= 0) {
          if (g_warmup_finished) {
            auto backoff = (Worker::GetThdId() % 16 + 1) * (g_remote_req_retries - retry_left);
            if (strstr(e.what(), "Too Many Requests") != nullptr)
              CountThrottle(tbl_name, g_thr429_rw, backoff * 1000);
            LOG_DEBUG("[thd-%u] Cannot StoreAndLoad data %s: %s. %d-th retry after %u ms",
                      Worker::GetThdId(), key.c_str(), e.what(),
                      g_remote_req_retries - retry_left, backoff);
            usleep(backoff * 1000);
          } else {
            sleep(Worker::GetThdId() % 60 + 1);
          }
        } else {
          LOG_ERROR("[thd-%u] Cannot StoreAndLoad data %s in table %s after retries: %s",
                    Worker::GetThdId(), key.c_str(), tbl_name.c_str(), e.what());
        }
      }
    }
  }

  // Cosmos/Azure Table entity-group transactions accept at most 100 ops;
  // an oversized flush group (eviction leaves hold up to fanout=250 keys of
  // one partition) is rejected wholesale by the server, so every batch path
  // must chunk client-side. The sim store accepts any size and masks this.
  static constexpr size_t kMaxBatchOps = 100;

  // Eviction write-backs default to persisting row-by-row through StoreSync
  // (idempotent committed-state persists need no cross-row atomicity);
  // g_evict_batch_egt routes them through entity-group transactions instead
  // (see BatchStoreEgt below for the SDK caveats that requires). Bulk loads
  // bypass this path entirely via g_dump_load_dir + experiments/upload_dump.py.
  void BatchStoreSync(const std::string &tbl_name,
                      std::string part,
                      const std::multimap<std::string, std::string> &map) {
    if (g_evict_batch_egt) {
      BatchStoreEgt(tbl_name, part, map);
      return;
    }
    for (auto &item : map) {
      StoreSync(tbl_name, part, item.first,
                (char *) item.second.data(), item.second.size());
    }
    g_evict_batch_ok_cnt.fetch_add(1);
  }

  // Entity-group transactions against Cosmos. Two SDK caveats, both verified
  // by reading the rows back after the batch:
  //
  //   * without "DataServiceVersion: 3.0" the batch genuinely does nothing --
  //     the server accepts it and stores no rows. The SDK sends
  //     "MaxDataServiceVersion: 3.0;Native" instead, the same header that
  //     makes Cosmos reject its range query with a misleading 400.
  //   * WITH that header the server executes the batch and answers 202, every
  //     row lands, and the SDK then throws because it cannot parse the
  //     multipart response ("received 0 batch results").
  //
  // Treating that parse failure as "throttled" and retrying would re-execute
  // a batch that already succeeded, forever. Here a 202 ends the attempt;
  // only genuine throttling retries.
  //
  // Worth it because the write path is latency-bound, not RU-bound: per-row
  // cost falls roughly 7x going from 1 to 100 rows per round trip, so the
  // batched path sustains several times the row-by-row write-back rate.
  void BatchStoreEgt(const std::string &tbl_name, const std::string &part,
                     const std::multimap<std::string, std::string> &map) {
    if (tables_.find(tbl_name) == tables_.end()) {
      tables_[tbl_name] = table_client_.get_table_reference(U(tbl_name));
    }
    auto table = tables_[tbl_name];
    azure::storage::table_request_options options;
    options.set_payload_format(azure::storage::table_payload_format::json);
    azure::storage::operation_context ctx;
    ctx.set_sending_request([](web::http::http_request &req,
                               azure::storage::operation_context) {
      req.headers().remove(U("MaxDataServiceVersion"));
      req.headers().remove(U("DataServiceVersion"));
      req.headers().add(U("DataServiceVersion"), U("3.0"));
    });

    auto chunk_begin = map.begin();
    while (chunk_begin != map.end()) {
      auto chunk_end = chunk_begin;
      size_t chunk_n = 0;
      for (; chunk_n < kMaxBatchOps && chunk_end != map.end(); chunk_n++) {
        ++chunk_end;
      }
      int retry = 0;
      while (true) {
        // rebuilt every attempt: a table_batch_operation is consumed by a
        // failed execute_batch and re-executing the stale object fails forever
        azure::storage::table_batch_operation batch_op;
        for (auto it = chunk_begin; it != chunk_end; ++it) {
          azure::storage::table_entity tuple(U(part), U(it->first));
          auto &col = tuple.properties();
          col.reserve(1);
          col[U("Data")] = azure::storage::entity_property(
              std::vector<uint8_t>(it->second.begin(), it->second.end()));
          batch_op.insert_or_replace_entity(tuple);
        }
        try {
          table.execute_batch(batch_op, options, ctx);
          g_evict_batch_ok_cnt.fetch_add(1);
          break;
        } catch (const azure::storage::storage_exception &e) {
          if (e.result().http_status_code() == 202) {
            // executed; only the response could not be parsed
            g_egt_unparsed.fetch_add(1, std::memory_order_relaxed);
            g_evict_batch_ok_cnt.fetch_add(1);
            break;
          }
          if (e.result().http_status_code() != 429 && retry > 0) {
            g_evict_batch_fail_cnt.fetch_add(1);
            LOG_INFO("EGT failed (%zu rows, http %d): %s, table %s part %s",
                     chunk_n, e.result().http_status_code(), e.what(),
                     tbl_name.c_str(), part.c_str());
            break;
          }
          if (retry >= 600) {
            g_evict_batch_fail_cnt.fetch_add(1);
            LOG_INFO("EGT gave up after %d retries (%zu rows): %s, table %s",
                     retry, chunk_n, e.what(), tbl_name.c_str());
            break;
          }
          usleep(std::min(5000000, 50000 * (retry + 1)));
          retry++;
        }
      }
      chunk_begin = chunk_end;
    }
  }

  void BatchStoreSyncEgtUnused(const std::string &tbl_name,
                               std::string part,
                               const std::multimap<std::string, std::string> &map) {
    auto table = tables_[tbl_name];
    auto chunk_begin = map.begin();
    while (chunk_begin != map.end()) {
      auto chunk_end = chunk_begin;
      size_t chunk_n = 0;
      for (; chunk_n < kMaxBatchOps && chunk_end != map.end(); chunk_n++) {
        ++chunk_end;
      }
      if (chunk_n == 1) {
        // single-op entity-group txns are deterministically rejected for
        // this SDK/Cosmos combination ("0 batch results" forever; python
        // 1-op EGTs succeed) — write the row as a plain upsert instead
        StoreSync(tbl_name, part, chunk_begin->first,
                  (char *) chunk_begin->second.data(),
                  chunk_begin->second.size());
        g_evict_batch_ok_cnt.fetch_add(1);
        chunk_begin = chunk_end;
        continue;
      }
      // dataset loads come through here: retry through throttling, but a
      // silently dropped batch corrupts the dataset, so anything else is fatal
      bool should_retry = true;
      int retry = 0;
      while (should_retry) {
        // rebuild the operation every attempt: a table_batch_operation is
        // consumed by a failed execute_batch, and re-executing the stale
        // object fails forever with an empty batch-result set
        azure::storage::table_batch_operation batch_op;
        for (auto it = chunk_begin; it != chunk_end; ++it) {
          azure::storage::table_entity tuple(U(part), U(it->first));
          auto& col = tuple.properties();
          col.reserve(1);
          col[U("Data")] = azure::storage::entity_property(
              std::vector<uint8_t>(it->second.begin(), it->second.end()));
          batch_op.insert_or_replace_entity(tuple);
        }
        try {
          // plain 1-arg execute_batch: the only call form ever observed to
          // succeed against Cosmos (65K rows in the first batch-load run)
          auto results = table.execute_batch(batch_op);
          should_retry = false;
          g_evict_batch_ok_cnt.fetch_add(1);
        } catch (const std::exception &e) {
          // Cosmos surfaces a throttled entity-group txn either as 429 or as
          // an empty batch-result set ("received 0 batch results"); both are
          // retryable. Back off so 12 loader threads don't hot-spin the RU.
          bool throttled = strstr(e.what(), "Too Many Requests") != nullptr ||
              strstr(e.what(), "batch result size does not match") != nullptr;
          if (throttled && retry >= 600) {
            // a sustained RU-debt window (burst overconsumption is clawed
            // back over minutes) can legitimately throttle for a while;
            // with a 5 s backoff cap this bound is ~45 min of futility —
            // beyond any debt, so fail loudly rather than hang forever
            LOG_ERROR("BatchStoreSync gave up after %d retries (%zu rows): "
                      "%s, table %s part %s", retry, map.size(), e.what(),
                      tbl_name.c_str(), part.c_str());
          }
          if (throttled) {
            if (retry > 0 && (retry % 100) == 0) {
              LOG_INFO("BatchStoreSync still throttled: retry %d, table %s",
                       retry, tbl_name.c_str());
            }
            usleep(std::min(5000000, 50000 * (retry + 1)));
            // a throttled batch response (plain 429 body, not multipart)
            // poisons the pooled connection: later requests on it never see
            // a response at all. Retry through a FRESH client/connection
            // pool so one 429 cannot wedge the path permanently.
            table = storage_acct_.create_cloud_table_client()
                        .get_table_reference(U(tbl_name));
          } else {
            LOG_ERROR("Failed BatchStoreSync (%zu rows): %s, table %s part %s",
                      map.size(), e.what(), tbl_name.c_str(), part.c_str());
          }
        }
        retry++;
      }
      chunk_begin = chunk_end;
    }
  }

  void BatchStoreSync(const std::string &tbl_name,
                      azure::storage::table_batch_operation &batch_op) {
    auto table = tables_[tbl_name];
    try {
      table.execute_batch(batch_op);
    } catch (const std::exception &e) {
      // LOG_DEBUG("Warning in BatchStore: %s", e.what());
    }
  }

  // Routes "async" write-backs through the synchronous hardened batch path
  // and invokes on_settled exactly once when the data is durably stored.
  // The SDK's execute_batch_async consistently fails against Cosmos (small
  // unthrottled batches included) while sync execute_batch works; callers
  // already block on completion via latch/is_done, and the sim models a
  // synchronous batched write-back, so the semantics match the paper's model.
  void BatchStoreAsyncImpl(const std::string &tbl_name, const std::string &part,
                           const std::multimap<std::string, std::string> &map,
                           const std::function<void()> &on_settled) {
    BatchStoreSync(tbl_name, part, map);
    on_settled();
  }

  void BatchStoreAsync(const std::string &tbl_name, std::string part,
                      const std::multimap<std::string, std::string>& map,
                      volatile bool &is_done) {
    is_done = false;
    volatile bool *done_ptr = &is_done;
    // task-based settle: the old value-continuation never fired on a faulted
    // task, leaving is_done false forever and hanging the evictor
    BatchStoreAsyncImpl(tbl_name, part, map, [done_ptr]() { *done_ptr = true; });
  }

  // eviction write-backs (Algorithm-2 batched eviction) come through here;
  // chunking, failure counting, and exactly-once callback semantics live in
  // BatchStoreAsyncImpl
  void WriteBatchAsync(const std::string &tbl_name, std::string partition, const std::multimap<std::string, std::string> &data, std::function<void()> callback) {
    BatchStoreAsyncImpl(tbl_name, partition, data, std::move(callback));
  }

  void WaitForAsync(int cnt) {
  }

  void LoadRangeSync(std::string &tbl_name,
                     std::string &partition,
                     uint64_t cnt,
                     uint64_t low_key,
                     char **data_ptrs) {
    auto table = tables_[tbl_name];
    // async execution.
    std::atomic<int32_t> semaphore(cnt);
    for (size_t i = 0; i < cnt; i++) {
      // Execute the query.
      try {
        auto key = SearchKey(low_key + i).ToString();
        auto op = azure::storage::table_operation::retrieve_entity(
            U(partition), U(key));
        auto resp = table.execute_async(op);
        auto data_dest = data_ptrs[i];
        resp.then([&semaphore, data_dest](azure::storage::table_result result) {
          azure::storage::table_entity entity = result.entity();
          auto &properties = entity.properties();
          auto property = properties.at(U("Data"));
          std::string data;
          if (property.property_type() == azure::storage::edm_type::binary) {
            auto ret = property.binary_value();
            data = std::string(ret.begin(), ret.end());
          } else if (property.property_type() == azure::storage::edm_type::string) {
            data = property.string_value();
          }
          memcpy(data_dest, data.c_str(), data.size());
          auto sem = atomic_fetch_sub(&semaphore, 1);
          // LOG_DEBUG("async request stored key %s at %p with semaphore = %u",
          //           entity.row_key().c_str(), data_dest, sem - 1);
        });
        if (i == cnt - 1) {
          resp.wait();
          while (semaphore > 0) {}
        }
      } catch (const azure::storage::storage_exception &e) {
        azure::storage::request_result result = e.result();
        azure::storage::storage_extended_error extended_error = result.extended_error();
        if (!extended_error.message().empty()) {
          ucout << extended_error.message() << std::endl;
        }
        LOG_ERROR("Error: %s", e.what());
      } catch (const std::exception &e) {
        ucout << _XPLATSTR("Error: ") << e.what() << std::endl;
      }
    }
  }


  // Range scan through the C++ SDK instead of the embedded interpreter.
  //
  // Cosmos rejects the SDK's query with 400 InvalidInput "Request url is
  // invalid" for exactly ONE reason: the SDK sends
  // "MaxDataServiceVersion: 3.0;Native" where the Table API wants
  // "DataServiceVersion: 3.0" (the header azure-data-tables sends on the
  // request Cosmos answers 200 to). The error text points at the url, which
  // is a red herring: the entity-set "()" suffix and a flat, ungrouped
  // $filter are BOTH optional, and swapping that header alone makes the
  // identical query succeed.
  //
  // Why replace the python path at all: ExecPyRangeSearch holds the GIL across
  // the whole round trip AND allocates one python string per row, so scans
  // serialise process-wide no matter how many workers run or how large the
  // buffer is, and the scan rate caps end-to-end throughput. This path is an
  // order of magnitude cheaper per query and per row, and scales with threads
  // instead of serialising.
  //
  // Payload format stays minimal-metadata ON PURPOSE. json_no_metadata drops
  // the "Data@odata.type": "Edm.Binary" annotation, and without it the SDK
  // hands back the base64 TEXT instead of the decoded bytes -- silent data
  // corruption, not an error.
  void LoadRangeCpp(const std::string& tbl_name, const std::string& partition,
                    const std::string& low_key, const std::string& high_key,
                    char** tuples, size_t total_tuple_sz, size_t& res_cnt) {
    res_cnt = 0;
    if (tables_.find(tbl_name) == tables_.end()) {
      tables_[tbl_name] = table_client_.get_table_reference(U(tbl_name));
    }
    auto table = tables_[tbl_name];

    std::string flt = "PartitionKey eq '" + partition + "' and RowKey ge '" +
                      low_key + "' and RowKey le '" + high_key + "'";
    azure::storage::table_query query;
    query.set_filter_string(U(flt));
    azure::storage::table_request_options options;
    options.set_payload_format(azure::storage::table_payload_format::json);
    azure::storage::operation_context ctx;
    ctx.set_sending_request([](web::http::http_request& req,
                               azure::storage::operation_context) {
      req.headers().remove(U("MaxDataServiceVersion"));
      req.headers().remove(U("DataServiceVersion"));
      req.headers().add(U("DataServiceVersion"), U("3.0"));
    });

    std::vector<std::string> rows;
    int retry_left = g_remote_req_retries;
    bool ok = false;
    while (retry_left >= 0) {
      try {
        rows.clear();
        azure::storage::continuation_token token;
        do {
          auto seg = table.execute_query_segmented(query, token, options, ctx);
          for (auto& e : seg.results()) {
            const auto& props = e.properties();
            auto it = props.find(U("Data"));
            if (it == props.end()) continue;
            if (it->second.property_type() == azure::storage::edm_type::binary) {
              auto v = it->second.binary_value();
              rows.emplace_back(v.begin(), v.end());
            } else {
              rows.emplace_back(it->second.string_value());
            }
          }
          token = seg.continuation_token();
        } while (!token.empty());
        ok = true;
        break;
      } catch (const azure::storage::storage_exception& e) {
        retry_left--;
        if (retry_left >= 0) {
          // same backoff shape as LoadSync: staggered by thread so 16 workers
          // do not retry in lockstep against a throttled partition
          auto backoff = (Worker::GetThdId() % 16 + 1) * (g_remote_req_retries - retry_left);
          usleep(backoff * 1000);
        } else {
          // deliberately NOT LOG_ERROR: that is exit(1), and killing the run
          // here would destroy a measurement instead of marking it. A scan that
          // returns nothing makes its transaction read an empty range, so any
          // run with a non-zero count below is disqualified, not merely noisy.
          g_scan_query_fail.fetch_add(1, std::memory_order_relaxed);
          LOG_INFO("SCAN QUERY FAILED after %d retries on %s part %s [%s, %s]: %s",
                   g_remote_req_retries, tbl_name.c_str(), partition.c_str(),
                   low_key.c_str(), high_key.c_str(), e.what());
        }
      }
    }
    if (!ok || rows.empty()) return;

    // same layout the python path produces and ITable consumes: one
    // total_tuple_sz slot per row, the row bytes placed after the ITuple header
    // the caller placement-news over the front.
    res_cnt = rows.size();
    *tuples = (char*) MemoryAllocator::Alloc(total_tuple_sz * res_cnt);
    auto payload = total_tuple_sz - sizeof(ITuple);
    for (size_t i = 0; i < res_cnt; i++) {
      memcpy(*tuples + i * total_tuple_sz + sizeof(ITuple), rows[i].data(),
             std::min(rows[i].size(), payload));
    }
  }

  void LoadRangePython(std::string& tbl_name, std::string& partition, std::string low_key, std::string high_key,
                       char** tuples, size_t total_tuple_sz, size_t& res_cnt) {
    // async execution.
    python_helper_->ExecPyRangeSearch(tbl_name, partition, low_key, high_key, tuples, total_tuple_sz, res_cnt);
  }

 private:
  azure::storage::cloud_storage_account storage_acct_;
  azure::storage::cloud_table_client table_client_;
  std::unordered_map<std::string, azure::storage::cloud_table> tables_;
  AzurePythonHelper* python_helper_;

};
}

#endif //README_MD_SRC_REMOTE_AZURETABLECLIENT_H_
