#include "SimNetClient.h"
#include "SimNetProtocol.h"
#include "local/ITuple.h"

namespace arboretum {

using namespace simnet;

// server startup race tolerance: scripts probe readiness before launching the
// engine, but the first lazily-created connections may still race a slow bind
static constexpr int kConnectRetries = 50;
static constexpr int kConnectRetryUs = 100 * 1000;

SimNetClient::SimNetClient() {
  std::string addr(g_sim_server_addr);
  auto colon = addr.rfind(':');
  if (colon == std::string::npos || colon == 0 || colon + 1 == addr.size()) {
    LOG_ERROR("malformed g_sim_server_addr '%s' (want host:port)",
              g_sim_server_addr);
  }
  host_ = addr.substr(0, colon);
  port_ = std::stoi(addr.substr(colon + 1));
  async_fd_ = Connect();
  receiver_ = std::thread(SimNetClient::ReceiverLoop, this);
  LOG_INFO("Created simnet client for %s:%d (latency is injected "
           "server-side)", host_.c_str(), port_);
}

SimNetClient::~SimNetClient() {
  stop_ = true;
  shutdown(async_fd_, SHUT_RDWR);
  receiver_.join();
  close(async_fd_);
  std::lock_guard<std::mutex> lk(sync_latch_);
  for (auto &pair : sync_fds_) close(pair.second);
}

// -------------------- transport --------------------

int SimNetClient::Connect() const {
  struct addrinfo hints {};
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  struct addrinfo *res = nullptr;
  auto port_str = std::to_string(port_);
  if (getaddrinfo(host_.c_str(), port_str.c_str(), &hints, &res) != 0 ||
      res == nullptr) {
    LOG_ERROR("cannot resolve sim store server %s", host_.c_str());
  }
  int fd = -1;
  for (int attempt = 0; attempt < kConnectRetries; attempt++) {
    fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd >= 0 && connect(fd, res->ai_addr, res->ai_addrlen) == 0) break;
    if (fd >= 0) close(fd);
    fd = -1;
    usleep(kConnectRetryUs);
  }
  freeaddrinfo(res);
  if (fd < 0) {
    // fail fast: never silently fall back to the in-process store
    LOG_ERROR("cannot connect to sim store server %s:%d", host_.c_str(),
              port_);
  }
  SetNoDelay(fd);
  // HELLO guards against pointing the engine at some other service's port
  std::string hello;
  PutU64(hello, kHelloMagic);
  uint64_t req_id = 0;
  uint8_t status;
  std::string resp;
  if (!SendFrame(fd, 0, OP_HELLO, hello) ||
      !RecvFrame(fd, req_id, status, resp) || status != ST_OK) {
    LOG_ERROR("sim store server %s:%d failed the HELLO handshake",
              host_.c_str(), port_);
  }
  return fd;
}

int SimNetClient::SyncFd() {
  auto tid = std::this_thread::get_id();
  {
    std::lock_guard<std::mutex> lk(sync_latch_);
    auto it = sync_fds_.find(tid);
    if (it != sync_fds_.end()) return it->second;
  }
  auto fd = Connect();
  std::lock_guard<std::mutex> lk(sync_latch_);
  sync_fds_[tid] = fd;
  return fd;
}

uint8_t SimNetClient::SyncCall(uint8_t op, const std::string &req,
                               std::string &resp) {
  auto fd = SyncFd();
  auto id = next_req_id_.fetch_add(1);
  uint64_t resp_id;
  uint8_t status;
  if (!SendFrame(fd, id, op, req) || !RecvFrame(fd, resp_id, status, resp)) {
    LOG_ERROR("sim store server connection lost (op %u)", op);
  }
  if (resp_id != id) {
    // lockstep connections have exactly one request in flight
    LOG_ERROR("sim store reply id mismatch (%lu vs %lu)", resp_id, id);
  }
  return status;
}

void SimNetClient::AsyncCall(uint8_t op, const std::string &req,
                             Completion completion) {
  auto id = next_req_id_.fetch_add(1);
  {
    // register before sending so the receiver can never see an unknown id
    std::lock_guard<std::mutex> lk(pending_latch_);
    pending_[id] = std::move(completion);
  }
  std::lock_guard<std::mutex> lk(send_latch_);
  if (!SendFrame(async_fd_, id, op, req)) {
    LOG_ERROR("sim store async channel lost (op %u)", op);
  }
}

void SimNetClient::ReceiverLoop(SimNetClient *client) {
  while (true) {
    uint64_t id;
    uint8_t status;
    std::string payload;
    if (!RecvFrame(client->async_fd_, id, status, payload)) {
      if (client->stop_) return;
      LOG_ERROR("sim store server closed the async channel");
    }
    Completion completion;
    {
      std::lock_guard<std::mutex> lk(client->pending_latch_);
      auto it = client->pending_.find(id);
      if (it == client->pending_.end()) {
        LOG_ERROR("unmatched async reply id %lu", id);
      }
      completion = std::move(it->second);
      client->pending_.erase(it);
    }
    completion(status, payload);
  }
}

// -------------------- table ops --------------------

void SimNetClient::CreateTable(const std::string &tbl_name) {
  std::string req, resp;
  PutStr(req, tbl_name);
  if (SyncCall(OP_CREATE_TABLE, req, resp) != ST_OK) {
    LOG_ERROR("cannot create simulated table %s", tbl_name.c_str());
  }
}

void SimNetClient::DeleteTable(const std::string &tbl_name) {
  std::string req, resp;
  PutStr(req, tbl_name);
  if (SyncCall(OP_DELETE_TABLE, req, resp) != ST_OK) {
    LOG_ERROR("cannot delete simulated table %s", tbl_name.c_str());
  }
}

RC SimNetClient::LoadSync(const std::string &tbl_name, std::string partition,
                          const std::string &key, std::string &data) {
  std::string req, resp;
  PutStr(req, tbl_name);
  PutStr(req, partition);
  PutStr(req, key);
  auto status = SyncCall(OP_LOAD, req, resp);
  if (status == ST_NOT_FOUND) return RC::NOT_FOUND;
  if (status != ST_OK) {
    LOG_ERROR("load %s from table %s failed", key.c_str(), tbl_name.c_str());
  }
  data = std::move(resp);
  return RC::OK;
}

uint64_t SimNetClient::LoadNumericSync(const std::string &tbl_name,
                                       std::string partition,
                                       const std::string &key) {
  std::string data;
  auto rc = LoadSync(tbl_name, std::move(partition), key, data);
  if (rc != RC::OK || data.size() != sizeof(int64_t)) {
    LOG_ERROR("Cannot load data %s from table %s", key.c_str(),
              tbl_name.c_str());
  }
  int64_t num;
  memcpy(&num, data.data(), sizeof(int64_t));
  return (uint64_t) num;
}

void SimNetClient::StoreSync(const std::string &tbl_name,
                             std::string partition, const std::string &key,
                             char *data, size_t sz) {
  std::string req, resp;
  PutStr(req, tbl_name);
  PutStr(req, partition);
  PutStr(req, key);
  PutBytes(req, data, sz);
  if (SyncCall(OP_STORE, req, resp) != ST_OK) {
    LOG_ERROR("store %s to table %s failed", key.c_str(), tbl_name.c_str());
  }
}

void SimNetClient::StoreNumericSync(const std::string &tbl_name,
                                    std::string partition,
                                    const std::string &key, int64_t number) {
  StoreSync(tbl_name, std::move(partition), key, (char *) &number,
            sizeof(int64_t));
}

void SimNetClient::StoreAndLoadSync(const std::string &tbl_name,
                                    std::string store_part,
                                    const std::string &key, char *data,
                                    size_t sz, std::string load_part,
                                    const std::string &load_key,
                                    std::string &load_data) {
  std::string req, resp;
  PutStr(req, tbl_name);
  PutStr(req, store_part);
  PutStr(req, key);
  PutBytes(req, data, sz);
  PutStr(req, load_part);
  PutStr(req, load_key);
  if (SyncCall(OP_STORE_AND_LOAD, req, resp) != ST_OK) {
    LOG_ERROR("Cannot StoreAndLoad data %s in table %s", load_key.c_str(),
              tbl_name.c_str());
  }
  load_data = std::move(resp);
}

// -------------------- batch / async ops --------------------

std::string SimNetClient::EncodeBatch(
    const std::string &tbl_name, const std::string &part,
    const std::multimap<std::string, std::string> &map) {
  std::string req;
  PutStr(req, tbl_name);
  PutStr(req, part);
  PutU32(req, (uint32_t) map.size());
  for (auto &row : map) {
    PutStr(req, row.first);
    PutStr(req, row.second);
  }
  return req;
}

static void CheckBatchStatus(uint8_t status, const std::string &tbl_name,
                             size_t rows) {
  if (status == ST_BATCH_TOO_LARGE) {
    // g_sim_enforce_batch_limit: real Cosmos rejects EGTs above 100 entities;
    // this batch would break the cloud deployment
    LOG_ERROR("EGT batch of %zu rows to table %s exceeds the 100-entity "
              "limit", rows, tbl_name.c_str());
  }
  if (status != ST_OK) {
    LOG_ERROR("batch store of %zu rows to table %s failed", rows,
              tbl_name.c_str());
  }
}

void SimNetClient::BatchStoreSync(
    const std::string &tbl_name, std::string part,
    const std::multimap<std::string, std::string> &map) {
  std::string resp;
  auto status = SyncCall(OP_BATCH_STORE, EncodeBatch(tbl_name, part, map),
                         resp);
  CheckBatchStatus(status, tbl_name, map.size());
}

void SimNetClient::StoreAsync(const std::string &tbl_name,
                              std::string partition, const std::string &key,
                              char *data, size_t sz, volatile RC *rc) {
  std::string req;
  PutStr(req, tbl_name);
  PutStr(req, partition);
  PutStr(req, key);
  PutBytes(req, data, sz);
  AsyncCall(OP_STORE, req, [tbl_name, key, rc](uint8_t status,
                                               const std::string &) {
    if (status != ST_OK) {
      LOG_ERROR("async store %s to table %s failed", key.c_str(),
                tbl_name.c_str());
    }
    *rc = RC::OK;
  });
}

void SimNetClient::BatchStoreAsync(
    const std::string &tbl_name, std::string part,
    const std::multimap<std::string, std::string> &map, volatile bool &is_done) {
  is_done = false;
  volatile bool *done_ptr = &is_done;
  auto rows = map.size();
  AsyncCall(OP_BATCH_STORE, EncodeBatch(tbl_name, part, map),
            [tbl_name, rows, done_ptr](uint8_t status, const std::string &) {
              CheckBatchStatus(status, tbl_name, rows);
              *done_ptr = true;
            });
}

void SimNetClient::WriteBatchAsync(
    const std::string &tbl_name, std::string partition,
    const std::multimap<std::string, std::string> &data,
    std::function<void()> callback) {
  auto rows = data.size();
  AsyncCall(OP_BATCH_STORE, EncodeBatch(tbl_name, partition, data),
            [tbl_name, rows, callback = std::move(callback)](
                uint8_t status, const std::string &) {
              CheckBatchStatus(status, tbl_name, rows);
              callback();
            });
}

// -------------------- range scan --------------------

void SimNetClient::LoadRangePython(std::string &tbl_name,
                                   std::string &partition, std::string low_key,
                                   std::string high_key, char **tuples,
                                   size_t total_tuple_sz, size_t &res_cnt) {
  std::string req, resp;
  PutStr(req, tbl_name);
  PutStr(req, partition);
  PutStr(req, low_key);
  PutStr(req, high_key);
  if (SyncCall(OP_RANGE, req, resp) != ST_OK) {
    LOG_ERROR("range scan [%s, %s] on table %s failed", low_key.c_str(),
              high_key.c_str(), tbl_name.c_str());
  }
  Reader reader(resp);
  res_cnt = reader.GetU32();
  if (res_cnt > 0) {
    // same buffer contract as AzurePythonHelper::ExecPyRangeSearch: one
    // total_tuple_sz slot per row, payload at offset sizeof(ITuple)
    *tuples = (char *) MemoryAllocator::Alloc(total_tuple_sz * res_cnt);
    std::string value;
    for (size_t i = 0; i < res_cnt; i++) {
      if (!reader.GetStr(value)) {
        LOG_ERROR("truncated range reply from table %s", tbl_name.c_str());
      }
      M_ASSERT(value.size() + sizeof(ITuple) <= total_tuple_sz,
               "row in table %s (%zu B) exceeds the %zu B scan slot",
               tbl_name.c_str(), value.size(), total_tuple_sz);
      memcpy(*tuples + i * total_tuple_sz + sizeof(ITuple), value.data(),
             value.size());
    }
  }
}

// -------------------- log / checkpoint --------------------

void SimNetClient::LogFlush(const std::string &tbl_name, const char *payload,
                            size_t payload_sz) {
  std::string req, resp;
  PutStr(req, tbl_name);
  PutBytes(req, payload, payload_sz);
  if (SyncCall(OP_LOG_FLUSH, req, resp) != ST_OK) {
    LOG_ERROR("log flush to table %s failed", tbl_name.c_str());
  }
}

void SimNetClient::Checkpoint() {
  std::string resp;
  // dumping a large dataset can take a while; the lockstep read just waits
  if (SyncCall(OP_CHECKPOINT, "", resp) != ST_OK) {
    LOG_ERROR("sim store server checkpoint failed");
  }
}

} // namespace arboretum
