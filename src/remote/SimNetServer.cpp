#include "SimNetServer.h"
#include "SimNetProtocol.h"

#include <poll.h>
#include <sys/prctl.h>

namespace arboretum {

using namespace simnet;

// executors wake this early and spin the rest for deadline stability. The
// margin must absorb the whole condvar wake-up path (futex timeout + mutex
// reacquire + runqueue latency): 100µs — the in-process InjectNs margin —
// proved too tight here and skewed injected latencies +50µs at p50, so the
// executors also drop their timer slack to 1ns (default 50µs) and wake 300µs
// early. Spin cost per op = margin, parallel across executors.
static constexpr auto kSpinMargin = std::chrono::microseconds(300);

SimNetServer::~SimNetServer() { Stop(); }

int SimNetServer::Start(int port) {
  listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd_ < 0) LOG_ERROR("SimNetServer: cannot create socket");
  int one = 1;
  setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  struct sockaddr_in addr {};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons((uint16_t) port);
  if (bind(listen_fd_, (struct sockaddr *) &addr, sizeof(addr)) != 0) {
    LOG_ERROR("SimNetServer: cannot bind port %d", port);
  }
  if (listen(listen_fd_, 128) != 0) {
    LOG_ERROR("SimNetServer: cannot listen on port %d", port);
  }
  if (port == 0) {
    socklen_t len = sizeof(addr);
    getsockname(listen_fd_, (struct sockaddr *) &addr, &len);
    port = ntohs(addr.sin_port);
  }
  stop_ = false;
  for (size_t i = 0; i < g_sim_server_exec_thds; i++) {
    executors_.emplace_back(&SimNetServer::ExecutorLoop, this);
  }
  accept_thd_ = std::thread(&SimNetServer::AcceptLoop, this);
  LOG_INFO("SimNetServer listening on port %d (rd/wr/rw/scan/batch/log "
           "latency = %lu/%lu/%lu/%lu/%lu/%lu us, %lu executors, "
           "batch limit %s)",
           port, g_sim_rd_latency_us, g_sim_wr_latency_us,
           SimTableClient::RWLatencyUs(), g_sim_scan_latency_us,
           g_sim_batch_latency_us, SimTableClient::LogLatencyUs(),
           g_sim_server_exec_thds,
           g_sim_enforce_batch_limit ? "enforced" : "off");
  return port;
}

void SimNetServer::Stop() {
  if (listen_fd_ < 0) return;
  stop_ = true;
  accept_thd_.join(); // polls with a timeout, wakes within 100ms
  close(listen_fd_);
  listen_fd_ = -1;
  task_cv_.notify_all();
  for (auto &thd : executors_) thd.join();
  executors_.clear();
  tasks_.clear();
  std::lock_guard<std::mutex> lk(conns_latch_);
  for (auto conn : conns_) {
    shutdown(conn->fd, SHUT_RDWR);
    conn->reader.join();
    close(conn->fd);
    delete conn;
  }
  conns_.clear();
}

void SimNetServer::AcceptLoop() {
  while (!stop_) {
    // poll with a timeout: closing a listening fd does not reliably wake a
    // thread blocked in accept() on Linux
    struct pollfd pfd {listen_fd_, POLLIN, 0};
    if (poll(&pfd, 1, 100) <= 0) continue;
    auto fd = accept(listen_fd_, nullptr, nullptr);
    if (fd < 0) continue;
    SetNoDelay(fd);
    auto conn = new Conn{fd};
    conn->reader = std::thread(&SimNetServer::ReaderLoop, this, conn);
    std::lock_guard<std::mutex> lk(conns_latch_);
    conns_.push_back(conn);
  }
}

void SimNetServer::ReaderLoop(Conn *conn) {
  while (true) {
    uint64_t req_id;
    uint8_t op;
    std::string payload;
    if (!RecvFrame(conn->fd, req_id, op, payload)) {
      // client closed (engine done) or Stop(); the fd is closed in Stop()
      return;
    }
    Dispatch(conn, req_id, op, std::move(payload));
  }
}

uint64_t SimNetServer::LatencyUs(uint8_t op) {
  switch (op) {
    case OP_LOAD: return g_sim_rd_latency_us;
    case OP_STORE: return g_sim_wr_latency_us;
    case OP_STORE_AND_LOAD: return SimTableClient::RWLatencyUs();
    case OP_BATCH_STORE: return g_sim_batch_latency_us;
    case OP_RANGE: return g_sim_scan_latency_us;
    case OP_LOG_FLUSH: return SimTableClient::LogLatencyUs();
    default: return 0; // HELLO / metadata ops / checkpoint
  }
}

void SimNetServer::Dispatch(Conn *conn, uint64_t req_id, uint8_t op,
                            std::string payload) {
  auto latency = LatencyUs(op);
  if (latency == 0) {
    Execute(conn, req_id, op, payload);
    return;
  }
  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::microseconds(latency);
  {
    std::lock_guard<std::mutex> lk(task_latch_);
    tasks_.emplace(deadline, [this, conn, req_id, op,
                              payload = std::move(payload)] {
      Execute(conn, req_id, op, payload);
    });
  }
  task_cv_.notify_one();
}

void SimNetServer::ExecutorLoop() {
  prctl(PR_SET_TIMERSLACK, 1);
  std::unique_lock<std::mutex> lk(task_latch_);
  while (true) {
    task_cv_.wait(lk, [this] { return stop_ || !tasks_.empty(); });
    if (stop_) return; // pending requests die with the server
    auto it = tasks_.begin();
    auto deadline = it->first;
    if (std::chrono::steady_clock::now() + kSpinMargin < deadline) {
      // wake early if an earlier-deadline task arrives or stop is requested
      task_cv_.wait_until(lk, deadline - kSpinMargin);
      continue;
    }
    auto task = std::move(it->second);
    tasks_.erase(it);
    lk.unlock();
    while (std::chrono::steady_clock::now() < deadline) {
#if defined(__x86_64__) || defined(__i386__)
      __asm__ __volatile__("pause");
#endif
    }
    task();
    lk.lock();
  }
}

void SimNetServer::Reply(Conn *conn, uint64_t req_id, uint8_t status,
                         const std::string &payload) {
  std::lock_guard<std::mutex> lk(conn->write_latch);
  // a failed send means the client is gone; its run is over either way
  SendFrame(conn->fd, req_id, status, payload);
}

void SimNetServer::Execute(Conn *conn, uint64_t req_id, uint8_t op,
                           const std::string &payload) {
  Reader req(payload);
  std::string resp;
  switch (op) {
    case OP_HELLO: {
      auto magic = req.GetU64();
      Reply(conn, req_id, req.ok() && magic == kHelloMagic ? ST_OK : ST_ERROR,
            resp);
      return;
    }
    case OP_CREATE_TABLE:
    case OP_DELETE_TABLE: {
      std::string tbl;
      if (!req.GetStr(tbl)) break;
      if (op == OP_CREATE_TABLE) {
        store_.CreateTable(tbl);
      } else {
        store_.DeleteTable(tbl);
      }
      Reply(conn, req_id, ST_OK, resp);
      return;
    }
    case OP_LOAD: {
      std::string tbl, part, key;
      if (!req.GetStr(tbl) || !req.GetStr(part) || !req.GetStr(key)) break;
      auto rc = store_.LoadSync(tbl, part, key, resp);
      Reply(conn, req_id, rc == RC::OK ? ST_OK : ST_NOT_FOUND, resp);
      return;
    }
    case OP_STORE: {
      std::string tbl, part, key, value;
      if (!req.GetStr(tbl) || !req.GetStr(part) || !req.GetStr(key) ||
          !req.GetStr(value)) {
        break;
      }
      store_.StoreSync(tbl, part, key, (char *) value.data(), value.size());
      Reply(conn, req_id, ST_OK, resp);
      return;
    }
    case OP_STORE_AND_LOAD: {
      std::string tbl, store_part, key, value, load_part, load_key;
      if (!req.GetStr(tbl) || !req.GetStr(store_part) || !req.GetStr(key) ||
          !req.GetStr(value) || !req.GetStr(load_part) ||
          !req.GetStr(load_key)) {
        break;
      }
      store_.StoreSync(tbl, store_part, key, (char *) value.data(),
                       value.size());
      auto rc = store_.LoadSync(tbl, load_part, load_key, resp);
      Reply(conn, req_id, rc == RC::OK ? ST_OK : ST_ERROR, resp);
      return;
    }
    case OP_BATCH_STORE: {
      std::string tbl, part;
      if (!req.GetStr(tbl) || !req.GetStr(part)) break;
      auto cnt = req.GetU32();
      if (g_sim_enforce_batch_limit && cnt > 100) {
        // real Cosmos rejects the whole EGT, nothing is applied
        Reply(conn, req_id, ST_BATCH_TOO_LARGE, resp);
        return;
      }
      std::multimap<std::string, std::string> rows;
      std::string key, value;
      for (uint32_t i = 0; i < cnt; i++) {
        if (!req.GetStr(key) || !req.GetStr(value)) break;
        rows.emplace(key, value);
      }
      if (rows.size() != cnt) break;
      store_.BatchStoreSync(tbl, part, rows);
      Reply(conn, req_id, ST_OK, resp);
      return;
    }
    case OP_RANGE: {
      std::string tbl, part, low, high;
      if (!req.GetStr(tbl) || !req.GetStr(part) || !req.GetStr(low) ||
          !req.GetStr(high)) {
        break;
      }
      std::vector<std::string> values;
      store_.LoadRangeValues(tbl, part, low, high, values);
      if (g_sim_scan_per_row_ns != 0) {
        SimTableClient::InjectNs(values.size() * g_sim_scan_per_row_ns);
      }
      PutU32(resp, (uint32_t) values.size());
      for (auto &v : values) PutStr(resp, v);
      Reply(conn, req_id, ST_OK, resp);
      return;
    }
    case OP_LOG_FLUSH: {
      // payload is discarded: log content is never read back; the transfer
      // itself already cost network time proportional to the group size
      Reply(conn, req_id, ST_OK, resp);
      return;
    }
    case OP_CHECKPOINT: {
      store_.Checkpoint();
      Reply(conn, req_id, ST_OK, resp);
      return;
    }
    default:
      break;
  }
  LOG_INFO("SimNetServer: malformed request (op %u, %zu B)", op,
           payload.size());
  Reply(conn, req_id, ST_ERROR, resp);
}

} // namespace arboretum
