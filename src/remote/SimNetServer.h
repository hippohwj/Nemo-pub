#ifndef ARBORETUM_SRC_REMOTE_SIMNETSERVER_H_
#define ARBORETUM_SRC_REMOTE_SIMNETSERVER_H_

#include "SimTableClient.h"

#include <atomic>
#include <condition_variable>
#include <thread>
#include <vector>

namespace arboretum {

// Network front-end of the simulated store: a
// SimTableClient embedded with inject_latency=false holds the data and
// snapshots; the server injects all latency by scheduling each request on a
// deadline queue (deadline = arrival + g_sim_*_latency_us for its op class)
// and replying after applying the µs-scale map operation. Sleeping happens
// in queue entries, not threads, so g_sim_server_exec_thds executors cover
// any number of in-flight requests. One reader thread per connection.
//
// Usually wrapped by the SimStoreServer binary (one fresh server per
// experiment run); unit tests embed it directly on port 0.
class SimNetServer {
 public:
  ~SimNetServer();
  // bind + listen + spawn accept/executor threads; port 0 picks a free
  // port; returns the bound port
  int Start(int port);
  // shuts down all threads and connections; pending scheduled requests are
  // dropped (their replies die with the run, like a real server crash)
  void Stop();

 private:
  struct Conn {
    int fd;
    std::mutex write_latch; // executors and the reader interleave replies
    std::thread reader;
  };

  void AcceptLoop();
  void ReaderLoop(Conn *conn);
  // schedules the request at now + LatencyUs(op), or executes it inline on
  // the reader thread when the latency is 0 (e.g. the load phase)
  void Dispatch(Conn *conn, uint64_t req_id, uint8_t op, std::string payload);
  void Execute(Conn *conn, uint64_t req_id, uint8_t op,
               const std::string &payload);
  void Reply(Conn *conn, uint64_t req_id, uint8_t status,
             const std::string &payload);
  void ExecutorLoop();
  static uint64_t LatencyUs(uint8_t op);

  SimTableClient store_{false}; // the scheduler owns all latency injection

  int listen_fd_{-1};
  std::atomic<bool> stop_{false};
  std::thread accept_thd_;
  std::mutex conns_latch_;
  std::vector<Conn *> conns_;

  std::mutex task_latch_;
  std::condition_variable task_cv_;
  std::multimap<std::chrono::steady_clock::time_point, std::function<void()>>
      tasks_;
  std::vector<std::thread> executors_;
};

} // namespace arboretum

#endif // ARBORETUM_SRC_REMOTE_SIMNETSERVER_H_
