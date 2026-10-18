#include "common/Common.h"

#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_SIMULATED

#include "db/ARDB.h"
#include "remote/SimNetServer.h"

#include <atomic>
#include <csignal>
#include <sys/prctl.h>

using namespace arboretum;

// Standalone simulated-store server: reads the SAME
// experiment config file as the engine — the g_sim_*_latency_us knobs and
// g_sim_snapshot_dir belong to this process in network mode, and the port is
// taken from g_sim_server_addr. Launch one fresh server per experiment run:
//   ./SimStoreServer configs/<experiment>.cfg
// Snapshots live in <cwd>/<g_sim_snapshot_dir>/ as usual.

static std::atomic<bool> stop_requested(false);
static void OnSignal(int) { stop_requested = true; }

int main(int argc, char *argv[]) {
  ARDB::LoadConfig(argc, argv);
  if (g_sim_server_addr[0] == '\0') {
    LOG_ERROR("config has no g_sim_server_addr; the server needs its port");
  }
  std::string addr(g_sim_server_addr);
  auto colon = addr.rfind(':');
  if (colon == std::string::npos || colon + 1 == addr.size()) {
    LOG_ERROR("malformed g_sim_server_addr '%s' (want host:port)",
              g_sim_server_addr);
  }
  auto port = std::stoi(addr.substr(colon + 1));

  // die with the launching script: an orphaned server would linger holding a
  // multi-GB dataset and the port
  prctl(PR_SET_PDEATHSIG, SIGTERM);
  struct sigaction sa {};
  sa.sa_handler = OnSignal;
  sigaction(SIGTERM, &sa, nullptr);
  sigaction(SIGINT, &sa, nullptr);

  SimNetServer server;
  server.Start(port);
  LOG_INFO("SimStoreServer ready (config %s)", g_config_fname);
  while (!stop_requested) {
    usleep(100 * 1000);
  }
  server.Stop();
  LOG_INFO("SimStoreServer stopped");
  return 0;
}

#else // REMOTE_STORAGE_TYPE != REMOTE_STORAGE_SIMULATED

#include <cstdio>

int main() {
  fprintf(stderr, "SimStoreServer requires -DREMOTE_STORAGE=SIMULATED\n");
  return 1;
}

#endif
