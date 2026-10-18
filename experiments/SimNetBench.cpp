#include "common/Common.h"

#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_SIMULATED

#include "db/ARDB.h"
#include "local/ITuple.h"
#include "remote/SimNetClient.h"

#include <algorithm>
#include <chrono>

using namespace arboretum;

// Network-mode calibration microbench. Run against a
// SimStoreServer started with the same config:
//   with all g_sim_*_latency_us = 0  -> per-op-class base overhead (RTT +
//     serialization + scheduling) to subtract from the Cosmos targets;
//   with calibrated values           -> end-to-end verification (should sit
//     within ~2% of the targets).
// Usage: ./SimNetBench configs/<simnet config>.cfg

static uint64_t NowUs() {
  return (uint64_t) std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

struct LatencyStats {
  std::vector<uint64_t> samples;
  void Report(const char *name) {
    std::sort(samples.begin(), samples.end());
    double sum = 0;
    for (auto s : samples) sum += (double) s;
    auto pct = [&](double p) {
      return samples[std::min(samples.size() - 1,
                              (size_t) (p * (double) samples.size()))];
    };
    printf("%-22s n=%-6zu mean=%8.1f us  min=%6lu  p50=%6lu  p95=%6lu  "
           "p99=%6lu\n",
           name, samples.size(), sum / (double) samples.size(), samples[0],
           pct(0.5), pct(0.95), pct(0.99));
  }
};

template <typename F>
static void Measure(const char *name, int trials, F &&op) {
  LatencyStats stats;
  stats.samples.reserve(trials);
  for (int i = 0; i < trials; i++) {
    auto start = NowUs();
    op(i);
    stats.samples.push_back(NowUs() - start);
  }
  stats.Report(name);
}

int main(int argc, char *argv[]) {
  ARDB::LoadConfig(argc, argv);
  if (!SimNetEnabled()) {
    LOG_ERROR("SimNetBench needs g_sim_server_addr in the config");
  }
  SimNetClient client;
  const std::string tbl = "simnetbench";
  client.CreateTable(tbl);

  const int trials = 2000;
  const size_t record_sz = 108;  // TPC-C style tuple
  const size_t page_sz = 8192;   // PGBUF page
  std::string record(record_sz, 'r'), page(page_sz, 'p');

  // preload the rows the read/scan loops touch
  for (int i = 0; i < trials; i++) {
    client.StoreSync(tbl, "0", SearchKey((uint64_t) i).ToString(),
                     record.data(), record.size());
  }

  printf("--- SimNetBench against %s ---\n", g_sim_server_addr);
  Measure("point read 108B", trials, [&](int i) {
    std::string out;
    client.LoadSync(tbl, "0", SearchKey((uint64_t) i).ToString(), out);
  });
  Measure("point write 108B", trials, [&](int i) {
    client.StoreSync(tbl, "1", SearchKey((uint64_t) i).ToString(),
                     record.data(), record.size());
  });
  Measure("point write 8KB", trials, [&](int i) {
    client.StoreSync(tbl, "2", SearchKey((uint64_t) i).ToString(),
                     page.data(), page.size());
  });
  Measure("rw pair 108B", trials, [&](int i) {
    std::string out;
    client.StoreAndLoadSync(tbl, "1", SearchKey((uint64_t) i).ToString(),
                            record.data(), record.size(), "0",
                            SearchKey((uint64_t) i).ToString(), out);
  });
  // one eviction-sized batch = one round trip (32 rows, TPC-C record size)
  Measure("batch 32x108B", trials, [&](int i) {
    std::multimap<std::string, std::string> batch;
    for (uint64_t k = 0; k < 32; k++) {
      batch.emplace(SearchKey((uint64_t) i * 32 + k).ToString(), record);
    }
    client.BatchStoreSync(tbl, "3", batch);
  });
  // a group-commit flush of 32 txns x ~100B of log
  std::string log_payload(32 * 100, 'l');
  Measure("log flush 3.2KB", trials, [&](int i) {
    client.LogFlush(tbl + "-log", log_payload.data(), log_payload.size());
  });
  const size_t total_tuple_sz = sizeof(ITuple) + record_sz;
  Measure("scan 100 rows", trials, [&](int i) {
    std::string t = tbl, part = "0";
    auto lo = (uint64_t) (i % (trials - 100));
    char *tuples = nullptr;
    size_t cnt = 0;
    client.LoadRangePython(t, part, SearchKey(lo).ToString(),
                           SearchKey(lo + 99).ToString(), &tuples,
                           total_tuple_sz, cnt);
    if (tuples != nullptr) DEALLOC(tuples);
  });
  return 0;
}

#else // REMOTE_STORAGE_TYPE != REMOTE_STORAGE_SIMULATED

#include <cstdio>

int main() {
  fprintf(stderr, "SimNetBench requires -DREMOTE_STORAGE=SIMULATED\n");
  return 1;
}

#endif
