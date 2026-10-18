#ifndef ARBORETUM_BENCHMARK_TPCC_H_
#define ARBORETUM_BENCHMARK_TPCC_H_

#include "tpcc/TPCCConfig.h"
#include "tpcc/TPCCWorkload.h"
#include "common/BenchWorker.h"
#include "db/ARDB.h"

namespace arboretum {
namespace tpcc {

void tpcc(ARDB *db, TPCCConfig *config) {
  LOG_INFO("Start tpcc workload");
  auto workload = TPCCWorkload(db, config);
  std::vector<std::thread> threads;
  BenchWorker workers[config->num_workers_];
  for (size_t i = 0; i < config->num_workers_; i++) {
    workers[i].worker_id_ = i;
    threads.emplace_back(TPCCWorkload::Execute, &workload, &workers[i]);
  }
  // warmup-only workers: extra threads that run the same mix and exit when
  // warmup ends (their stats are gated off); with generative warmup they
  // fill the buffer at CPU speed instead of remote-latency speed
  std::vector<std::thread> warm_threads;
  std::vector<BenchWorker> warm_workers(g_warmup_workers);
  for (size_t i = 0; i < g_warmup_workers; i++) {
    warm_workers[i].worker_id_ = config->num_workers_ + i;
    warm_workers[i].warmup_only_ = true;
    warm_threads.emplace_back(TPCCWorkload::Execute, &workload, &warm_workers[i]);
  }

  // Warm-up ends at whichever comes first: warmup_time_, or the first district
  // reaching g_warmup_slot_budget. Generative warm-up runs the full mix at CPU
  // speed, so a purely time-bounded warm-up can consume nearly all of a
  // district's NewOrder slots before the measured window opens. Bounding the
  // warm-up leaves every transaction's semantics untouched (rewinding the
  // cursor instead would let the measured window re-insert still-resident
  // warm-up keys, inflating the hit rate). The price is that points can
  // differ in warm-up length -- the same treatment the page baseline already
  // gets (pgbuf_warmup() in the grid), and it is reported per point rather
  // than assumed.
  auto warm_t0 = GetSystemClock();
  if (g_warmup_slot_budget > 0) {
    for (uint64_t i = 0; i < config->warmup_time_ * 10; i++) {
      if (g_warmup_budget_hit.load(std::memory_order_acquire)) break;
      usleep(100000);
    }
  } else {
    sleep(config->warmup_time_);
  }
  auto warm_secs = (GetSystemClock() - warm_t0) / 1e9;
  if (g_warmup_slot_budget > 0) {
    LOG_INFO("warm-up ended after %.1f s of a %lu s cap (%s); slot budget %lu "
             "per district above the loaded %d", warm_secs,
             config->warmup_time_,
             g_warmup_budget_hit.load() ? "budget reached" : "time expired",
             g_warmup_slot_budget, (int) tpcc::INIT_ORDERS_PER_DIST);
    if (g_save_output) {
      g_out_str << "\"warmup_actual_sec_\": " << warm_secs << ",";
      g_out_str << "\"warmup_budget_hit_\": "
                << (g_warmup_budget_hit.load() ? 1 : 0) << ",";
    }
  }
  g_warmup_finished = true;
  // latch accounting starts with the measured window, not the warm-up
  arboretum::g_latch_on.store(g_latch_bench, std::memory_order_relaxed);
  LOG_INFO("Finished warming up");
  if (g_generative_warmup) {
    LOG_INFO("generative warmup: generated %lu rows locally, fetched %lu remotely "
             "(dirtied-key skips %lu)", g_warmup_generated.load(),
             g_warmup_fetched.load(), g_warmup_gen_dirty_skip.load());
    TPCCWorkload::PrintGenHookStats();
    if (g_save_output) {
      g_out_str << "\"warmup_generated_\": " << g_warmup_generated.load() << ",";
      g_out_str << "\"warmup_fetched_\": " << g_warmup_fetched.load() << ",";
    }
  }
  std::for_each(warm_threads.begin(), warm_threads.end(), std::mem_fn(&std::thread::join));
  arboretum::ARDB::PrintWarmedUpBuffer();

  sleep(config->runtime_);
  // join worker threads; each worker sums its own bench stats into
  // sum_bench_stats_ at the end of Execute
  db->Terminate(threads);
  BenchWorker::PrintStats(config);
  if (g_save_output) {
    g_out_file.open(g_out_fname, std::ios_base::app);
    g_out_file << "{" << g_out_str.str() << "}" << std::endl;
  }
  // correctness gate: validate remote state after all stats are persisted;
  // fails loudly (exit 1) on any consistency violation
  workload.CheckConsistency();
}

} // tpcc
} // arboretum

#endif //ARBORETUM_BENCHMARK_TPCC_H_
