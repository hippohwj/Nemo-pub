#include "BenchWorker.h"

namespace arboretum {

BenchStats BenchWorker::sum_bench_stats_;
std::mutex BenchWorker::sum_latch_;

BenchWorker::BenchWorker() {
  // pass a query execution function
  // start a new thread to execute it
  // collect stats
}

}