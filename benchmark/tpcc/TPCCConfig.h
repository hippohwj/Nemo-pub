#ifndef ARBORETUM_BENCHMARK_TPCC_TPCCCONFIG_H_
#define ARBORETUM_BENCHMARK_TPCC_TPCCCONFIG_H_

#include "common/BenchConfig.h"

namespace arboretum {

// fields owned by TPCCConfig itself; shared BENCH_CONFIG fields stay in
// BenchConfig so BenchWorker::PrintStats(BenchConfig*) reads the real values
#define TPCC_CONFIG_OWN(x) \
x(uint64_t, num_warehouses_, 10) \
x(uint64_t, order_cap_per_district_, 6000) \
x(uint64_t, history_cap_per_district_, 8000) \
x(double, perc_neworder_, 0.45) \
x(double, perc_payment_, 0.43) \
x(double, perc_orderstatus_, 0.04) \
x(double, perc_delivery_, 0.04) \
x(double, perc_stocklevel_, 0.04) \
x(double, by_name_perc_, 0.6) \
x(double, payment_remote_perc_, 0.15) \
x(double, remote_ol_perc_, 0.01) \
x(double, neworder_rollback_perc_, 0.01) \
x(double, wh_zipf_theta_, 0) \
x(double, rec_zipf_theta_, 0) \
x(int64_t, rand_order_reads_, 0) \
x(int64_t, cust_payment_, 0) \
x(int64_t, redraw_on_abort_, 0) \
x(int64_t, global_cust_zipf_, 0) \
x(int64_t, cust_hot_scatter_, 0) \
x(int64_t, order_hot_scatter_, 0) \
x(int64_t, loading_start_wh_, 0) \
x(int64_t, loading_end_wh_, -1)
#define TPCC_CONFIG(x) TPCC_CONFIG_OWN(x) BENCH_CONFIG(x)

class TPCCConfig : public BenchConfig {
 public:
  TPCCConfig(int argc, char * argv[]) : BenchConfig() {
    if (argc > 1) {
      std::strncpy(g_config_fname, argv[1], 100);
    }
    LOG_INFO("Loading TPCC Config from file: %s", g_config_fname);
    read_json(g_config_fname, config_);
    auto config_name = config_.get<std::string>("config_name");
    LOG_INFO("Loading TPCC Config: %s", config_name.c_str());
    for (ptree::value_type &item : config_.get_child("tpcc_config")){
      TPCC_CONFIG(IF_GLOBAL_CONFIG3)
    }
    num_workers_ = g_num_worker_threads;
    TPCC_CONFIG(PRINT_BENCH_CONFIG)
  };
  TPCC_CONFIG_OWN(DECL_BENCH_CONFIG)
};

}

#endif //ARBORETUM_BENCHMARK_TPCC_TPCCCONFIG_H_
