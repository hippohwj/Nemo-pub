#include "GlobalData.h"
#include "Message.h"

namespace arboretum {

char g_config_fname[100] = "configs/sample.cfg";
char tikv_config_fname[100] = "configs/ifconfig_tikv.txt";
char redis_config_fname[100] = "configs/ifconfig_redis.txt";
char azure_config_fname[100] = "configs/ifconfig_azure.txt";
char g_sim_snapshot_dir[100] = "simstore_snapshots";
char g_sim_server_addr[100] = "";

char g_dataset_id[32] = "default";
char g_top_dataset_id[32] = "default";
char g_bottom_dataset_id[32] = "default";
// optional namespace suffix appended to every dataset id (and thus every
// remote table name), e.g. "_clv" -> obj-10g_clv-0; keeps reruns separate
// from legacy tables on a shared storage account
char g_dataset_suffix[16] = "";
// when set, record-dataset loads dump rows to tsv files in this directory
// instead of writing to the remote store (a python uploader ships them;
// the wastorage write path is pathologically slow / batch-incompatible)
char g_dump_load_dir[100] = "";
std::vector<bool> g_warmup_bitmap;
// comma-separated tbl ids whose REMOTE tables are skipped entirely
// (no CreateTable, no RestoreTable, no warmup anchors): lets narrow
// workload mixes (e.g. read-only TPC-C OrderStatus+StockLevel) run
// without provisioning Cosmos tables they never touch. The runtime
// NEVER guards accesses — only set ids the configured mix cannot reach.
char g_skip_tbl_ids[64] = "";
std::set<OID> g_skip_tbl_set;

// eviction write-back EGT outcome counters (Azure client only): failures
// used to be swallowed silently; both land in the per-run stats output so
// an unexercised path (0/0) is distinguishable from a healthy one
std::atomic<uint64_t> g_evict_batch_ok_cnt{0};
std::atomic<uint64_t> g_evict_batch_fail_cnt{0};

void SetDatasetId(char *dst, size_t cap, const std::string &id) {
  std::string full = id + g_dataset_suffix;
  M_ASSERT(full.size() < cap, "dataset id '%s' overflows %zu-byte buffer",
           full.c_str(), cap);
  std::strcpy(dst, full.c_str());
}
std::stringstream g_out_str;
GenRowHook g_gen_row_hook = nullptr;
std::atomic<uint64_t> g_warmup_generated{0}, g_warmup_fetched{0}, g_warmup_gen_dirty_skip{0};
std::atomic<uint64_t> g_evict_records{0}, g_evict_tasks{0}, g_evict_hand_steps{0}, g_evict_lock_wait_ns{0}, g_evict_lru_moves{0}, g_evict_task_ns{0};
std::atomic<uint64_t> g_evict_wr_rows{0}, g_evict_wr_ns{0};
std::atomic<uint64_t> g_scan_calls{0}, g_scan_rows_buf{0}, g_scan_rows_fetched{0},
    g_scan_rows_wasted{0};
std::atomic<uint64_t> g_scan_gaps_raw{0}, g_scan_gaps_merged{0};
std::atomic<uint64_t> g_scan_query_fail{0};
std::atomic<uint64_t> g_egt_unparsed{0};
std::atomic<bool> g_warmup_budget_hit{false};
std::atomic<uint64_t> g_null_tuple_skips{0};
std::atomic<uint64_t> g_thr429_rd{0}, g_thr429_wr{0}, g_thr429_rw{0};
std::atomic<uint64_t> g_thr_backoff_ns{0};
std::atomic<uint64_t> g_thr429_tbl[kThrottleSlots];
std::atomic<uint64_t> g_pgbuf_evicts{0}, g_pgbuf_evict_steps{0};
std::atomic<uint64_t> g_pgbuf_evict_ns{0}, g_pgbuf_lockwait_ns{0};
std::atomic<uint64_t> g_pgbuf_lock_acqs{0};
std::atomic<bool> g_latch_on{false};
std::atomic<uint64_t> g_latch_wait_ns_ex{0}, g_latch_wait_ns_sh{0}, g_latch_hold_ns_ex{0}, g_latch_ex_cnt{0}, g_latch_sh_cnt{0};
std::ofstream g_out_file;
char g_out_fname[100] = "sample.out";
volatile bool g_terminate_exec = false;
volatile bool g_terminate_evict = false;

DB_CONFIGS(DEFN_GLOBAL_CONFIG2, DEFN_GLOBAL_CONFIG3,
           DEFN_GLOBAL_CONFIG4, DEFN_GLOBAL_CONFIG5)


} // arboretum