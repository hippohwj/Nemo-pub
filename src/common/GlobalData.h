#ifndef ARBORETUM_SRC_COMMON_GLOBALDATA_H_
#define ARBORETUM_SRC_COMMON_GLOBALDATA_H_

#include "Types.h"
// #include <cstddef>   // for size_t
#include <limits>    // for std::numeric_limits
#include <vector>
#include <atomic>
#include <string>
#include <cstdlib>

namespace arboretum {

// 2tree top-miss diagnosis: which keys the HYBRBUF warmup fabricated into
// the top tree (write-once in warmup, read-only afterwards). Empty unless
// the HYBRBUF warmup ran.
extern std::vector<bool> g_warmup_bitmap;

#define DECL_GLOBAL_CONFIG2(tpe, var) extern tpe var;
#define DECL_GLOBAL_CONFIG3(tpe, var, val) DECL_GLOBAL_CONFIG2(tpe, var)
#define DECL_GLOBAL_CONFIG4(tpe, var, val, f) DECL_GLOBAL_CONFIG2(tpe, var)
#define DECL_GLOBAL_CONFIG5(tpe, var, val, f1, f2) DECL_GLOBAL_CONFIG2(tpe, var)
#define DEFN_GLOBAL_CONFIG2(tpe, var) tpe var;
#define DEFN_GLOBAL_CONFIG3(tpe, var, val) tpe var = val;
#define DEFN_GLOBAL_CONFIG4(tpe, var, val, f) DEFN_GLOBAL_CONFIG3(tpe, var, val)
#define DEFN_GLOBAL_CONFIG5(tpe, var, val, f1, f2) DEFN_GLOBAL_CONFIG3(tpe, var, val)
#define IF_GLOBAL_CONFIG2(tpe, var) ;
#define IF_GLOBAL_CONFIG3(tpe, var, val) \
if (item.first == #var) { (var) = item.second.get_value<tpe>(); continue; }
#define IF_GLOBAL_CONFIG4(tpe, var, val, f) IF_GLOBAL_CONFIG3(tpe, var, val)
#define IF_GLOBAL_CONFIG5(tpe, var, val, f1, f2)          \
if (item.first == #var) {                                 \
  auto str = item.second.get_value<std::string>();        \
  (var) = f2(str); continue;                              \
}
#define PRINT_DB_CONFIG2(tpe, name) ;
#define PRINT_DB_CONFIG3(tpe, name, val) { \
std::cout << #name << ": " << (name) << std::endl; \
if (g_save_output) g_out_str << "\"" << #name << "\"" << ": " << (name) << ", "; }
#define PRINT_DB_CONFIG4(tpe, name, val, func) { \
std::cout << #name << ": " << func(name) << std::endl; \
if (g_save_output) g_out_str << "\"" << #name << "\"" << ": \"" \
<< func(name) << "\", "; }
#define PRINT_DB_CONFIG5(tpe, name, val, f1, f2) \
PRINT_DB_CONFIG4(tpe, name, val, f1)

// pre-declaration
class BufferManager;
class IDataStore;
class ILogStore;
class Stats;

// Configuration
// ====================
extern char g_config_fname[100];
extern char tikv_config_fname[100];
extern char redis_config_fname[100];
extern char azure_config_fname[100];
extern char g_sim_snapshot_dir[100];
// "" = in-process simulated store; "host:port" = SimStoreServer over TCP
// (latency then injected server-side)
extern char g_sim_server_addr[100];
inline bool SimNetEnabled() { return g_sim_server_addr[0] != '\0'; }
extern char g_dataset_id[32];
extern char g_top_dataset_id[32];
extern char g_bottom_dataset_id[32];
extern char g_dataset_suffix[16];
extern char g_dump_load_dir[100];
extern char g_skip_tbl_ids[64];
extern std::set<OID> g_skip_tbl_set;
inline bool IsSkippedTable(OID tbl_id) {
  return !g_skip_tbl_set.empty() && g_skip_tbl_set.count(tbl_id) > 0;
}
extern std::atomic<uint64_t> g_evict_batch_ok_cnt;
extern std::atomic<uint64_t> g_evict_batch_fail_cnt;
// composes id + g_dataset_suffix into dst, asserting it fits
void SetDatasetId(char *dst, size_t cap, const std::string &id);

// Index
// ====================
#define DB_INDEX_CONFIG(x, y, z, a)                         \
a(IndexType, g_index_type, IndexType::REMOTE,               \
IndexTypeToString, StringToIndexType)                       \
y(size_t, g_idx_btree_fanout, 250)                          \
y(double, g_idx_btree_split_ratio, 0.9)                     \
z(bool, g_uni_page_idx_tbl, true, BoolToString)                

// Local Storage
// ====================
#define AR_PAGE_SIZE 8192
#define DB_LOCAL_STORE_CONFIG(x, y, z, a)                    \
x(BufferManager *, g_buf_mgr)                                \
x(BufferManager *, g_top_buf_mgr)                                \
x(BufferManager *, g_bottom_buf_mgr)                                \
a(BufferType, g_buf_type, NOBUF,                             \
BufferTypeToString, StringToBufferType)                      \
y(size_t, g_total_buf_sz, 128 * 1024 * 1024)                 \
y(size_t, g_top_tree_buf_sz, 128 * 1024 * 1024)                 \
y(size_t, g_bottom_tree_buf_sz, 128 * 1024 * 1024)                 \
y(size_t, g_pagebuf_num_slots, g_total_buf_sz / AR_PAGE_SIZE) \
y(size_t, g_buf_entry_sz, 1000)                            \
y(size_t, g_batch_eviction_period_ms, 100)                            \
z(bool, g_retain_idx_page, false, BoolToString)            \
z(bool, g_pgbuf_debug, false, BoolToString)                \
z(bool, g_pgbuf_census, false, BoolToString)               \
z(bool, g_pgbuf_no_preload, false, BoolToString)           \
z(bool, g_retain_idx_evict_cold, false, BoolToString)            \
z(bool, g_generative_warmup, false, BoolToString)           \
z(bool, g_evict_write_once_first, false, BoolToString)     \
z(bool, g_evict_policy_bench, false, BoolToString)         \
z(bool, g_evict_lru, false, BoolToString)                   \
y(size_t, g_evict_meta_bytes, 0)                            \
z(bool, g_warmup_skip_log, false, BoolToString)             \
y(size_t, g_warmup_workers, 0)                              \
z(bool, g_batch_eviction, false, BoolToString)             \
y(size_t, g_warmup_slot_budget, 0)                          \
y(size_t, g_dump_write_latency_us, 0)                       \
y(size_t, g_log_write_latency_us, 0)                        \
z(bool, g_evict_batch_egt, false, BoolToString)             \
z(bool, g_eviction_simple_clock, true, BoolToString)       \
 
// Remote Storage
// ====================
#define DB_REMOTE_STORE_CONFIG(x, y, z, a)                 \
x(IDataStore *, g_data_store)                              \
x(ILogStore *, g_log_store)                                \
z(bool, g_enable_group_commit, true, BoolToString)         \
y(size_t, g_commit_queue_limit, 5)                         \
y(size_t, g_commit_group_sz, 32)                           \
y(uint64_t, g_log_freq_us, 50)                             \
z(bool, g_enable_log, true, BoolToString)                  \
z(bool, g_load_to_remote_only, false, BoolToString)        \
z(bool, g_restore_from_remote, true, BoolToString)         \
z(bool, g_check_loaded, false, BoolToString)               \
z(bool, g_tpcc_consistency_check, false, BoolToString)     \
z(bool, g_tpcc_blind_inserts, false, BoolToString)         \
y(size_t, g_partition_sz, 10000)                           \
z(bool, g_load_range, false, BoolToString)                 \
y(size_t, g_num_restore_thds, 8)                           \
y(size_t, g_num_load_thds, 8)                              \
y(int, g_remote_req_retries, 100)                          \
y(size_t, g_sim_rd_latency_us, 0)                          \
y(size_t, g_sim_wr_latency_us, 0)                          \
y(size_t, g_sim_rw_latency_us, 0)                          \
y(size_t, g_sim_scan_latency_us, 0)                        \
y(size_t, g_sim_scan_per_row_ns, 0)                        \
y(size_t, g_sim_batch_latency_us, 0)                       \
y(size_t, g_sim_log_latency_us, 0)                         \
y(size_t, g_sim_async_thds, 4)                             \
y(size_t, g_sim_server_exec_thds, 8)                       \
z(bool, g_sim_warmup_no_latency, false, BoolToString)      \
z(bool, g_sim_enforce_batch_limit, false, BoolToString)

// System
// ====================
#define DB_SYSTEM_CONFIG(x, y, z, a)                       \
x(Stats *, g_stats) y(size_t, g_num_worker_threads, 1)     \
y(size_t, g_commit_pool_sz, 4) y(double, g_cpu_freq, 1)    \
y(size_t, g_num_evict_thds, 5)                             \
y(size_t, g_batch_write_fanout, 1)                         \
y(size_t, g_scan_gap_merge, 1000000000)                     \
z(bool, g_scan_cpp_query, false, BoolToString)              \
z(bool, g_evict_dirty_last, false, BoolToString)           \
z(bool, g_latch_bench, false, BoolToString)                \
z(bool, g_evict_flush_outside_latch, false, BoolToString)  \
y(double, g_evict_threshold, 0.7)                     \
z(bool, g_warmup_finished, false, BoolToString)            \
z(bool, g_enable_logging, true, BoolToString)              \
z(bool, g_force_write, false, BoolToString)                \
z(bool, g_zipf_random_hotspots, false, BoolToString)                \
z(bool, g_scan_zipf_random_hotspots, false, BoolToString)                \
y(size_t, g_scan_zipf_partition_sz, 10000)                             \
y(OID, g_node_id, 0) z(bool, g_save_output, false, BoolToString) \
z(bool, g_early_lock_release, false, BoolToString)         \
z(bool, g_enable_phantom_protection, false, BoolToString)  \
z(bool, g_enable_partition_covering_lock, false, BoolToString) \
z(bool, g_negative_point_wl, false, BoolToString) \
z(bool, g_negative_scan_wl, false, BoolToString) \
a(NegativeSearchOpType, g_negative_search_op_type, GAPBIT_PHANTOM,                             \
  NegativeOpTypeToString, StringToNegativeOpType)                      \
a(PhantomProtectionType, g_phantom_protection_type, NO_PRO,                             \
  PhantomProtectionTypeToString, StringToPhantomProtectionType)                      \
z(bool, g_negative_search_op_enable, true, BoolToString) \
z(bool, g_gap_bit_op_enable, true, BoolToString) \
y(size_t, g_partition_covering_lock_unit_sz, 10000)         \
z(bool, g_enable_latency_distr_log, false, BoolToString)  \
y(size_t, g_scan_length, 999)         \
z(bool, g_scan_query_scatter, true, BoolToString)         \
y(size_t, g_negative_dataset_seed, 0)         \
y(size_t, g_scan_workload_rw_thd_num, std::numeric_limits<std::size_t>::max()) \
y(size_t, g_workingset_partition_num, 0)     \
z(bool, g_scan_materialize, false, BoolToString) \
y(size_t, g_record_size, 1008)


extern std::stringstream g_out_str;
// generative warmup (TPC-C): while !g_warmup_finished, a record-buffer miss
// is served by regenerating the row from the benchmark's deterministic
// loader instead of a remote read. The workload registers the generator;
// returns false if the row cannot be generated (caller then fetches).
typedef bool (*GenRowHook)(uint32_t tbl_id, uint64_t key, char *buf, size_t sz);
extern GenRowHook g_gen_row_hook;
extern std::atomic<uint64_t> g_warmup_generated, g_warmup_fetched, g_warmup_gen_dirty_skip;
// buffer-replacement microbenchmark counters (g_evict_policy_bench):
// records retired, eviction tasks (nemo: leaf batches; clock/lru: single
// victims), clock hand steps, lock/latch wait ns on the eviction structure,
// LRU move-to-front ops
extern std::atomic<uint64_t> g_evict_records, g_evict_tasks, g_evict_hand_steps,
    g_evict_lock_wait_ns, g_evict_lru_moves, g_evict_task_ns;
// eviction write-back, the path no counter used to cover: rows actually
// pushed to remote storage and the wall time the flushing threads spent
// inside those writes (sum over threads, so > wall when fanned out)
extern std::atomic<uint64_t> g_evict_wr_rows, g_evict_wr_ns;
// Range-scan buffer coverage. The scan path never skips a remote fetch: the
// per-scan count of uncovered sub-ranges is exactly 1 at p0 through p100, at
// every buffer ratio, on Cosmos AND in simulation. These four say whether
// that is because the rows are genuinely absent, or because RangeSearch
// cannot prove a gap is empty and refetches rows the buffer already holds --
// which is what decides whether replacing the range fetch with point reads
// would pay. rows_wasted counts fetched rows thrown away by the
// g_scan_materialize merge because the slot was already filled from cache.
extern std::atomic<uint64_t> g_scan_calls, g_scan_rows_buf, g_scan_rows_fetched,
    g_scan_rows_wasted;
// gap structure of a range scan, before and after coalescing. RangeEntryIter
// records the true holes, but RangeSearch used to collapse them into one
// bounding interval so that a scan always cost exactly one remote query --
// at the price of refetching rows the buffer already held.
// g_scan_gap_merge is the coalescing threshold in resident keys: 0 issues the
// exact holes, a huge value reproduces the old bounding-box behaviour.
extern std::atomic<uint64_t> g_scan_gaps_raw, g_scan_gaps_merged;
// range scans that exhausted their retries and returned NOTHING. Such a scan
// makes its transaction read an empty range, so any run with a non-zero count
// is disqualified rather than merely noisy.
extern std::atomic<uint64_t> g_scan_query_fail;
// entity-group txns that Cosmos executed (HTTP 202, rows verified present)
// but whose multipart response the SDK could not parse. Swallowed on purpose;
// counted so an unexpected rate is visible rather than silent.
extern std::atomic<uint64_t> g_egt_unparsed;
// warm-up slot budget (g_warmup_slot_budget): set by the first district whose
// NewOrder/history cursor reaches the budget, polled by the driver so the
// warm-up ends there instead of at warmup_time_. Bounds slot consumption
// without touching transaction semantics. (Rewinding the cursor instead is
// not equivalent: the measured window then re-inserts still-resident warm-up
// keys, which degrades the blind insert into an in-place update and inflates
// the hit rate.)
extern std::atomic<bool> g_warmup_budget_hit;
// accesses whose slot was reclaimed before PreCommit could apply the update
extern std::atomic<uint64_t> g_null_tuple_skips;
// Remote-store throttling. Cosmos answers an over-budget request with 429 and
// every client path then sleeps and retries. In a Release build LOG_DEBUG is
// empty and the retry is invisible, so a run that spends three quarters of its
// worker time asleep in that backoff reads exactly like one that never blocks:
// the operation is charged to remote_wr_time_/remote_rd_time_ as if it were
// service latency. These counters separate the two. g_thr429_tbl is keyed by
// ThrottleSlot(): data tables land at 0..30 and index tables at 32+t, so a
// throttled table can be named -- which is what sizing the per-table RU plan
// needs, and index tables must not all collapse into one bucket to do it.
constexpr int kThrottleSlots = 64;
// Storage id -> histogram slot, shared by the 429 histogram and the per-table
// read/write histogram so the two can be read side by side.
//   "<dataset>-<t>"        data table t        -> slot t          (t < 31)
//   "<dataset>-<tag+1>"    index of table t    -> slot 32+t
// with tag = ((t+1)<<28)+t. Index tables get their own slots: they take no
// writes at all, yet one of them (the ORDER index) can be the read-hottest
// table in the dataset, and folding every index into a single "other" bucket
// would hide that. Anything unparseable (e.g. "<dataset>-log") lands in the
// last slot.
inline int RemoteTableSlot(const std::string &tbl_name) {
  auto pos = tbl_name.rfind('-');
  if (pos == std::string::npos || pos + 1 >= tbl_name.size())
    return kThrottleSlots - 1;
  char *end = nullptr;
  auto id = std::strtol(tbl_name.c_str() + pos + 1, &end, 10);
  if (end == nullptr || *end != '\0' || id < 0) return kThrottleSlots - 1;
  if (id >= (1L << 28)) {
    auto t = (id - 1) & 0x0FFFFFFFL;
    return (t < 31) ? 32 + (int) t : kThrottleSlots - 1;
  }
  return (id < 31) ? (int) id : kThrottleSlots - 1;
}
extern std::atomic<uint64_t> g_thr429_rd, g_thr429_wr, g_thr429_rw;
extern std::atomic<uint64_t> g_thr_backoff_ns;
extern std::atomic<uint64_t> g_thr429_tbl[kThrottleSlots];
// PGBUF eviction path accounting. The clock scan runs while free_desc_mutex_
// -- ONE global mutex for the whole pool -- is held (locked at
// PageBufferManager.cpp:171/266, released inside FindEmptyBufDesc), so every
// miss that has to evict serialises against every other, and that wait lands
// in no other counter (it is neither remote I/O nor cc_time_). These counters
// make the scan length and the mutex hold/wait time visible.
extern std::atomic<uint64_t> g_pgbuf_evicts, g_pgbuf_evict_steps;
extern std::atomic<uint64_t> g_pgbuf_evict_ns, g_pgbuf_lockwait_ns;
extern std::atomic<uint64_t> g_pgbuf_lock_acqs;
// B-tree node latch accounting (g_latch_bench). Latch waits land in no other
// counter: cc_time_ covers the lock table but not B-tree node latches, so
// without these a latch-bound run is indistinguishable from a CPU-bound one.
// Declared in RWLock.h too -- keep the two in sync.
extern std::ofstream g_out_file;
extern char g_out_fname[100];
extern volatile bool g_terminate_exec;
extern volatile bool g_terminate_evict;

#define DB_CONFIGS(x, y, z, a)     \
DB_INDEX_CONFIG(x, y, z, a)        \
DB_LOCAL_STORE_CONFIG(x, y, z, a)  \
DB_REMOTE_STORE_CONFIG(x, y, z, a) \
DB_SYSTEM_CONFIG(x, y, z, a)
DB_CONFIGS(DECL_GLOBAL_CONFIG2, DECL_GLOBAL_CONFIG3,
           DECL_GLOBAL_CONFIG4, DECL_GLOBAL_CONFIG5)

} 

#endif //ARBORETUM_SRC_COMMON_GLOBALDATA_H_
