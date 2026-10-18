#include "ObjectBufferManager.h"
#include <unordered_set>
#include <mutex>
#include "local/ITable.h"
#include "db/ARDB.h"
#include "remote/IDataStore.h"
#include "common/Worker.h"
#include "common/GlobalData.h"

namespace arboretum {

// ---- generative-warmup dirtied-key registry (striped) ----
namespace {
constexpr size_t kDirtyStripes = 64;
struct DirtyStripe { std::mutex m; std::unordered_set<uint64_t> keys; };
DirtyStripe g_dirty_stripes[kDirtyStripes];
inline uint64_t DirtyKey(uint32_t tbl_id, uint64_t key) { return (((uint64_t) tbl_id) << 56) ^ key; }
}
void ObjectBufferManager::NoteDirtied(uint32_t tbl_id, uint64_t key) {
  auto k = DirtyKey(tbl_id, key);
  auto &st = g_dirty_stripes[k % kDirtyStripes];
  std::lock_guard<std::mutex> g(st.m);
  st.keys.insert(k);
}
bool ObjectBufferManager::WasDirtied(uint32_t tbl_id, uint64_t key) {
  auto k = DirtyKey(tbl_id, key);
  auto &st = g_dirty_stripes[k % kDirtyStripes];
  std::lock_guard<std::mutex> g(st.m);
  return st.keys.count(k) != 0;
}

ObjectBufferManager::ObjectBufferManager(ARDB *db) : db_(db) {
  // auto table = db_->GetTable(0);
  // g_buf_entry_sz = table->GetTupleDataSize();
  g_buf_entry_sz = g_record_size;
  LOG_INFO("Buffer entry size: %d", g_buf_entry_sz);
  if (g_buf_type == HYBRBUF) {
    num_slots_ = g_top_tree_buf_sz * 1.0 / g_buf_entry_sz; // an estimate for now
  } else {
    if (g_batch_eviction) {
      num_slots_ = g_total_buf_sz * 1.0 / g_buf_entry_sz; // an estimate for now
    } else {
      g_buf_entry_sz = g_record_size + 10;
      num_slots_ = g_total_buf_sz * 1.0 / g_buf_entry_sz; // an estimate for now
      // num_slots_ = g_total_buf_sz * 1.0 / (1.25 * g_buf_entry_sz); // an estimate for now
    }
    if (g_evict_policy_bench && g_evict_meta_bytes > 0) {
      // buffer-replacement microbenchmark: charge each policy its per-record
      // replacement metadata against the byte budget (nemo ~1 B of bits,
      // record-CLOCK 8 B ring pointer, exact LRU 16 B list pointers), so an
      // equal-byte budget holds a policy-specific number of records
      num_slots_ = g_total_buf_sz * 1.0 / (g_record_size + g_evict_meta_bytes);
      LOG_INFO("evict-policy bench: charging %zu B/record of replacement metadata "
               "-> %zu slots for %zu bytes", (size_t) g_evict_meta_bytes, num_slots_,
               (size_t) g_total_buf_sz);
    }
  }
  // evict_end_threshold_ is used for AllocTuple caused eviction only
  // on-demand eviction only evict till need
  LOG_INFO("Initiated Object Buffer with %zu slots", num_slots_);
}

ObjectBufferManager::ObjectBufferManager(ARDB *db, size_t buffer_sz) : db_(db) {
  num_slots_ = buffer_sz * 1.0 / g_buf_entry_sz; // an estimate for now
  // evict_end_threshold_ is used for AllocTuple caused eviction only
  // on-demand eviction only evict till need
  LOG_INFO("Initiated Object Buffer with %zu slots", num_slots_);
}

bool ObjectBufferManager::ReachEvictionThreshold() { 
  // return g_warmup_finished && (GetAllocated() >= static_cast<size_t>(g_evict_threshold * num_slots_)); 
  return GetAllocated() >= static_cast<size_t>(g_evict_threshold * num_slots_); 
}

void ObjectBufferManager::PeriodicEviction(ObjectBufferManager *buff_mgr, size_t thd_id) {
  arboretum::ARDB::InitRand();
  bool skip_wait = false;
  //TODO(nemo): optimize the stop parameter
  if (g_batch_eviction) {
    while (!g_terminate_evict) {
      if (!skip_wait) {
        std::this_thread::sleep_for(std::chrono::milliseconds(g_batch_eviction_period_ms)); // sleep to avoid busy waiting
      }
      skip_wait = buff_mgr->ReachEvictionThreshold();
      if (skip_wait) {
        skip_wait = !(buff_mgr->BatchTraverseEvict());
      }
    }

  } else {
    M_ASSERT(g_eviction_simple_clock, "for record clock, only simple clock(ref bit is 0 or 1) supports proactive eviction!");
    while (!g_terminate_evict) {
      if (!skip_wait) {
        std::this_thread::sleep_for(std::chrono::milliseconds(g_batch_eviction_period_ms)); // sleep to avoid busy waiting
      }
      if (buff_mgr->ReachEvictionThreshold()) {
        buff_mgr->RecordEvict();
        skip_wait = true; 
      } else {
        skip_wait = false;
      }
    }

  }

}

void ObjectBufferManager::StartEvictThds() {
  for (size_t i = 0; i < g_num_evict_thds; i++) {
    evict_thds_.emplace_back(ObjectBufferManager::PeriodicEviction, this, i);
  }
  LOG_INFO("[ObjectBufferManager] Started %d eviction threads.", g_num_evict_thds);
}

void ObjectBufferManager::StopEvictThds() {
  std::for_each(evict_thds_.begin(), evict_thds_.end(), std::mem_fn(&std::thread::join));
  LOG_INFO("[ObjectBufferManager] Joined all %d eviction threads.", evict_thds_.size());
}

bool ObjectBufferManager::BatchTraverseEvict() {
  // pick a table weighted by its row count so random-leaf sampling
  // stays approximately uniform across all tables' cached records
  auto tbl_cnt = db_->GetTableCount();
  if (tbl_cnt == 1) {
    return db_->GetTable(0)->BatchEvictionViaIndexTraverse();
  }
  // The TABLE-level tier stays on even under g_evict_dirty_last. Switching it
  // off is a trap: with the pick left to row count alone, most picks land on
  // the largest insert-heavy table (ORDER_LINE), and the clean cold rows it
  // then finds are the read working set -- evictions and remote writes per
  // transaction go UP (the evicted rows are re-read and re-admitted) while
  // the hit rate and throughput collapse. The mechanism: the insert stream
  // sets the eviction rate, and uniform victims mean the read-hot set is
  // evicted at that rate.
  //
  // The insert stream genuinely has to leave the record buffer, and leaving
  // means one remote write per row. That is the structural cost of record
  // granularity here, and it is what the page baseline avoids -- a page
  // carries on the order of a hundred ORDER_LINE rows, so the same insert
  // stream costs it one write per that many rows.
  if (g_evict_write_once_first) {
    // tier 1: while any table holds cached write-once records (blind-insert
    // slot fills nobody reads back), evict from those tables first, weighted
    // by their cached write-once footprint. Only when none are left does the
    // pick fall through to the regular weighting below (tier 2). The insert
    // stream must not evict the read-hot set.
    size_t total_wo = 0;
    for (OID i = 0; i < tbl_cnt; i++) total_wo += db_->GetTable(i)->cached_write_once_.load();
    if (total_wo > 0) {
      auto pick = (size_t) (ARDB::RandDouble() * total_wo);
      OID picked = tbl_cnt - 1;
      for (OID i = 0; i < tbl_cnt; i++) {
        auto wo = db_->GetTable(i)->cached_write_once_.load();
        if (pick < wo) { picked = i; break; }
        pick -= wo;
      }
      // persist on the write-once tables (a random leaf may hold no cold
      // write-once row); only after many misses fall through to the regular
      // pick -- falling through is what evicts the read-hot set, so tier 1
      // must be exhausted first
      for (int t = 0; t < 32; t++) {
        if (db_->GetTable(picked)->BatchEvictionViaIndexTraverse()) return true;
      }
    }
  }
  size_t total_rows = 0;
  for (OID i = 0; i < tbl_cnt; i++) {
    total_rows += db_->GetTable(i)->GetRowCnt();
  }
  if (total_rows == 0) return false;
  auto pick = (size_t) (ARDB::RandDouble() * total_rows);
  OID picked = tbl_cnt - 1;
  for (OID i = 0; i < tbl_cnt; i++) {
    auto rows = db_->GetTable(i)->GetRowCnt();
    if (pick < rows) {
      picked = i;
      break;
    }
    pick -= rows;
  }
  // if the picked table has nothing evictable, fall back to the other tables
  // once before reporting failure (the caller busy-retries on false)
  for (OID off = 0; off < tbl_cnt; off++) {
    auto tbl = (picked + off) % tbl_cnt;
    if (db_->GetTable(tbl)->BatchEvictionViaIndexTraverse()) {
      return true;
    }
  }
  return false;
}


// ---- exact-LRU mode (g_evict_lru; buffer-replacement microbenchmark) ----
// One global doubly-linked recency list: evict_hand_ = LRU end, evict_tail_ =
// MRU end. clock_next_ links toward MRU, lru_prev_ toward LRU. All list ops
// under latch_ (a global lock -- the point of the baseline is that exact
// record-level LRU serializes on every access).
void ObjectBufferManager::RingInsert(ITuple *t) {
  if (g_evict_lru) LruPushMRU(t); else ClockInsert(evict_hand_, evict_tail_, t);
}
void ObjectBufferManager::LruUnlink(ITuple *t) {
  auto prev = t->lru_prev_; auto next = t->clock_next_;
  if (prev) prev->clock_next_ = next; else evict_hand_ = next;
  if (next) next->lru_prev_ = prev; else evict_tail_ = prev;
  t->clock_next_ = nullptr; t->lru_prev_ = nullptr;
}
void ObjectBufferManager::LruPushMRU(ITuple *t) {
  t->clock_next_ = nullptr; t->lru_prev_ = evict_tail_;
  if (evict_tail_) evict_tail_->clock_next_ = t; else evict_hand_ = t;
  evict_tail_ = t;
}
void ObjectBufferManager::LruTouch(ITuple *t) {
  auto s0 = GetSystemClock();
  latch_.lock();
  g_evict_lock_wait_ns.fetch_add(GetSystemClock() - s0, std::memory_order_relaxed);
  if (t->clock_next_ || t->lru_prev_ || evict_hand_ == t) { LruUnlink(t); LruPushMRU(t); }
  g_evict_lru_moves.fetch_add(1, std::memory_order_relaxed);
  latch_.unlock();
}
void ObjectBufferManager::RecordEvictLRU() {
  auto s0 = GetSystemClock();
  latch_.lock();
  g_evict_lock_wait_ns.fetch_add(GetSystemClock() - s0, std::memory_order_relaxed);
  auto t0 = GetSystemClock();
  auto victim = evict_hand_;
  size_t steps = 0;
  // walk from the LRU end past pinned/latched entries
  while (victim && (IsPinned(victim->buf_state_) || !victim->buf_latch_.try_lock())) {
    victim = victim->clock_next_; steps++;
  }
  if (!victim) { latch_.unlock(); return; }
  LruUnlink(victim);
  allocated_--;
  batch_cv_.notify_all();
  latch_.unlock();
  g_evict_hand_steps.fetch_add(steps, std::memory_order_relaxed);
  auto table = db_->GetTable(victim->GetTableId());
  bool flush = CheckFlag(victim->buf_state_, BM_DIRTY);
  if (flush) {
    SearchKey k; victim->GetPrimaryKey(table->GetSchema(), k);
    g_data_store->Write(table->GetTopStorageId(), k, victim->GetData(), table->GetTupleDataSize());
  }
  victim->buf_latch_.unlock();
  if (!CheckFlag(victim->buf_state_, BM_NOT_IN_INDEX)) table->IndexDelete(victim);
  g_evict_records.fetch_add(1, std::memory_order_relaxed);
  g_evict_tasks.fetch_add(1, std::memory_order_relaxed);
  g_evict_task_ns.fetch_add(GetSystemClock() - t0, std::memory_order_relaxed);
}

void ObjectBufferManager::RecordEvict() {
  if (g_evict_lru) { RecordEvictLRU(); return; }
  auto s0 = GetSystemClock();
  latch_.lock();
  g_evict_lock_wait_ns.fetch_add(GetSystemClock() - s0, std::memory_order_relaxed);
  auto t0 = GetSystemClock();
  size_t hand_steps = 0;
  auto start = evict_hand_;
  auto rounds = 0;
  while(true) {
    if (!evict_hand_ || allocated_ == 0) {
      M_ASSERT(false, "Run out of buffer, increase buffer size! allocated %d", allocated_.load());
      LOG_ERROR("Run out of buffer, increase buffer size!");
    }
    auto table = db_->GetTable(evict_hand_->GetTableId());
    // if latched, skip; otherwise, try write lock.
    // alloc tuple (1) -> latch -> pin -> insert -> latch (2) evict: insert. release latch
    // latch
    if (!IsPinned(evict_hand_->buf_state_) && evict_hand_->buf_latch_.try_lock()) {
      // already held tuple latch.
        if (ClockSweep(evict_hand_)) {
          evict_hand_->buf_latch_.unlock();
          ClockAdvance(evict_hand_, evict_tail_);
          hand_steps++;
          continue;
        } 
      // try to evict the tuple
      // write to storage if dirty and load expected in the same roundtrip
      auto tbl_name = table->GetTopStorageId();
      bool flush = CheckFlag(evict_hand_->buf_state_, BM_DIRTY);
      auto flush_tuple = evict_hand_;
      auto flush_size = table->GetTupleDataSize();
      SearchKey flush_key;
      flush_tuple->GetPrimaryKey(table->GetSchema(), flush_key);
      if (flush_key.ToUInt64() != flush_tuple->GetTID()) {
        LOG_ERROR("Invalid tuple to evict (tid = %u, pkey in data = %lu)",
                  flush_tuple->GetTID(), flush_key.ToUInt64());
      }
      // add tuple to buffer and remove evict_hand_ from buffer
      ClockRemoveHand(evict_hand_, evict_tail_, allocated_);
      batch_cv_.notify_all();
      latch_.unlock();
      g_evict_hand_steps.fetch_add(hand_steps, std::memory_order_relaxed);
      g_evict_records.fetch_add(1, std::memory_order_relaxed);
      g_evict_tasks.fetch_add(1, std::memory_order_relaxed);
      g_evict_task_ns.fetch_add(GetSystemClock() - t0, std::memory_order_relaxed);
      std::string data;
      if (flush) {
        // need to flush, then flush
         // two-tree cannot reach this branch
        //  M_ASSERT(loaded_data, "Two-tree shouldn't reach this code");
         M_ASSERT(g_index_type != TWOTREE, "Two-tree shouldn't reach this code");
         g_data_store->Write(tbl_name, flush_key, flush_tuple->GetData(), flush_size);
        flush_tuple->buf_latch_.unlock();
        // LOG_DEBUG("Thread-%u Flushed tuple %lu and load tuple %lu", Worker::GetThdId(), flush_key.ToUInt64(), load_key.ToUInt64());
        if (!CheckFlag(flush_tuple->buf_state_, BM_NOT_IN_INDEX))
          table->IndexDelete(flush_tuple); // will unlock and free
      } else {
        flush_tuple->buf_latch_.unlock();
        // LOG_DEBUG("Evicted tuple %lu", flush_key.ToUInt64());
        if (!CheckFlag(flush_tuple->buf_state_, BM_NOT_IN_INDEX))
          table->IndexDelete(flush_tuple); // will unlock and free
      }
      return;
    } else {
       ClockAdvance(evict_hand_, evict_tail_);
       hand_steps++;
       if (evict_hand_ == start) {
         rounds++;
         if (rounds >= BM_MAX_USAGE_COUNT) {
           LOG_ERROR("All pinned! Make sure # buffer slots > # threads * # reqs per txn");
           std::cout.flush();
         }
       }
    }
  }
}


bool ObjectBufferManager::WaitAvailableSlots() { 
  if (g_num_evict_thds == 0) {
    return true; // no eviction thread, no need to wait for capacity management
  }
  auto starttime = GetSystemClock();
  unique_lock<mutex> lock(batch_latch_);
  batch_cv_.wait(lock, [this] { return (allocated_ + (g_evict_policy_bench ? 0 : other_allocated_ / g_buf_entry_sz)) <= num_slots_; });  // Wait until capacity drops
  if (g_warmup_finished) {
    auto latency = GetSystemClock() - starttime;
    g_stats->db_stats_[Worker::GetThdId()].incr_rw_await_evict_time_(latency);
  }
  return true;
};

ITuple *ObjectBufferManager::AccessTuple(SharedPtr &ptr, uint64_t expected_key) {
  // called after lock permission is granted.
  auto tuple = reinterpret_cast<ITuple *>(ptr.Get());
  if (!tuple) {
    LOG_ERROR("Error accessing tuple: invalid addr");
  }
  if ((g_phantom_protection_type == PhantomProtectionType::NO_PRO) && tuple->GetTID() != expected_key) {
    LOG_ERROR("Error accessing tuple: key (%u) does not match expectation (%lu)",
              tuple->GetTID(), expected_key);
  }
  if (g_batch_eviction) {
    ObjectBufferManager::TupleAccess(tuple);
  } else {
    tuple->buf_latch_.lock();
    // HYBRBUF: the top tree's background eviction (BatchTraverseEvict ->
    // EvictRandomLeafNode -> ClockSweep) spares only tuples with the ref
    // bit set, so top-tree hits must set it even though the config runs
    // with batch=false/simple_clock=false. Without this, warmup-fabricated
    // hot records look cold to every eviction pass and the hot set erodes
    // (visible as a high share of steady-state misses on keys that were
    // resident at run start).
    if (g_eviction_simple_clock || g_buf_type == HYBRBUF) {
      ObjectBufferManager::TupleAccess(tuple);
    }
    Pin(tuple);
    tuple->buf_latch_.unlock();
    if (g_evict_lru) ((ObjectBufferManager *) g_buf_mgr)->LruTouch(tuple); // exact LRU: move to MRU on every hit
  }
  return tuple;
}

void ObjectBufferManager::FinishAccessTuple(SharedPtr &ptr, bool dirty) {
  auto tuple = reinterpret_cast<ITuple *>(ptr.Get());
  FinishAccessTuple(tuple, dirty);
  ptr.Free();
}

void ObjectBufferManager::FinishAccessTuple(ITuple * tuple, bool dirty) {
  if (dirty && g_generative_warmup && !g_warmup_finished)
    NoteDirtied(tuple->GetTableId(), tuple->GetTID());
  if (g_batch_eviction) {
    assert(tuple);
    if (dirty) {
      tuple->buf_state_ = SetFlag(tuple->buf_state_, BM_DIRTY);
    }
  } else {
    assert(tuple);
    tuple->buf_latch_.lock();
    UnPin(tuple);
    if (dirty) {
      tuple->buf_state_ = SetFlag(tuple->buf_state_, BM_DIRTY);
      // giving writes more weights
      if (GetUsageCount(tuple->buf_state_) < BM_MAX_USAGE_COUNT)
        tuple->buf_state_ += BUF_USAGECOUNT_ONE;
    }
    tuple->buf_latch_.unlock();
  }
}

ITuple* ObjectBufferManager::AllocTuple(size_t tuple_sz, OID tbl_id, OID tid) {
  auto tuple = new (MemoryAllocator::Alloc(tuple_sz)) ITuple(tbl_id, tid);
  if (g_batch_eviction || g_eviction_simple_clock) {
    ReserveSlots(1);
    // optimize this Spinning
    // while (!HasAvailableSlots()) {
    // }
    WaitAvailableSlots();
    if (g_batch_eviction) {
      ObjectBufferManager::TupleAccess(tuple);
    } else {
      latch_.lock();
      Pin(tuple);
      ObjectBufferManager::TupleAccess(tuple);
      RingInsert( tuple);
      latch_.unlock();
    }
  } else {
    // can be not accurate since it is not atomic and the buf_sz doesn't include the added space (1 tuple)
    // auto buf_sz = allocated_.fetch_add(1) + other_allocated_ / g_buf_entry_sz;
    ReserveSlots(1);
    // if (buf_sz > num_slots_) {
    if (!HasAvailableSlots()) {
      SearchKey dummy;
      Pin(tuple);
      EvictAndLoad(dummy, tuple, false);
    } else {
      latch_.lock();
      Pin(tuple);
      ObjectBufferManager::TupleAccess(tuple);
      RingInsert( tuple);
      latch_.unlock();
    }
  }
  return tuple;
}

void ObjectBufferManager::AdmitTuple(ITuple* tuple) {
  if (g_batch_eviction || g_eviction_simple_clock) {
    ReserveSlots(1);
    WaitAvailableSlots();
    if (g_batch_eviction) {
      ObjectBufferManager::TupleAccess(tuple);
    } else {
      latch_.lock();
      Pin(tuple);
      ObjectBufferManager::TupleAccess(tuple);
      RingInsert( tuple);
      latch_.unlock();
    }
  } else {
    // can be not accurate since it is not atomic and the buf_sz doesn't include the added space (1 tuple)
    // auto buf_sz = allocated_.fetch_add(1) + other_allocated_ / g_buf_entry_sz;
    ReserveSlots(1);
    // if (buf_sz > num_slots_) {
    if (!HasAvailableSlots()) {
      SearchKey dummy;
      Pin(tuple);
      EvictAndLoad(dummy, tuple, false);
    } else {
      latch_.lock();
      Pin(tuple);
      ObjectBufferManager::TupleAccess(tuple);
      RingInsert( tuple);
      latch_.unlock();
    }
  }
}




//TODO: support range query for two-tree
ITuple **ObjectBufferManager::LoadRangeFromStorage(OID tbl_id,
                                                   SearchKey low_key,
                                                   SearchKey high_key, size_t& cnt) {
  M_ASSERT(high_key.ToUInt64() - low_key.ToUInt64() <= g_scan_length + 1, "Invalid range query (too large): low_key %lu, high_key %lu",
           low_key.ToUInt64(), high_key.ToUInt64());
  auto table = db_->GetTable(tbl_id);
  auto tuple_sz = table->GetTotalTupleSize(); 
  if (g_batch_eviction) {
    // TODO: support range query for two-tree
    M_ASSERT(g_buf_type != HYBRBUF, "HYBRBUF is an unexpected buffer type type for LoadRangeFromStorage()");

    char* addrs = nullptr;
    g_data_store->ReadRange(table->GetStorageId(), low_key, high_key, &addrs, table->GetTotalTupleSize(), cnt);
    // ReserveSlots(cnt); 
    auto tuples = (ITuple **) MemoryAllocator::Alloc(sizeof(void *) * cnt);
    size_t next = 0;
    ITuple * first_tuple;
    for (int i = 0; i < cnt; i++) {
      // initially assign a fake TID to ITuple
      auto loaded_tuple = new (&addrs[next]) ITuple(tbl_id, low_key.ToUInt64() + i);
      if (i == 0) {
        first_tuple = loaded_tuple;
      }
      auto tuple = new (MemoryAllocator::Alloc(tuple_sz)) ITuple(
        tbl_id, low_key.ToUInt64() + i);
      tuple->SetFromLoaded(loaded_tuple->GetData(), tuple_sz - sizeof(ITuple));
      tuples[i] = tuple;

      next += tuple_sz;
      SearchKey key;
      // no need to set from loaded since memcpy is done in azure client.
      // tuple->SetFromLoaded(const_cast<char *>(data.c_str()), GetTupleDataSize());
      tuple->GetPrimaryKey(table->GetSchema(), key);
      // set actual TID to ITuple
      tuple->SetTID(key.ToUInt64());
      // set ref bit to 1
      TupleAccess(tuple);
      if (!(low_key.ToUInt64() <= key.ToUInt64() && key.ToUInt64() <= high_key.ToUInt64())) {
        M_ASSERT(false, "Loaded data does not match: low_key: %ld, high_key: %ld, index: %d, cur_key: %ld",
                 low_key.ToUInt64(), high_key.ToUInt64(), i, key.ToUInt64());
      } 
      // } else {
      //   LOG_INFO("Loaded data matches: low_key: %ld, high_key: %ld, index: %d, cur_key: %ld", low_key.ToUInt64(),
      //            high_key.ToUInt64(), i, key.ToUInt64());
      //   std::cout.flush();
      // }
    }
    // WaitAvailableSlots();
    if (cnt > 0) {
      DEALLOC(first_tuple);
    }
    return tuples;
    // g_data_store->ReadRange(table->GetStorageId(), low_key, high_key, data_ptrs, &result_sz);
  } else {
    M_ASSERT(false, "TODO: support new semantic of g_data_store->ReadRange");
    size_t result_sz = 0;
    auto cnt = high_key.ToUInt64() - low_key.ToUInt64() + 1;
    auto table = db_->GetTable(tbl_id);
    auto tuple_sz = table->GetTotalTupleSize();
    auto tuples = (ITuple **) MemoryAllocator::Alloc(sizeof(void *) * cnt);
    char ** data_ptrs = (char **) MemoryAllocator::Alloc(sizeof(void *) * cnt);
  
    auto buf_sz = allocated_.fetch_add(cnt) + other_allocated_ / g_buf_entry_sz;
    volatile bool is_done = true;
    if (buf_sz > num_slots_) {
      // evict a batch.
      BatchEvict(buf_sz - num_slots_, is_done);
    }
    // batch load data
    for (int i = 0; i < cnt; i++) {
      tuples[i] = new (MemoryAllocator::Alloc(tuple_sz)) ITuple(
          tbl_id, low_key.ToUInt64() + i);
      data_ptrs[i] = tuples[i]->GetData();
      SetUsageCount(tuples[i], 2);
      TupleAccess(tuples[i]);
    }
    //TODO: support range query for two-tree
    M_ASSERT(g_buf_type != HYBRBUF, "HYBRBUF is an unexpected buffer type type for LoadRangeFromStorage()");
    M_ASSERT(false, "TODO: support new semantic of g_data_store->ReadRange, uncomment the code below");
    // g_data_store->ReadRange(table->GetStorageId(), low_key, high_key, data_ptrs, &result_sz);
    // insert data into the buffer ring
    latch_.lock();
    for (int i = 0; i < cnt; i++) {
      RingInsert( tuples[i]);
    }
    latch_.unlock();
    // block until all writes are flushed. (not necessary but limit rate)
    while(!is_done) {};
    return tuples;
  }
}


// if the key is not found:  
//      if g_negative_search_op_enable is true, tombstone will be tracked by cache management and  returned
//      Otherwise, tombstone won't be tracked by cache management and directly returned
ITuple *ObjectBufferManager::LoadFromStorage(OID tbl_id, SearchKey key, char * loaded_data, size_t sz) {
  // will delete tuple from index if evicted.
  auto table = db_->GetTable(tbl_id);
  auto tuple_sz = table->GetTotalTupleSize();
  auto tuple = new (MemoryAllocator::Alloc(tuple_sz)) ITuple(tbl_id, key.ToUInt64());
  if (g_batch_eviction || g_eviction_simple_clock) {
    // load data
    std::string data;
    if (loaded_data) {
      // asynchrounous eviction
      ReserveSlots(1);
      tuple->SetFromLoaded(loaded_data, sz);
    } else {
      M_ASSERT(g_buf_type != HYBRBUF,
               "HYBRBUF is an unexpected buffer type type for this branch of LoadFromStorage()");
      auto rc = g_data_store->Read(table->GetStorageId(), key, data);
      if (rc == RC::NOT_FOUND) {
        tuple->SetTombstone();
        if (g_negative_search_op_enable) {
          ReserveSlots(1);
        } else {
          return tuple;
        } 
      } else if (rc == RC::OK) {
        ReserveSlots(1);
        // don't copy everything, only copy things you need
        tuple->SetFromLoaded(const_cast<char*>(data.c_str()), table->GetTupleDataSize());
      } else {
        M_ASSERT(false, "unexpected return code from data store for LoadFromStorage()");
      }

    }

    if (g_batch_eviction) {
      ObjectBufferManager::TupleAccess(tuple);
      WaitAvailableSlots();
    } else {
      // wait BEFORE taking latch_ (AdmitTuple's order): RecordEvict needs
      // latch_ to free slots, so waiting while holding it deadlocks the
      // moment a miss hits a full buffer (evict-at-full made this reachable;
      // confirmed by stack dump: 1 holder in WaitAvailableSlots, 5 evictors
      // and 15 workers queued on latch_)
      WaitAvailableSlots();
      latch_.lock();
      ObjectBufferManager::TupleAccess(tuple);
      RingInsert( tuple);
      latch_.unlock();
    }
  } else {
    // auto buf_sz = allocated_.fetch_add(1) + other_allocated_ / g_buf_entry_sz;
    ReserveSlots(1);
    // if (buf_sz > num_slots_) {
    if (!HasAvailableSlots()) {
      EvictAndLoad(key, tuple, true, loaded_data, sz);
    } else {
      std::string data;
      if (loaded_data) {
        tuple->SetFromLoaded(loaded_data, sz);
      } else {
        M_ASSERT(g_buf_type != HYBRBUF,
                 "HYBRBUF is an unexpected buffer type type for this branch of LoadFromStorage()");
        g_data_store->Read(table->GetStorageId(), key, data);
        // don't copy everything, only copy things you need
        tuple->SetFromLoaded(const_cast<char*>(data.c_str()), table->GetTupleDataSize());
      }
      // no need to hold individual latch since no one can access it now
      SetUsageCount(tuple, 2);
      TupleAccess(tuple);
      // add to buffer
      latch_.lock();
      RingInsert( tuple);
      latch_.unlock();
      // LOG_DEBUG("Load tuple %s from storage", storage_key.c_str());
    }
  }

  if (!tuple->IsTombstone()) {
    SearchKey pkey;
    tuple->GetPrimaryKey(table->GetSchema(), pkey);
    M_ASSERT(pkey == key, "tid does not match");
  }
  return tuple;
}

void ObjectBufferManager::BatchEvict(size_t cnt, volatile bool &is_done) {
  // partition to map
  size_t flushed = 0;
  std::unordered_map<uint64_t, std::multimap<std::string, std::string>> flush_set;
  latch_.lock();
  auto start = evict_hand_;
  auto rounds = 0;
  std::string tbl_name;
  while (true) {
    if (!evict_hand_ || allocated_ == 0) {
      LOG_ERROR("Run out of buffer, increase buffer size!");
    }
    auto table = db_->GetTable(evict_hand_->GetTableId());
    // if latched, skip; otherwise, try write lock.
    if (!IsPinned(evict_hand_->buf_state_) && evict_hand_->buf_latch_.try_lock()) {
      // already held tuple latch.
      if (g_eviction_simple_clock) {
        if (ClockSweep(evict_hand_)) {
          evict_hand_->buf_latch_.unlock();
          ClockAdvance(evict_hand_, evict_tail_);
          continue;
        } 
      } else {
        if (GetUsageCount(evict_hand_->buf_state_) != 0) {
          evict_hand_->buf_state_ -= BUF_USAGECOUNT_ONE;
          evict_hand_->buf_latch_.unlock();
          ClockAdvance(evict_hand_, evict_tail_);
          continue;
        }
      }
      // try to evict the tuple
      // write to storage if dirty and load expected in the same roundtrip
      M_ASSERT(g_buf_type != HYBRBUF, "HYBRBUF is an unexpected buffer type type for BatchEvict()");
      tbl_name = table->GetStorageId();
      auto flush_tuple = evict_hand_;
      if (CheckFlag(flush_tuple->buf_state_, BM_DIRTY)) {
        SearchKey flush_key;
        flush_tuple->GetPrimaryKey(table->GetSchema(), flush_key);
        std::string flush_data(flush_tuple->GetData(), table->GetTupleDataSize());
        flush_set[flush_key.ToUInt64() / g_partition_sz].emplace(
            flush_key.ToString(), flush_data);
      }
      flush_tuple->buf_latch_.unlock();
      // remove evict_hand_ from buffer
      ClockRemoveHand(evict_hand_, evict_tail_, allocated_);
      if (++flushed == cnt) break;
    }
    else {
      ClockAdvance(evict_hand_, evict_tail_);
      if (evict_hand_ == start) {
        rounds++;
        if (rounds >= BM_MAX_USAGE_COUNT) {
          LOG_ERROR("All pinned! Make sure # buffer slots > # threads * # reqs per txn");
        }
      }
    }
  }
  latch_.unlock();
  if (flushed != 0) {
    int64_t num_parts = flush_set.size();
    for (auto &pair : flush_set) {
      if (--num_parts == 0) {
        g_data_store->WriteBatchAsync(tbl_name, std::to_string(pair.first),
                                      pair.second, is_done);
      } else {
        g_data_store->WriteBatch(tbl_name, std::to_string(pair.first), pair.second);
      }
    }
  }
}

//TODO: no longer used for background evction thread (proactive eviction)
void ObjectBufferManager::EvictAndLoad(SearchKey &load_key,
                                       ITuple *tuple, bool load, char * loaded_data, size_t sz) {
  
  latch_.lock();
  auto start = evict_hand_;
  auto rounds = 0;
  while(true) {
    if (!evict_hand_ || allocated_ == 0) {
      M_ASSERT(false, "Run out of buffer, increase buffer size! allocated %d", allocated_.load());
      LOG_ERROR("Run out of buffer, increase buffer size!");
    }
    auto table = db_->GetTable(evict_hand_->GetTableId());
    // if latched, skip; otherwise, try write lock.
    // alloc tuple (1) -> latch -> pin -> insert -> latch (2) evict: insert. release latch
    // latch
    if (!IsPinned(evict_hand_->buf_state_) && evict_hand_->buf_latch_.try_lock()) {
      // already held tuple latch.
      if (g_eviction_simple_clock) {
        if (ClockSweep(evict_hand_)) {
          evict_hand_->buf_latch_.unlock();
          ClockAdvance(evict_hand_, evict_tail_);
          continue;
        } 
      } else {
        if (GetUsageCount(evict_hand_->buf_state_) != 0) {
          evict_hand_->buf_state_ -= BUF_USAGECOUNT_ONE;
          evict_hand_->buf_latch_.unlock();
          ClockAdvance(evict_hand_, evict_tail_);
          continue;
        }
      }


      // try to evict the tuple
      // write to storage if dirty and load expected in the same roundtrip
      auto tbl_name = table->GetTopStorageId();
      bool flush = CheckFlag(evict_hand_->buf_state_, BM_DIRTY);
      auto flush_tuple = evict_hand_;
      auto flush_size = table->GetTupleDataSize();
      SearchKey flush_key;
      flush_tuple->GetPrimaryKey(table->GetSchema(), flush_key);
      if (flush_key.ToUInt64() != flush_tuple->GetTID()) {
        LOG_ERROR("Invalid tuple to evict (tid = %u, pkey in data = %lu)",
                  flush_tuple->GetTID(), flush_key.ToUInt64());
      }
      // add tuple to buffer and remove evict_hand_ from buffer
      ClockRemoveHand(evict_hand_, evict_tail_, allocated_);
      SetUsageCount(tuple, 2);
      if (g_eviction_simple_clock) {
        ObjectBufferManager::TupleAccess(tuple);
      }
      // when load == true this function fills the incoming tuple's data only
      // after the victim's (possibly remote, ms-scale) flush below; pin it
      // across that window so a concurrent evictor cannot pick a tuple whose
      // data is not loaded yet (load == false callers pin before calling)
      if (load) {
        Pin(tuple);
      }
      RingInsert( tuple);
      latch_.unlock();
      std::string data;
      if (flush) {
        // need to flush, then flush and load
        if (load) {
          if (loaded_data) {
            // two-tree: flush to the bottom tree
            table->TwoTreeEviction(flush_key, flush_tuple->GetData(), flush_size);
            // g_data_store->Write(tbl_name, flush_key, flush_tuple->GetData(), flush_size);
            // tuple->SetFromLoaded(const_cast<char *>(data.c_str()), table->GetTupleDataSize());
            tuple->SetFromLoaded(loaded_data, sz);
          } else {
            g_data_store->WriteAndRead(tbl_name, flush_key,
                                     flush_tuple->GetData(), flush_size,
                                     load_key, data);
            tuple->SetFromLoaded(const_cast<char *>(data.c_str()), table->GetTupleDataSize());
            LOG_INFO("XXX loading from remote storage");
          }
          UnPin(tuple);
        } else {
          // two-tree cannot reach this branch
          M_ASSERT(loaded_data, "Two-tree shouldn't reach this code");
          M_ASSERT(g_index_type != TWOTREE, "Two-tree shouldn't reach this code");
          g_data_store->Write(tbl_name, flush_key, flush_tuple->GetData(), flush_size);
        }
        flush_tuple->buf_latch_.unlock();
        // LOG_DEBUG("Thread-%u Flushed tuple %lu and load tuple %lu", Worker::GetThdId(), flush_key.ToUInt64(), load_key.ToUInt64());
        if (!CheckFlag(flush_tuple->buf_state_, BM_NOT_IN_INDEX))
          table->IndexDelete(flush_tuple); // will unlock and free
      }
      else if (load) {
        // no need to flush, just load
        flush_tuple->buf_latch_.unlock();
        if (loaded_data) {
            // two-tree: read from top tree
            tuple->SetFromLoaded(loaded_data, sz);
        } else {
            M_ASSERT(g_index_type != TWOTREE, "Two-tree shouldn't reach this code");
            g_data_store->Read(tbl_name, load_key, data);
            // TODO: think about when to pin the tuple and
            //  why loading from storage does not pin but allicating pins the page
            tuple->SetFromLoaded(const_cast<char *>(data.c_str()), table->GetTupleDataSize());
            SearchKey pkey;
            tuple->GetPrimaryKey(table->GetSchema(), pkey);
            // LOG_DEBUG("Thread-%u Load tuple %lu", Worker::GetThdId(), load_key.ToUInt64());
        }
        UnPin(tuple);
        if (!CheckFlag(flush_tuple->buf_state_, BM_NOT_IN_INDEX)) {
          table->IndexDelete(flush_tuple); // will unlock and free
        }

      } else {
        flush_tuple->buf_latch_.unlock();
        // LOG_DEBUG("Evicted tuple %lu", flush_key.ToUInt64());
        if (!CheckFlag(flush_tuple->buf_state_, BM_NOT_IN_INDEX))
          table->IndexDelete(flush_tuple); // will unlock and free
      }
      return;
    } else {
       ClockAdvance(evict_hand_, evict_tail_);
       if (evict_hand_ == start) {
         rounds++;
         if (rounds >= BM_MAX_USAGE_COUNT) {
           LOG_ERROR("All pinned! Make sure # buffer slots > # threads * # reqs per txn");
         }
       }
    }
  }
}

void ObjectBufferManager::SetUsageCount(ITuple *row, size_t cnt) {
  // cleaned buf_state_
  auto usage_cnt = GetUsageCount(row->buf_state_);
  row->buf_state_ += (usage_cnt + cnt > BM_MAX_USAGE_COUNT) ?
                     (BM_MAX_USAGE_COUNT - usage_cnt) * BUF_USAGECOUNT_ONE :
                     BUF_USAGECOUNT_ONE * cnt;
}

void ObjectBufferManager::Pin(ITuple *row) {
  // sanity bound against pin leaks; legitimate concurrency reaches
  // num_workers x num_req_per_query pins on one hot tuple (e.g. 16 x 16
  // under zipf with uniform storage latencies), so x10 was too tight.
  // The refcount field is 18 bits, so x1000 stays far from overflow.
  if (GetPinCount(row->buf_state_) >= g_num_worker_threads * 1000) {
    LOG_ERROR("pin count overflow! tuple %u pin count %u", row->GetTID(),
              GetPinCount(row->buf_state_));
  }
  row->buf_state_ += BUF_REFCOUNT_ONE;
  // update usage count (weight)
  if (GetUsageCount(row->buf_state_) < BM_MAX_USAGE_COUNT) {
    row->buf_state_ += BUF_USAGECOUNT_ONE;
  }
}

void ObjectBufferManager::UnPin(ITuple *row) {
  M_ASSERT(GetPinCount(row->buf_state_) > 0,
           "unpin on pin count 0 (underflow)! tuple %u", row->GetTID());
  row->buf_state_ -= BUF_REFCOUNT_ONE;
}

} // arboretum