#include "ITxn.h"
#include "CCManager.h"
#include "db/access/IndexBTreeObject.h"
#include "remote/IDataStore.h"
#include "remote/ILogStore.h"
#include "buffer/ObjectBufferManager.h"
#include "buffer/PageBufferManager.h"
#include "common/Worker.h"
#include "common/Stats.h"
#include <unordered_set>
#include <unordered_map>

namespace arboretum {
RC ITxn::InsertTuple(SearchKey& pkey, ITable * p_table, char * data, size_t sz) {
  auto starttime = GetSystemClock();
  auto rc = p_table->InsertTuple(pkey, data, sz, this);
  if (g_warmup_finished) {
    auto endtime = GetSystemClock();
    g_stats->db_stats_[Worker::GetThdId()].incr_idx_time_(endtime - starttime);
  }

  M_ASSERT(false, "add AccessTuple to acquire write locks")
  // return AccessTuple(result, p_table, key.ToUInt64(), AccessType::UPDATE, data, sz);
  return rc;
}

RC ITxn::GetTuples(ITable *p_table, OID idx_id, SearchKey low_key, SearchKey
high_key) {
  RC rc = RC::OK;
  auto starttime = GetSystemClock();
  auto ac_begin = tuple_accesses_.size();
  if (g_scan_materialize) {
    scan_rows_.clear();
    // release any prior scan result the caller did not consume
    ReleaseScanArray();
  }
  // try to *read* the index to get tuple address
  size_t cnt = 0;
  auto result = p_table->IndexRangeSearch(low_key, high_key, idx_id,
                                          this, rc, cnt);
  // OBJBUF gap-bit scans publish a slot-per-key array on scan_array (even
  // when they abort midway); record its size so ReleaseScanArray can free the
  // refs without caller-side arithmetic
  if (g_scan_materialize && scan_array != nullptr) {
    scan_array_sz_ = high_key.ToUInt64() - low_key.ToUInt64() + 1;
  }

  // abort on index accessing conflicts.
  if (rc == RC::ABORT) {
    LOG_INFO("XXX GetTuples abort");
    return rc;
  }
  if (g_warmup_finished) {
    auto endtime = GetSystemClock();
    g_stats->db_stats_[Worker::GetThdId()].incr_idx_time_(endtime - starttime);
  }
  if (g_buf_type == PGBUF) {
  auto starttime = GetSystemClock();

  std::unordered_set<OID> pages;
  bool is_miss = false;

  for (size_t i = 0; i < cnt; i++) {
      char * data;
      size_t sz;

      auto item_tag = result[i].ReadAsPageTag();
      pages.insert(item_tag.pg_id_);
      rc = AccessTuple(&result[i], p_table, low_key.ToUInt64() + i,
                       SCAN, data, sz, false, &is_miss);
      if (rc == RC::ABORT) return rc;
      if (g_scan_materialize) {
        scan_rows_.emplace_back(low_key.ToUInt64() + i, data);
      }
  }

  auto endtime = GetSystemClock();

  } else if (g_buf_type == HYBRBUF) {
    // two-tree scan merge. Coverage: the bottom page tree returned a tag per
    // key in range (all TPC-C keys are pre-slotted). Freshness: records
    // currently DIRTY in the top tree have stale bottom copies (top eviction
    // flushes through on its own schedule), so they must overlay the bottom
    // bytes. Order matters: the per-table lock domain is shared by both
    // trees, so FIRST pin+lock every bottom row -- from then on no writer
    // can commit into the range, and any writer that committed earlier has
    // already set BM_DIRTY before releasing its lock -- THEN peek the top
    // tree's dirty set and override. Clean top records equal the bottom
    // bytes by the flush-through invariant, so dirty-only overlay suffices.
    auto expected_cnt = high_key.ToUInt64() - low_key.ToUInt64() + 1;
    if (cnt != expected_cnt) {
      // pre-slotted datasets guarantee full coverage: treat a hole like the
      // point path's bottom-miss fallback -- rate-capped log, any nonzero
      // count invalidates the run
      static std::atomic<uint64_t> hole_cnt{0};
      auto n = hole_cnt.fetch_add(1);
      if (n < 5 || (n & (n - 1)) == 0) {
        LOG_INFO("2tree scan bottom hole #%lu: [%lu, %lu] returned %lu rows",
                 n + 1, low_key.ToUInt64(), high_key.ToUInt64(), cnt);
      }
    }
    for (size_t i = 0; i < cnt; i++) {
      char *bt_data;
      rc = AccessBottomTuple(&result[i], p_table, low_key.ToUInt64() + i,
                             bt_data);
      if (rc == RC::ABORT) {
        // hand the tag array to the already-registered accesses for cleanup
        // (the PGBUF path leaks it on abort; no reason to copy that)
        if (tuple_accesses_.size() > ac_begin)
          tuple_accesses_[ac_begin].free_ptr = true;
        else if (result)
          DEALLOC(result);
        return rc;
      }
      if (g_scan_materialize)
        scan_rows_.emplace_back(low_key.ToUInt64() + i, bt_data);
    }
    // freshness overlay: peek the top tree only after every range lock is
    // held (see the ordering argument above)
    auto overlay = NEW_SZ(SharedPtr, expected_cnt);
    auto top_index = (IndexBTreeObject *) p_table->GetTopIdx();
    auto dirty_cnt = top_index->PeekDirtyRange(low_key, high_key, overlay);
    if (dirty_cnt > 0) {
      // diagnostic (rate-capped like the bottom-miss fallback): proof the
      // freshness path fires -- scans overlaying stale bottom copies with
      // top-dirty records. Zero occurrences across a write-heavy run would
      // mean scans silently read stale bottom bytes.
      static std::atomic<uint64_t> overlay_cnt{0};
      auto n = overlay_cnt.fetch_add(1);
      if (n < 5 || (n & (n - 1)) == 0) {
        LOG_INFO("2tree scan overlay #%lu: %zu top-dirty rows in [%lu, %lu]",
                 n + 1, dirty_cnt, low_key.ToUInt64(), high_key.ToUInt64());
      }
    }
    if (dirty_cnt > 0 && g_scan_materialize) {
      for (size_t i = 0; i < expected_cnt; i++) {
        auto tuple = reinterpret_cast<ITuple *>(overlay[i].Get());
        if (!tuple) continue;
        auto key = low_key.ToUInt64() + i;
        if (i < scan_rows_.size() && scan_rows_[i].first == key) {
          scan_rows_[i].second = tuple->GetData();
        } else {
          // a bottom hole covered by a cached fresh record
          scan_rows_.emplace_back(key, tuple->GetData());
        }
      }
    }
    // the overlay refs (and the empty slots' counters) are released by the
    // caller's ReleaseScanArray once the rows are consumed
    scan_array = overlay;
    scan_array_sz_ = expected_cnt;
  } else if (g_scan_materialize && result && scan_array == result) {
    // OBJBUF gap-bit path: expose materialized rows (cached + fetched) through
    // the uniform interface; memory is held by the scan_array refs until
    // ReleaseScanArray
    for (size_t i = 0; i < scan_array_sz_; i++) {
      auto tuple = reinterpret_cast<ITuple *>(result[i].Get());
      if (tuple) {
        scan_rows_.emplace_back(low_key.ToUInt64() + i, tuple->GetData());
      }
    }
  } else if (g_scan_materialize) {
    // record-cache / remote paths return a compact array and register the rows
    // as tuple accesses; expose those instead of walking per-key slots
    for (size_t i = ac_begin; i < tuple_accesses_.size(); i++) {
      auto tuple = reinterpret_cast<ITuple *>(tuple_accesses_[i].rows_[0].Get());
      if (tuple) {
        scan_rows_.emplace_back(tuple->GetTID(), tuple->GetData());
      }
    }
  }
  if (tuple_accesses_.size() > ac_begin) {
    // set special free_ptr on the first access added by THIS scan (the txn may
    // have point-read accesses before it); used to release the scan's
    // contiguous buffer / shared-ptr array at commit or abort
    tuple_accesses_[ac_begin].free_ptr = true;
  }
  // if (result) {
  //   DEALLOC(result);
  // }

  return rc;
}

RC ITxn::GetTuple(ITable *p_table, OID idx_id, SearchKey key, AccessType ac_type,
                  char *&data, size_t &sz, bool blind) {
  // convention: idx_id 0 means the table's primary index (real ids start at 1)
  if (idx_id == 0) idx_id = p_table->GetPrimaryIdxId();
  M_ASSERT(idx_id == p_table->GetPrimaryIdxId(),
           "expect table's primary index %d, current is idx_id %d",
           p_table->GetPrimaryIdxId(), idx_id);
  RC rc;
  
  // acquire intention lock
  if (g_enable_partition_covering_lock && ac_type == AccessType::UPDATE) {
      auto pid = GetPartitionID(key.ToUInt64());
      auto lock = p_table->GetIntentionLockStatePtr(pid);
      // auto wait_state = p_table->IxLockWaitStatePtr(pid);
      ILockType lock_type = ILockType::IX;
      uint64_t cc_starttime = GetSystemClock(); 
      // rc = CCManager::AcquireIntentionLock(*lock, lock_type, *wait_state);
      rc = CCManager::AcquirePhantomIntentionLock(*lock, true);
      if (g_warmup_finished) {
        uint64_t cctime =  GetSystemClock() - cc_starttime;
        AddCCTime(cctime);
        LOG_XXX("XXX CC time for target key %ld in partition %d: %ld us", key.ToUInt64(), pid, cctime);
       } 
      if (rc == ABORT) {
          // release previously held index locks
        LOG_XXX("XXX Failed to Acq IX lock on pid %d for key %ld", pid, key.ToUInt64());
        return rc;
      }
      LOG_XXX("XXX Success to Acq IX lock on pid %d for key %ld", pid, key.ToUInt64());
      IntentionLockAccess iac = {pid, lock_type, lock};
      intention_locks_.push_back(iac);
  }

  // try to *read* the index to get tuple address
  auto starttime = GetSystemClock();
  auto result = p_table->IndexSearch(key, ac_type, this, rc, g_index_type,
                                     g_buf_type, blind);

  // abort on index accessing conflicts.
  if (rc == RC::ABORT) return rc;
  if (g_warmup_finished) {
    auto endtime = GetSystemClock();
    g_stats->db_stats_[Worker::GetThdId()].incr_idx_time_(endtime - starttime);
  }
  if (result == nullptr) {
    return RC::OK;
  }
  return AccessTuple(result, p_table, key.ToUInt64(), ac_type, data, sz);
}



//TODO: support two-tree
RC ITxn::AccessTuple(SharedPtr *result, ITable * p_table, uint64_t expected_key,
                     AccessType ac_type, char *&data, size_t &sz, bool free_ptr, bool *is_miss) {
    // try to record the tuple access
  if (!result->Get()) {
    M_ASSERT(false, "null ptr for access tuple");
  }
  // access the tuple object (not the data)
  ITuple * tuple;
  if (g_warmup_finished) g_stats->db_stats_[Worker::GetThdId()].incr_tuple_accesses_(1);
  if (g_buf_type == OBJBUF || g_buf_type == HYBRBUF) {
    // pin the tuple
    tuple = arboretum::ObjectBufferManager::AccessTuple(*result, expected_key);
  } else if (g_buf_type == PGBUF) {
    // pin the page when accessing the tuple.
    // if (*result->Get().IsTombstone()) {
    //   tuple = reinterpret_cast<ITuple *>(result->Get());
    // } else {
      tuple = ((PageBufferManager *) g_buf_mgr)->AccessTuple(p_table, *result, expected_key, is_miss);
    // }
  } else {
    tuple = reinterpret_cast<ITuple *>(result->Get());
    g_stats->db_stats_[Worker::GetThdId()].incr_accesses_(1);
    g_stats->db_stats_[Worker::GetThdId()].incr_misses_(1);
  }
  // auto need_lock = !(ac_type == SCAN && g_enable_partition_covering_lock);
  auto need_lock = !(ac_type == SCAN && g_enable_partition_covering_lock);
  // if tombstone, generage SearchKey key through TID, otherwise through GetPrimaryKey
  // auto key should be defined before if/else block
  SearchKey key;
  if (tuple->IsTombstone() || tuple->IsPhantom()) {
    key = SearchKey(tuple->GetTID());
  } else {
    key = tuple->GetPrimaryKey(p_table->GetSchema());
  }
  // auto key = tuple->GetPrimaryKey(p_table->GetSchema());
  // M_ASSERT(key.ToUInt64() == expected_key, "key does not match, cur key is %ld, expected key is %ld", key.ToUInt64(), expected_key);
  RC rc = RC::OK;
  if (need_lock) {
     // try to acquire lock for accessing the tuple
     auto starttime = GetSystemClock();
     rc = CCManager::AcquireLock(p_table->GetLockState(tuple->GetTID()), ac_type == UPDATE);
     if (g_warmup_finished) {
       uint64_t cctime =  GetSystemClock() - starttime;
       AddCCTime(cctime);
     }
     if (rc == ABORT) {
        // LOG_INFO("XXX Failed to Acq X lock for key %ld", GetPartitionID(key.ToUInt64()), key.ToUInt64());
        if (g_enable_partition_covering_lock) {
          LOG_XXX("XXX Failed to Acq %s lock for key %ld in partition %ld", (ac_type == UPDATE ? "X" : "S"), key.ToUInt64(), GetPartitionID(key.ToUInt64()));
        } else {
          LOG_XXX("XXX Failed to Acq %s lock for key %ld", (ac_type == UPDATE ? "X" : "S"), key.ToUInt64());
        }
       // tuple is not added to access, must free here
       FinishAccess(*result, false);
       return rc;
     } else {
      if (g_enable_partition_covering_lock) {
        LOG_XXX("XXX Success to Acq %s lock for key %ld in partition %ld", (ac_type == UPDATE ? "X" : "S"), key.ToUInt64(), GetPartitionID(key.ToUInt64()));
      } else {
        LOG_XXX("XXX Success to Acq %s lock for key %ld", (ac_type == UPDATE ? "X" : "S"), key.ToUInt64());
      }
        // if (ac_type == UPDATE) {
        //    LOG_INFO("XXX Acq X lock for key %ld", key.ToUInt64());
        // }

     }
  }

  Access ac = {ac_type == UPDATE ? UPDATE : READ,
               key, p_table, result, 1};
  ac.need_release_lock = need_lock;
  // ac.free_ptr = free_ptr;
  sz = p_table->GetTupleDataSize();
  if (ac_type == UPDATE) {
    // since limit = 1, write copy here is a single copy. for cases with more than
    // one rows sharing same key, here should call new for each of them
    auto write_copy = new WriteCopy(sz);
    write_copy->Copy(tuple->GetData());
    ac.data_ = write_copy;
    data = write_copy->data_;
    if (g_buf_type == NOBUF) {
      ac.status_ = NEW(volatile RC);
      *ac.status_ = RC::PENDING;
      // start flushing writes async if no buf.
      // Note: here assume updating is done. no performance-wise differences
      // as flushing after the actual updates.
      g_data_store->WriteAsync(p_table->GetStorageId(), key,
                               tuple->GetData(), sz, ac.status_);
    }
  } else {
    if (ac_type == SCAN) ac.free_ptr = false;
    data = tuple->GetData();
  }

  tuple_accesses_.push_back(ac);
  return rc;
}

RC ITxn::AccessBottomTuple(SharedPtr *slot, ITable *p_table,
                           uint64_t expected_key, char *&data) {
  // two-tree (HYBRBUF) range-scan row: pin the bottom-tree page holding the
  // key's pre-slotted copy and take the key's read lock. The lock lives in
  // the per-table lock domain (ITable::GetLockState) shared with the
  // top-tree point ops, so scans conflict correctly with writers even
  // though the bytes are read from the bottom copy.
  if (g_warmup_finished)
    g_stats->db_stats_[Worker::GetThdId()].incr_tuple_accesses_(1);
  auto tuple = ((PageBufferManager *) g_bottom_buf_mgr)
                   ->AccessTuple(p_table, *slot, expected_key);
  // same lock policy as the PGBUF scan path (SCAN rows skip record locks
  // only under partition covering locks)
  auto need_lock = !g_enable_partition_covering_lock;
  RC rc = RC::OK;
  if (need_lock) {
    auto starttime = GetSystemClock();
    rc = CCManager::AcquireLock(p_table->GetLockState(tuple->GetTID()), false);
    if (g_warmup_finished) AddCCTime(GetSystemClock() - starttime);
    if (rc == RC::ABORT) {
      // not registered: unpin the page here
      ((PageBufferManager *) g_bottom_buf_mgr)->FinishAccessTuple(*slot, false);
      return rc;
    }
  }
  // TID == key (asserted by the page manager); do NOT use GetPrimaryKey
  // here -- it offsets by the global g_buf_type, wrong for in-page tuples
  Access ac = {READ, SearchKey(tuple->GetTID()), p_table, slot, 1};
  ac.need_release_lock = need_lock;
  ac.free_ptr = false;
  ac.bottom_pg_ = true;
  data = tuple->GetData(PGBUF);
  tuple_accesses_.push_back(ac);
  return rc;
}

void ITxn::Abort() {
    // collect stats
  if (g_warmup_finished) {
    g_stats->db_stats_[Worker::GetThdId()].incr_abort_cnt_(1);
    LOG_XXX("XXX abort count is %ld is for worker id %ld", g_stats->db_stats_[Worker::GetThdId()].abort_cnt_, Worker::GetThdId());
    if (txn_tpe == TxnType::RW_TXN) {
      g_stats->db_stats_[Worker::GetThdId()].incr_rw_abort_cnt_(1);
    } else if (txn_tpe == TxnType::INSERT_TXN) {
      g_stats->db_stats_[Worker::GetThdId()].incr_insert_abort_cnt_(1);
    } else if (txn_tpe == TxnType::SCAN_TXN) {
      g_stats->db_stats_[Worker::GetThdId()].incr_scan_abort_cnt_(1);
    }
  }
  // auto phantom_entries = GetPhantomEntries();
  // for (auto & entry : *phantom_entries) {
  //   auto table = db_->GetTable(entry->GetTableId());
  //   table->IndexDelete(entry);
  // }
  RemovePhantomEntries();

  // release lock and dealloc space used for storing accesses
  for (const auto& access : idx_accesses_) {
    auto ac = (access.ac_ == READ || access.ac_ == SCAN) ? READ : UPDATE;
    for (auto & lk : access.locks_) {
      // LOG_XXX("XXX release %s lock for pid %d in abort", (iac.lock_type == S)? "S":"IX", ac.)
      CCManager::ReleasePhantomIntentionLock(*lk, (ac == UPDATE) ? true : false);
    }
  }
  for (auto access : tuple_accesses_) {
    for (int i = 0; i < access.num_rows_; i++) {
      auto tuple = reinterpret_cast<ITuple *>(access.rows_[i].Get());
      auto lock_id = tuple->GetTID();
      // wait for async req to finish to avoid seg fault.
      if (access.status_) {
        while(*access.status_ == RC::PENDING) {};
        DEALLOC((RC *) access.status_);
      }
      // for now, always flush (line: 60) before unpin.
      FinishAccess(access.rows_[i], false, access.free_ptr, access.bottom_pg_);
      if (access.need_release_lock) {
        CCManager::ReleaseLock(access.tbl_->GetLockState(lock_id), access.ac_);
      }
      delete &access.data_[i];
    }
    // Free is already called above, here only dealloc shared ptr itself
    if (access.free_ptr)
      DEALLOC(access.rows_);
  }
  tuple_accesses_.clear();
  // DEALLOC(scan_array);
  // release intention locks
  for (auto & iac: intention_locks_ ) {
     LOG_XXX("XXX release %s lock for pid %d in abort", (iac.lock_type == S)? "S":"IX", iac.partition_id_)
    //  CCManager::ReleaseIntentionLock(*(iac.lock_), iac.lock_type);
     CCManager::ReleasePhantomIntentionLock(*(iac.lock_), (iac.lock_type == ILockType::IX) ? true : false);
  }
   

}

void ITxn::PreCommit() {
  std::stringstream to_flush;
  for (const auto& access : idx_accesses_) {
    auto ac = (access.ac_ == READ || access.ac_ == SCAN) ? READ : UPDATE;
    if (!g_early_lock_release || ac != READ)
      continue;
    for (auto & lk : access.locks_) {
      CCManager::ReleasePhantomIntentionLock(*lk, ac);
    }
  }
  for (auto access : tuple_accesses_) {
    for (int i = 0; i < access.num_rows_; i++) {
      auto tuple = reinterpret_cast<ITuple *>(access.rows_[i].Get());
      if (tuple == nullptr) {
        // Use-after-evict: the slot this access points at was reclaimed before
        // commit, so applying the update would dereference null. Skipping keeps
        // the run alive at the cost of DROPPING this row's update; the counter
        // makes that loss visible so a run can be judged (a handful is a
        // perturbation, thousands invalidate the run). The real fix is to pin
        // the slot for the access's lifetime.
        g_null_tuple_skips.fetch_add(1, std::memory_order_relaxed);
        continue;
      }
      if (access.ac_ == UPDATE) {
        // A reclaimed slot can also surface as a non-null tuple whose table id
        // no longer matches the access; writing through it would index
        // tables_ out of bounds. Same treatment as the null case above.
        if (tuple->GetTableId() != access.tbl_->GetTableId()) {
          g_null_tuple_skips.fetch_add(1, std::memory_order_relaxed);
          continue;
        }
        tuple->SetData(access.data_[i].data_, access.data_[i].sz_);
        auto schema = db_->GetTable(tuple->GetTableId())->GetSchema();
        auto key = tuple->GetPrimaryKey(schema).ToString();
        if (g_buf_type != NOBUF) {
          to_flush << key << "," << std::string(
              reinterpret_cast<char *>(tuple), access.tbl_->GetTotalTupleSize())
                   << std::endl;
        }
      }
      if (g_early_lock_release && access.ac_ != UPDATE) {
        // release read locks (and write locks if using elr)
        if (g_buf_type == NOBUF && access.status_) {
          // need to wait until the data is flushed
          while (!g_terminate_exec && *access.status_ == RC::PENDING) {};
          M_ASSERT(*access.status_ != RC::ERROR || g_terminate_exec, "error in async request");
          if (!g_terminate_exec) {
            DEALLOC((RC *) access.status_);
          }
        }
        auto lock_id = tuple->GetTID();
        FinishAccess(access.rows_[i], access.ac_ == UPDATE, access.free_ptr,
                     access.bottom_pg_); // unpin the data
        if (access.need_release_lock) {
          CCManager::ReleaseLock(access.tbl_->GetLockState(lock_id), access.ac_);
        }
      }
    }
  }

  // release intention locks
  for (auto & iac: intention_locks_) {
     //TODO: confirm how to deal with intention lock for g_early_lock_release
     if ((!g_early_lock_release) || (iac.lock_type == ILockType::IX)) {
      continue;
     }
     LOG_XXX("XXX release %s lock for pid %d in precommit", (iac.lock_type == ILockType::S)? "S":"IX", iac.partition_id_)
    CCManager::ReleasePhantomIntentionLock(*(iac.lock_), (iac.lock_type == ILockType::IX) ? true : false);

  }

  if (g_buf_type == NOBUF && g_early_lock_release) {
    // no need to contain redo logs as write data will be forced.
    // if w/o elr, still need to contain write data since server may fail.
    to_flush << "committed";
  }
  auto log = to_flush.str();
  log_starttime_ = GetSystemClock(); 
  if (!is_read_only && !(g_warmup_skip_log && !g_warmup_finished)) {
      lsn_ = g_log_store->AppendToLogBuffer(GetTxnId(), log);
  } 

}

RC ITxn::Commit() {
  // wait for log to be flushed
  auto starttime = GetSystemClock();
  auto flushed = true;
  if (!is_read_only && !(g_warmup_skip_log && !g_warmup_finished)) {
    flushed = g_log_store->WaitForFlush(lsn_);
  }
  

  flushed_ = flushed;
  // LOG_INFO("commit thread %u finished logging", Worker::GetCommitThdId());
  log_time_ = GetSystemClock() - log_starttime_;

  // release locks
  for (const auto& access : idx_accesses_) {
    auto ac = (access.ac_ == READ || access.ac_ == SCAN) ? READ : UPDATE;
    if (ac == UPDATE || (!g_early_lock_release)) {
      for (auto & lk : access.locks_) {
        CCManager::ReleasePhantomIntentionLock(*lk, ac);
      }
    }
  }
  for (auto access : tuple_accesses_) {
    for (int i = 0; i < access.num_rows_; i++) {

      if ((!g_early_lock_release) || access.ac_ == UPDATE) {
        if (g_buf_type == NOBUF && access.status_) {
          // need to wait until the data is flushed
          while (!g_terminate_exec && *access.status_ == RC::PENDING) {};
          M_ASSERT(*access.status_ == RC::OK || g_terminate_exec, "error in async request");
          if (!g_terminate_exec) {
            DEALLOC((RC *) access.status_);
          }
        }
        auto tuple = reinterpret_cast<ITuple *>(access.rows_[i].Get());
        auto lock_id = tuple->GetTID();
        FinishAccess(access.rows_[i], !g_force_write, access.free_ptr,
                     access.bottom_pg_); // unpin the data
        if (access.need_release_lock) {
         CCManager::ReleaseLock(access.tbl_->GetLockState(lock_id), access.ac_);
        }
      }
      delete &access.data_[i];
    }
    // Free is already called, here only dealloc shared ptr itself
    if (access.free_ptr)
      DEALLOC(access.rows_);
  }
  tuple_accesses_.clear();
  // DEALLOC(scan_array);
  // release intention locks
  for (auto & iac: intention_locks_) {
     //TODO: confirm how to deal with intention lock for g_early_lock_release
     if ((!g_early_lock_release) || (iac.lock_type == ILockType::IX)) {
        LOG_XXX("XXX release %s lock for pid %d in commit", (iac.lock_type == ILockType::S)? "S":"IX", iac.partition_id_)
        // CCManager::ReleaseIntentionLock(*(iac.lock_), iac.lock_type);
        CCManager::ReleasePhantomIntentionLock(*(iac.lock_), (iac.lock_type == ILockType::IX) ? true : false);

     }
  }

  //delete phantom entries
  RemovePhantomEntries();
  

  // update stats
  if (flushed && g_warmup_finished && !g_terminate_exec) {
    // commit stats
    auto latency = GetSystemClock() - txn_starttime_;
  }
  return RC::OK;
}

void ITxn::FinishAccess(SharedPtr &ptr, bool dirty, bool batch_begin, bool bottom_pg) {
  if (bottom_pg) {
    // two-tree scan row: unpin the BOTTOM-tree page (the default HYBRBUF
    // route below is the top object manager and would misread the ref)
    ((PageBufferManager *) g_bottom_buf_mgr)->FinishAccessTuple(ptr, dirty);
  } else if (g_buf_type == OBJBUF || g_buf_type == HYBRBUF) {
    arboretum::ObjectBufferManager::FinishAccessTuple(ptr, dirty);
  } else if (g_buf_type == PGBUF) {
    ((PageBufferManager *) g_buf_mgr)->FinishAccessTuple(ptr, dirty);
  } else{
     if (batch_begin) ptr.Free();
  }
}
}