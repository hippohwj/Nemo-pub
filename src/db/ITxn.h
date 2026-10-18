#ifndef ARBORETUM_SRC_DB_ITXN_H_
#define ARBORETUM_SRC_DB_ITXN_H_

#include "common/Common.h"
#include "local/ITuple.h"
#include "local/ITable.h"
#include "db/ARDB.h"

namespace arboretum {

struct WriteCopy {
  explicit WriteCopy(size_t sz) : sz_(sz) {
    data_ = static_cast<char *>(MemoryAllocator::Alloc(sz));
  };
  ~WriteCopy() { DEALLOC(data_); };
  void Copy(void * src) const { memcpy(data_, src, sz_); };
  char *data_{nullptr}; // copy-on-write
  size_t sz_{0};
};

struct Access {
  AccessType ac_{READ};
  SearchKey key_{};
  ITable * tbl_{nullptr};
  SharedPtr *rows_{nullptr};
  int num_rows_{0};
  WriteCopy *data_{nullptr};
  // needs to be dynamically allocated! vector may resize and change the address
  volatile RC *status_{nullptr};
  bool free_ptr{true};
  bool need_release_lock{true};
  // two-tree (HYBRBUF) scan rows pin BOTTOM-tree pages: FinishAccess must
  // route to g_bottom_buf_mgr's page manager, not the top object manager
  bool bottom_pg_{false};
};

struct IdxAccess {
  AccessType ac_{READ};
  OID tbl_id_{0};
  std::vector<std::atomic<uint32_t> *> locks_;
};

struct IntentionLockAccess {
  OID partition_id_{0};
  ILockType lock_type{ILockType::IS};
  // std::atomic<uint64_t> * lock_;
  std::atomic<uint32_t> * lock_;
};

class ITxn {
 public:
  ITxn(OID txn_id, ARDB * db, ITxn *txn) : txn_id_(txn_id), db_(db) {
    starttime_ = GetSystemClock();
    if (!txn) {
      txn_starttime_ = starttime_;
    } else {
      txn_starttime_ = txn->GetStartTime(); 
    }
    thd_id_ = Worker::GetThdId();
  };
  OID GetTxnId() const { return txn_id_; };
  OID GetWorkerThdId() const { return thd_id_; };
  uint64_t GetStartTime() const { return txn_starttime_; };
  uint64_t GetCurrrentStartTime() const { return starttime_; };
  uint64_t GetCCTime() const { return cctime_; };
  uint64_t GetPhantomInsertTime() const { return phantom_insert_time_; };
  uint64_t GetLogTime() const { return log_time_; };
  bool HasFlush(){return flushed_;};
  void AddCCTime(uint64_t delta_time ) { cctime_ += delta_time; };
  void AddPhantomInsertTime(uint64_t delta_time ) { phantom_insert_time_ += delta_time; };
  void Abort();
  void PreCommit();
  bool ReadOnlyTxn() {
    return is_read_only;
  }

  void SetReadOnly(bool read_only) {
    is_read_only = read_only; 
  }
  RC Commit();
  RC GetTuple(ITable *p_table, OID idx_id, SearchKey key, AccessType ac,
              char *&data, size_t &size, bool blind = false);
  RC GetTuples(ITable *p_table, OID idx_id, SearchKey low_key, SearchKey
  high_key);
  RC InsertTuple(SearchKey& pkey, ITable *p_table, char *data, size_t sz);
  RC AccessTuple(SharedPtr *result, ITable * p_table, uint64_t expected_key,
                 AccessType ac_type, char *&data, size_t &sz, bool free_ptr = false, bool *is_miss = nullptr);
  // two-tree (HYBRBUF) range-scan row access: pin the bottom (page-tree)
  // copy and take the key's read lock in the per-table lock domain shared
  // with the top-tree point ops; registers a bottom_pg_-routed access
  RC AccessBottomTuple(SharedPtr *slot, ITable *p_table, uint64_t expected_key,
                       char *&data);

  // release a materialized scan result (see g_scan_materialize); callers must
  // invoke this after consuming each range scan. The slot count is recorded by
  // GetTuples when the OBJBUF gap-bit path publishes its slot-per-key array;
  // PGBUF/REMOTE compact arrays are freed through the registered tuple
  // accesses at commit/abort instead (scan_array_sz_ stays 0 for them).
  void ReleaseScanArray() {
    // free the buffer too: ITxn objects are DEALLOCed without running the
    // destructor, so capacity left in the vector would leak every transaction
    std::vector<std::pair<uint64_t, char *>>().swap(scan_rows_);
    if (scan_array_sz_ == 0) {
      scan_array = nullptr;
      return;
    }
    if (!scan_array) return;
    for (size_t i = 0; i < scan_array_sz_; i++) {
      scan_array[i].ReleaseSlot();
    }
    DEALLOC(scan_array);
    scan_array = nullptr;
    scan_array_sz_ = 0;
  }

  void AddPhantomEntry(ITuple* phantom) {
    phantom_entries_.push_back(phantom);
  }

  vector<ITuple *> * GetPhantomEntries() {
    return &phantom_entries_;
  }

  void RemovePhantomEntries() {
    phantom_entries_.clear();
  }

 public:
  std::vector<Access> tuple_accesses_;
  std::vector<IdxAccess> idx_accesses_;
  // std::vector<std::tuple<std::atomic<uint64_t> *, ILockType>> intention_locks_;
  std::vector<IntentionLockAccess> intention_locks_;
  TxnType txn_tpe;
  SharedPtr * scan_array{nullptr};
  // slots in scan_array when it is an OBJBUF slot-per-key result (else 0)
  size_t scan_array_sz_{0};
  // uniform scan consumption interface (filled when g_scan_materialize):
  // (key, tuple data) of each row returned by the latest GetTuples call
  std::vector<std::pair<uint64_t, char *>> scan_rows_;


 private:

  static void FinishAccess(SharedPtr &ptr, bool dirty = false, bool batch_begin = false,
                           bool bottom_pg = false);
  OID txn_id_{0};
  ARDB * db_;
  uint64_t txn_starttime_; // total execution, including abort time
  uint64_t starttime_; // current execution
  uint64_t log_starttime_{0}; // time to start log 
  uint64_t log_time_{0}; // time to log 
  uint64_t cctime_{0}; // current execution
  uint64_t phantom_insert_time_{0}; // time to insert phantom entries
  uint64_t lsn_;
  OID thd_id_; // worker thread that allocate this transaction
  bool flushed_{false};
  bool is_read_only{false};
  vector<ITuple *> phantom_entries_;

};
}

#endif //ARBORETUM_SRC_DB_ITXN_H_
