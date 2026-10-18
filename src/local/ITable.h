#ifndef ARBORETUM_SRC_LOCAL_ITABLE_H_
#define ARBORETUM_SRC_LOCAL_ITABLE_H_


#include <utility>

#include "common/Common.h"
#include "IPage.h"
#include "ITuple.h"


namespace arboretum {

class IIndex;
class ITxn;
class ITable {

 public:
  ITable(std::string tbl_name, OID tbl_id, ISchema * schema);
  void RestoreTable(uint64_t num_rows, uint64_t data_domain_sz);
  OID CreateIndex(OID col, IndexType tpe);
  OID CreateIndex(OID col, IndexType tpe, BufferType buf_tpe);
  void InitInsertTuple(char *data, size_t sz);
  // pin: keep the cached tuple resident (never evicted); used for the
  // per-table boundary anchor records that range scans rely on
  void InitInsertTupleWithPK(char *data, size_t sz, uint64_t pk, bool is_negative = false,  bool unset_gapbit = false, bool pin = false);
  bool DeleteWithPK(uint64_t pk, bool is_tombstone = false);

  // SharedPtr* IndexSearch(SearchKey key, OID idx_id, ITxn *txn, RC &rc);
  // blind: insert semantics for a full-row overwrite of a pre-allocated
  // slot whose remote content is known (zeros + key) - on a record-cache
  // miss the base row is fabricated locally instead of fetched (OBJBUF
  // only; other buffer types ignore the flag)
  SharedPtr* IndexSearch(SearchKey key, AccessType ac_type, ITxn *txn, RC &rc, IndexType idx_type, BufferType buf_type, bool blind = false);
  SharedPtr * IndexRangeSearch(SearchKey low_key, SearchKey high_key, OID
  idx_id, ITxn *txn, RC &rc, size_t &cnt);
  RC InsertTuple(SearchKey& pkey, char *data, size_t sz, ITxn *txn);
  void IndexDelete(ITuple *tuple);
  bool BatchEvictionViaIndexTraverse();
  // post-run: flush all dirty cached records to remote (OBJBUF only);
  // returns the number of records written
  size_t FlushDirtyObjects();
  void TwoTreeEviction(SearchKey flush_key, char *flush_data, size_t flush_sz);
  IIndex * GetBottomIdx();
  IIndex * GetTopIdx();
  const std::string& GetTableName() const { return tbl_name_; }
  void PrintIndex();

  inline ISchema *GetSchema() { return schema_; };
  inline OID GetTableId() const { return tbl_id_; };
  inline size_t GetRowCnt() const { return row_cnt_.load(); };
  inline std::string GetStorageId() const { return storage_id_; };
  inline std::string GetTopStorageId() const { 
      if (g_index_type == TWOTREE) {
        return top_storage_id_; 
      } else {
        return storage_id_;
      }
    };
  inline std::string GetBottomStorageId() const {
    if (g_index_type == TWOTREE) {
      return bottom_storage_id_; 
    } else {
      return storage_id_; 
    }
    };
  static std::string GetStorageId(OID tbl_id) {
    // std::string id = g_dataset_id;
    std::string id = (g_index_type == TWOTREE)? g_bottom_dataset_id: g_dataset_id;
    // Index with phantom protection is directed to a different table.
    if (tbl_id >= (1 << 28)) {
      if (g_uni_page_idx_tbl) {
        tbl_id = tbl_id - (1 << 28);
      } else {
        id = "page-wl" + id.substr(4, id.size() - 4);
      }
    }
    return id + "-" + std::to_string(tbl_id); };
  inline size_t GetTotalTupleSize() {
    return schema_->tuple_sz_ + sizeof(ITuple);
  }; // including size of metadata
  inline size_t GetTupleDataSize() { return schema_->tuple_sz_; };
  std::atomic<uint32_t>& GetLockState(OID tid) { return *lock_tbl_[tid]; };
  // std::atomic<uint64_t>& GetIntentionLockState(OID pid) { return *intention_lock_tbl_[pid]; };

  // std::atomic<uint64_t>* GetIntentionLockStatePtr(OID pid) { 
  //   M_ASSERT(pid < intention_lock_tbl_.size(), "pid is larger than container: %d vs %d", pid, intention_lock_tbl_.size());
  //   return intention_lock_tbl_[pid];
  // };


  std::atomic<uint32_t>& GetIntentionLockState(OID pid) { return *intention_lock_tbl_[pid]; };

  std::atomic<uint32_t>* GetIntentionLockStatePtr(OID pid) { 
    M_ASSERT(pid < intention_lock_tbl_.size(), "pid is larger than container: %d vs %d", pid, intention_lock_tbl_.size());
    return intention_lock_tbl_[pid];
  };



  // std::atomic<uint32_t>* IxLockWaitStatePtr(OID pid) { 
  //   M_ASSERT(pid < ix_lock_wait_tbl_.size(), "pid is larger than container: %d vs %d", pid, ix_lock_wait_tbl_.size());
  //   return ix_lock_wait_tbl_[pid];
  // };

  void FinishLoadingData();
  // batch-load bookkeeping: BatchInitInsert writes to the store directly,
  // so the loader credits row_cnt_ (source of the metadata row) here
  void AddLoadedRows(uint64_t n) { row_cnt_.fetch_add(n); }
  size_t GetPgCnt();

  void FinishWarmupCache();
  // the first index created on this table (index ids are globally assigned,
  // so table i's primary index id is generally not 1 when multiple tables exist)
  IIndex * GetPrimaryIdx() { return indexes_[primary_idx_id_]; };
  OID GetPrimaryIdxId() const { return primary_idx_id_; };
 private:
  bool InitIndexInsert(ITuple * p_tuple, OID pg_id, OID offset);

  
  SharedPtr * IndexRangeSearchNoNegativeOp(SearchKey low_key, SearchKey high_key, OID idx_id,
    ITxn *txn, RC &rc, size_t &cnt);
  SharedPtr * IndexRangeSearchNegativePhantomOp(SearchKey low_key, SearchKey high_key, OID idx_id,
    ITxn *txn, RC &rc, size_t &cnt);
  SharedPtr * IndexRangeSearchNegativeOp(SearchKey low_key, SearchKey high_key, OID idx_id,
    ITxn *txn, RC &rc, size_t &cnt);

  std::string tbl_name_;
  std::string storage_id_;
  std::string top_storage_id_;
  std::string bottom_storage_id_;
  OID tbl_id_{0};
  std::atomic<size_t> row_cnt_{0};
 public:
  // g_evict_write_once_first: number of write-once (blind-insert) records
  // currently cached in this table's object tree; drives tier-1 table
  // selection in ObjectBufferManager::BatchTraverseEvict
  std::atomic<size_t> cached_write_once_{0};
 private:
  std::atomic<size_t> pg_cnt_{1};
  std::unordered_map<OID, IIndex *> indexes_;
  OID primary_idx_id_{0};
  OID top_idx_id_{0};
  OID bottom_idx_id_{0};
  BasePage * pg_cursor_{nullptr};
  ISchema * schema_;
  std::vector<std::atomic<uint32_t>*> lock_tbl_; // each lock: 1-bit EX lock, 31-bit ref cnt
  std::vector<std::atomic<uint32_t>*> intention_lock_tbl_; // for partition-covering-lock
  
  // std::vector<std::atomic<uint64_t>*> intention_lock_tbl_; // for partition-covering-lock
  // std::vector<std::atomic<uint32_t>*> ix_lock_wait_tbl_; // for partition-covering-lock
};

} // arboretum

#endif //ARBORETUM_SRC_LOCAL_ITABLE_H_
