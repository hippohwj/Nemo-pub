#ifndef ARBORETUM_SRC_DB_ACCESS_INDEXBTREEOBJECT_H_
#define ARBORETUM_SRC_DB_ACCESS_INDEXBTREEOBJECT_H_

#include "IIndex.h"

namespace arboretum {

class ITuple;
class ITxn;
class IndexBTreeObject;
class IIndex;
class IdxAccess;

struct BTreeObjNode {
  OID id_{0}; // for debug
  size_t level_{0};
  SearchKey *keys_{};
  SharedPtr *children_{}; // leaf node: reserve children_[0] for neighbor ptr
  size_t key_cnt_{0};
  RWLock latch_{};
  // two-way link in leaf nodes for faster scan
  volatile BTreeObjNode *next_{nullptr};
  volatile BTreeObjNode *prev_{nullptr};

  // left bound info
  SearchKey left_bound_{0};
  SearchKey right_bound_{UINT64_MAX};
  std::atomic<uint32_t> left_bound_lock_{0};
  bool left_bound_in_storage_{false};
  inline bool IsLeaf() const { return level_ == 0; };
  bool IsPriorGapUnset(uint32_t key_idx);
  bool IsPriorGapSet(uint32_t key_idx);

  SharedPtr* SafeInsert(InsertType& insert_type, SearchKey &key, void *val, AccessType ac,
                        ITxn *txn, IndexBTreeObject* tree);
  SharedPtr* SafePhantomInsert(SearchKey& key, void* val, AccessType ac, ITxn* txn,
                          IndexBTreeObject* tree);
  SharedPtr* SafeUserInsert(SearchKey &key, void * val, AccessType ac,
                            ITxn *txn, IndexBTreeObject *tree);
  SharedPtr* SafeCacheInsert(SearchKey& key, void* val, AccessType ac,
                              ITxn* txn, IndexBTreeObject* tree);

  bool SafeDelete(SearchKey &key, ITuple *tuple = nullptr);
  bool SafeDelete(SearchKey &key, ITuple *tuple, bool* deleted);

};

// mimimized node structure
// four bytes: tree level + key_cnt 
// 2 * 8 bytes: two pointers for neighbor nodes
#define MIN_NODE_META_SZ (4 + (2*sizeof(BTreeObjNode *)))

class IndexBTreeObject : public IIndex {

  struct StackData {
    BTreeObjNode * node_{nullptr};
    struct StackData * parent_{nullptr};
  };
  typedef StackData *Stack;

 public:
  IndexBTreeObject(OID tbl_id, ITable *tbl);
  SharedPtr* RangeSearch(SearchKey start, SearchKey end, ITxn *txn,
                         std::vector<std::pair<SearchKey, SearchKey>> &fetch_req,
                         RC &rc, size_t &cnt, BTreeObjNode* hint_parent = nullptr, BTreeObjNode* hint_child = nullptr);
  // two-tree (HYBRBUF) freshness overlay: peek-only walk over [start, end]
  // collecting refs to CACHED DIRTY records into slots[key - start]; no
  // remote fills, no locks, no miss accounting (see the .cpp comment)
  size_t PeekDirtyRange(SearchKey start, SearchKey end, SharedPtr *slots);
  // debug census: walk the entire leaf chain and count resident entries by
  // kind (regular / tombstone / sentinel) plus their real bytes (tuple_sz per
  // regular record, sizeof(ITuple) per tombstone/sentinel). Not latched --
  // call only when the workload is stopped (ARDB::Terminate).
  void CensusResident(size_t tuple_sz, size_t &regular, size_t &tombstone,
                      size_t &sentinel, size_t &bytes);
  size_t census_write_once_{0}, census_dirty_{0}, census_ref_{0};
  SharedPtr* Search(SearchKey key, AccessType ac_type, ITxn *txn, RC &rc);
  // 2tree warmup-loss diagnosis: latch-free leaf-chain walk counting
  // resident keys, order violations, duplicates and tombstones. Call
  // only from quiesced phases (warmup/restore boundaries).
  void DiagLeafWalk(const char *label);
  RC Insert(SearchKey key, ITuple * tuple, AccessType ac, ITxn *txn,
            SharedPtr *tag = nullptr);
  RC PhantomInsert(SearchKey key, ITuple* tuple, AccessType ac, ITxn* txn, SharedPtr* tag, BTreeObjNode** hint_parent, BTreeObjNode** hint_child);
  RC EndPhantomInsert(SearchKey key, ITuple* tuple, AccessType ac, ITxn* txn, SharedPtr* tag, BTreeObjNode* hint_parent, BTreeObjNode* hint_child);
  // RC CacheInsert(SearchKey key, ITuple * tuple, AccessType ac, ITxn *txn,
  //                SharedPtr *tag = nullptr);
  void Delete(SearchKey key, ITuple *p_tuple);
  void Delete(SearchKey key, ITuple *p_tuple, bool* deleted);
  void Load(size_t pg_cnt) override {};
  bool PrintTree(ITuple * p_tuple=nullptr);
  static void PrintNode(BTreeObjNode *node);
  void MarkRightBoundAvailInStorage();
  // Randomly pick a leaf node and evict
  bool EvictRandomLeafNode();
  // post-run: write every dirty cached tuple back to remote storage without
  // touching the tree (single-threaded, callers must have joined all worker
  // and eviction threads); returns the number of tuples flushed
  size_t FlushDirtyTuples();

 private:
  bool IsPriorGapSet(uint32_t key_idx);
  bool IsPriorGapUnset(uint32_t key_idx);

  RC RangeEntryIter(SearchKey key, ITuple* entry, SearchKey start, SearchKey end,  std::vector<std::pair<SearchKey, SearchKey>>& fetch_req,  ITxn* txn, IdxAccess& idx_ac, bool& gap_open);
  RC PhantomProtectionHelper(std::atomic<uint32_t> * lock, ITxn* txn, IdxAccess& idx_ac, bool is_exclusive);
  void Search(SearchKey key, AccessType ac, BTreeObjNode * &parent,
              BTreeObjNode * &child);
  Stack PessimisticSearch(SearchKey key, BTreeObjNode * &parent,
                          BTreeObjNode * &child, AccessType ac);
  


  // only for concurrent eviction 
  void RandomPickLeafNode(AccessType ac, BTreeObjNode * &parent, BTreeObjNode * &child);
  
  // find eviction candidates from the leaf node
  void FindEvictionCandidates(BTreeObjNode *node, std::vector<SearchKey> &candidates_keys, std::vector<ITuple *> &candidates);

  RC RecursiveInsert(InsertType& insert_type, Stack stack, SearchKey key, void * val,
                             BTreeObjNode *child, Stack full_stack,
                             AccessType ac, ITxn *txn, SharedPtr *tag);
  void RecursiveDelete(Stack stack, SearchKey key, BTreeObjNode *child,
                       ITuple * tuple, Stack full_stack, bool* deleted);
  void RecursiveDeleteMultiple(Stack stack, std::vector<SearchKey> keys, BTreeObjNode *child,
                               std::vector<ITuple *> tuples, Stack full_stack);

  static OID ScanBranchNode(BTreeObjNode * node, SearchKey key);
  static OID RandomBranchNode(BTreeObjNode * node);
  static SharedPtr* ScanLeafNode(BTreeObjNode * node, SearchKey key);

  // helper functions
  BTreeObjNode * AllocNewNode();
  inline bool SplitSafe(BTreeObjNode *node, size_t insert_sz = 1) const {
    return !node || node->key_cnt_ + insert_sz <= split_bar_;
  }

  inline bool DeleteSafe(BTreeObjNode *node, size_t delete_sz = 1) const {
    return !node || node->key_cnt_ > delete_sz;
  }


  inline bool IsRoot(BTreeObjNode *node) { return node == root_.Get(); };
  static inline BTreeObjNode * AccessNode(SharedPtr& ptr) {
    return reinterpret_cast<BTreeObjNode *>(ptr.Get());
  }
  static inline BTreeObjNode * AccessNode(SharedPtr& ptr, bool exclusive) {
    auto node = AccessNode(ptr);
    if (node) node->latch_.Lock(exclusive);
    return node;
  }
  void FreeStack(Stack full_stack);

  SharedPtr root_{};
  size_t fanout_{6};
  size_t height_{0};
  size_t split_bar_{0};
  std::atomic<size_t> num_nodes_{0};
  size_t node_sz_{0};
  size_t minimized_node_sz_{0};
};

} // namespace arboretum

#endif //ARBORETUM_SRC_DB_ACCESS_INDEXBTREEOBJECT_H_
