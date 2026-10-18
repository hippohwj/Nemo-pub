#ifndef ARBORETUM_DISTRIBUTED_SRC_DB_DB_H_
#define ARBORETUM_DISTRIBUTED_SRC_DB_DB_H_

#include "common/ThreadPool.h"
#include "common/Common.h"

namespace arboretum {

class ITable;
class ISchema;
class ITxn;

class ARDB {

 public:
  ARDB();
  void StartBackgroundEviction();

  OID CreateTable(std::string tbl_name, ISchema * schema); // will use pkey as index
  OID CreateIndex(OID tbl_id, OID col, IndexType tpe, BufferType buff_tpe);
  void BatchInitInsert(OID tbl, OID partition, std::multimap<std::string, std::string> &map);
  void AddLoadedRows(OID tbl, uint64_t n);
  void InitInsert(OID tbl, char * data, size_t sz);
  void InitInsertWithPK(OID tbl, char *data, size_t sz, uint64_t pk, bool is_negative = false, bool unset_gapbit = false, bool pin = false);
  bool DeleteWithPK(OID tbl, uint64_t pk, bool is_tombstone = false);
  // transactional
  RC InsertTuple(SearchKey& pkey, OID tbl, char * data, size_t sz, ITxn * txn);
  RC GetTuple(OID tbl, OID idx_id, SearchKey key,
              char *& data, size_t &sz, AccessType ac, ITxn * txn,
              bool blind = false);
  RC GetTuples(OID tbl, OID idx_id, SearchKey low_key, SearchKey high_key, ITxn * txn);
  // non-transactional
  RC GetTuple(OID tbl_id, SearchKey key, std::string &data);
  // prev_txn: the worker's previous txn, reclaimed here (dtor + free) —
  // workers read its stats after CommitTxn, so it must outlive that read.
  // retry: this txn re-executes prev_txn's query; inherit its user-visible
  // start time (txn_starttime_) instead of starting fresh.
  ITxn * StartTxn(ITxn *prev_txn = nullptr, bool retry = false);
  void Terminate(std::vector<std::thread> & threads);
  RC CommitTxn(ITxn * txn, RC rc);
  static void CommitTask(ITxn * txn);

  ITable *GetTable(OID i) { return tables_[i]; };
  size_t GetTableCount() const { return table_cnt_; };
  static void LoadConfig(int i, char **p_string);
  static bool CheckBufferWarmedUp(uint64_t row_cnt = 0, uint64_t page_cnt = 0);
  static void PrintWarmedUpBuffer();
  void PrintIndex(OID tbl_id);

  static double RandDouble();
  static bool RandBoolean(double probability = 0.5);
  static int RandInteger(int min, int max);
  static int RandIntegerInclusive(int min, int max);
  static void InitRand(uint32_t thd_id) { rand_generator_ = new std::mt19937(thd_id); }
  static void InitRand() { rand_generator_ = new std::mt19937(std::random_device{}()); }

  void FinishLoadingData(OID tbl_id);
  void WaitForAsyncBatchLoading(int cnt);
  void RestoreTable(OID tbl_id, uint64_t num_rows, uint64_t data_domain_sz);

  size_t GetTotalPgCnt();
  void FinishWarmupCache();
 private:
  size_t table_cnt_{0};
  std::vector<ITable *> tables_;
  ThreadPool<std::function<void(ITxn *)>, ITxn> commit_thd_pool_;
  static __thread std::mt19937 * rand_generator_;
};

} // arboretum


#endif //ARBORETUM_DISTRIBUTED_SRC_DB_DB_H_
