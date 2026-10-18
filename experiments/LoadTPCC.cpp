#include "tpcc/tpcc.h"
#include "remote/IDataStore.h"

using namespace arboretum;

int main(int argc, char* argv[]) {
  // Note: must load db config first.
  ARDB::LoadConfig(argc, argv);
  auto bench_config = new(MemoryAllocator::Alloc(sizeof(TPCCConfig)))
      TPCCConfig(argc, argv);
  M_ASSERT(g_buf_type != HYBRBUF, "doesn't support data loading for hybrid "
           "buffer; load the otpcc (NOBUF) and ptpcc (PGBUF) datasets instead");
  M_ASSERT(!g_restore_from_remote, "data loading requires g_restore_from_remote = false");
  auto wh = std::to_string(bench_config->num_warehouses_);
  std::string dataset_id = (g_buf_type == PGBUF ? "ptpcc" : "otpcc") + wh + "w";
  SetDatasetId(g_dataset_id, sizeof(g_dataset_id), dataset_id);
  LOG_INFO("dataset id: %s", g_dataset_id);
  auto db = new (MemoryAllocator::Alloc(sizeof(ARDB))) ARDB;
  auto workload = TPCCWorkload(db, bench_config);
  // simulated store only: persist the loaded dataset for later runs.
  // dump mode writes tsv files instead of store state — checkpointing
  // there would write the (empty) store over any existing snapshot and,
  // when one is present, burn minutes rewriting GBs for nothing.
  if (!g_dump_load_dir[0]) {
    g_data_store->Checkpoint();
  }
  return 0;
}
