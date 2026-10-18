#include "ycsb/ycsb.h"
#include "remote/IDataStore.h"

using namespace arboretum;

int main(int argc, char* argv[]) {
  // Note: must load db config first.
  ARDB::LoadConfig(argc, argv);
  auto bench_config = new(MemoryAllocator::Alloc(sizeof(YCSBConfig)))
      YCSBConfig(argc, argv);
  M_ASSERT(g_buf_type != HYBRBUF, "doesn't support data loading for hybrid buffer, using pgbuf or objbuf instead");
  std::string recordsz_prefix = (g_record_size == 1008) ? "" : "-r" + std::to_string(g_record_size);
  std::string negative_prefix = (g_negative_point_wl || g_negative_scan_wl) ?  "-n" + std::to_string(bench_config->domain_size_/1000000)+"g": "";
  std::string dataset_id = (g_buf_type == PGBUF ?
      "page"+ std::to_string(g_idx_btree_fanout) + "-": "obj-")  +
      std::to_string(bench_config->num_rows_ / 1000000) + "g" + recordsz_prefix + negative_prefix;
  SetDatasetId(g_dataset_id, sizeof(g_dataset_id), dataset_id);
  LOG_DEBUG("dataset id: %s", g_dataset_id);
  auto db = new (MemoryAllocator::Alloc(sizeof(ARDB))) ARDB;
  auto workload = YCSBWorkload(db, bench_config);
  // simulated store only: persist the loaded dataset for later runs.
  // dump mode writes tsv files instead of store state — checkpointing
  // there would write the full dataset (100g-scale) to disk for nothing.
  if (!g_dump_load_dir[0]) {
    g_data_store->Checkpoint();
  }
  return 0;
}

