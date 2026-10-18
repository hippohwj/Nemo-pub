#include "tpcc/tpcc.h"
using namespace arboretum;

// dataset ids (g_dataset_id is char[32], g_dataset_suffix appended):
//   OBJBUF/NOBUF: otpcc<W>w    PGBUF: ptpcc<W>w
//   HYBRBUF: top = otpcc<W>w, bottom = ptpcc<W>w
int main(int argc, char* argv[]) {
  // Note: must load db config first.
  ARDB::LoadConfig(argc, argv);
  auto bench_config = new (MemoryAllocator::Alloc(sizeof(TPCCConfig))) TPCCConfig(argc, argv);
  if (g_enable_partition_covering_lock) {
    // partition_covering_lock doesn't need next-key locking
    g_enable_phantom_protection = false;
  }
  auto wh = std::to_string(bench_config->num_warehouses_);
  if (g_buf_type == PGBUF) {
    std::string dataset_id = "ptpcc" + wh + "w";
    SetDatasetId(g_dataset_id, sizeof(g_dataset_id), dataset_id);
    LOG_INFO("dataset id: %s", g_dataset_id);
  } else if (g_buf_type == OBJBUF || g_buf_type == NOBUF) {
    std::string dataset_id = "otpcc" + wh + "w";
    SetDatasetId(g_dataset_id, sizeof(g_dataset_id), dataset_id);
    LOG_INFO("dataset id: %s", g_dataset_id);
  } else if (g_buf_type == HYBRBUF) {
    std::string top_dataset_id = "otpcc" + wh + "w";
    SetDatasetId(g_dataset_id, sizeof(g_dataset_id), top_dataset_id);
    SetDatasetId(g_top_dataset_id, sizeof(g_top_dataset_id), top_dataset_id);
    std::string bottom_dataset_id = "ptpcc" + wh + "w";
    SetDatasetId(g_bottom_dataset_id, sizeof(g_bottom_dataset_id), bottom_dataset_id);
    LOG_INFO("two-tree datasets: top %s, bottom %s", g_top_dataset_id, g_bottom_dataset_id);
  }
  auto db = new (MemoryAllocator::Alloc(sizeof(ARDB))) ARDB;
  tpcc::tpcc(db, bench_config);
  return 0;
}
