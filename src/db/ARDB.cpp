
#include "ARDB.h"
#include "ITxn.h"
#include "buffer/ObjectBufferManager.h"
#include "buffer/PageBufferManager.h"
#include "access/IndexBTreeObject.h"
#include "common/Stats.h"
#include "common/Worker.h"
#include "local/ITable.h"
#include "remote/IDataStore.h"
#include "remote/ILogStore.h"


using boost::property_tree::ptree;

namespace arboretum {
__thread std::mt19937 * ARDB::rand_generator_;

ARDB::ARDB() {
  CalculateCPUFreq();
  // init classes
  LOG_INFO("Start ARDB instance.")
  g_stats = NEW(Stats);
  g_data_store = NEW(IDataStore);
  // g_enable_logging=false skips the log store entirely (no remote -log
  // table gets created/provisioned). Only valid for pure read-only mixes:
  // ITxn only touches g_log_store behind is_read_only guards, so a write
  // txn under this setting null-derefs loudly instead of losing logs.
  if (g_enable_logging) {
    g_log_store = NEW(ILogStore);
  }
  if (g_buf_type == OBJBUF) {
    g_buf_mgr = NEW(ObjectBufferManager)(this);
    // if (g_batch_eviction) {
    ((ObjectBufferManager *)g_buf_mgr)->StartEvictThds();
    // }
  } else if (g_buf_type == PGBUF) {
    g_buf_mgr = NEW(PageBufferManager)(this);
  } else if (g_buf_type == HYBRBUF) {
    // g_top_buf_mgr = NEW(ObjectBufferManager)(this, g_top_tree_buf_sz);
    g_top_buf_mgr = NEW(ObjectBufferManager)(this);
    g_buf_mgr = g_top_buf_mgr;
    g_bottom_buf_mgr =  NEW(PageBufferManager)(this);
    // the top pool relies on these threads for capacity: with
    // g_num_evict_thds > 0, WaitAvailableSlots parks every admitter on
    // batch_cv_ until eviction frees slots, so without them the first
    // full top pool hangs all workers permanently
    ((ObjectBufferManager *)g_top_buf_mgr)->StartEvictThds();
  }
  // create commit threads
  // commit_thd_pool_.Init(g_num_worker_threads * g_commit_pool_sz);
  g_enable_phantom_protection = (g_phantom_protection_type == PHANTOM_NEXT) || (g_phantom_protection_type == NEXT_KEY) || g_enable_phantom_protection;

  // create log threads
  if (g_enable_logging) {
    g_log_store->StartLogThd();
  }
  // create eviction threads
}

void ARDB::StartBackgroundEviction() {
  if (g_buf_type == OBJBUF) {
  // if (g_batch_eviction) {
  ((ObjectBufferManager *)g_buf_mgr)->StartEvictThds();
  // }
  }
}

void ARDB::LoadConfig(int argc, char * argv[]) {
  // load configuration
  if (argc > 1) {
    memcpy(g_config_fname, argv[1], 100);
  }
  ptree root;
  LOG_INFO("Loading DB Config from file: %s", g_config_fname);
  read_json(g_config_fname, root);
  auto config_name = root.get<std::string>("config_name");
  LOG_INFO("Loading DB Config: %s", config_name.c_str());
  for (ptree::value_type &item : root.get_child("db_config")){
    DB_CONFIGS(IF_GLOBAL_CONFIG2, IF_GLOBAL_CONFIG3,
               IF_GLOBAL_CONFIG4, IF_GLOBAL_CONFIG5)
    if (item.first == "g_out_fname") {
      std::strcpy(g_out_fname, item.second.get_value<std::string>().c_str());
      continue;
    }
    if (item.first == "g_sim_snapshot_dir") {
      std::strcpy(g_sim_snapshot_dir, item.second.get_value<std::string>().c_str());
      continue;
    }
    if (item.first == "g_sim_server_addr") {
      auto addr = item.second.get_value<std::string>();
      M_ASSERT(addr.size() < sizeof(g_sim_server_addr),
               "g_sim_server_addr too long");
      std::strcpy(g_sim_server_addr, addr.c_str());
      continue;
    }
    if (item.first == "g_skip_tbl_ids") {
      auto ids = item.second.get_value<std::string>();
      M_ASSERT(ids.size() < sizeof(g_skip_tbl_ids), "g_skip_tbl_ids too long");
      std::strcpy(g_skip_tbl_ids, ids.c_str());
      g_skip_tbl_set.clear();
      std::stringstream ss(ids);
      std::string tok;
      while (std::getline(ss, tok, ',')) {
        if (!tok.empty()) g_skip_tbl_set.insert((OID) std::stoul(tok));
      }
      continue;
    }
    if (item.first == "g_dump_load_dir") {
      auto dir = item.second.get_value<std::string>();
      M_ASSERT(dir.size() < sizeof(g_dump_load_dir), "g_dump_load_dir too long");
      std::strcpy(g_dump_load_dir, dir.c_str());
      continue;
    }
    if (item.first == "g_dataset_suffix") {
      auto suffix = item.second.get_value<std::string>();
      M_ASSERT(suffix.size() < sizeof(g_dataset_suffix),
               "g_dataset_suffix too long: %s", suffix.c_str());
      std::strcpy(g_dataset_suffix, suffix.c_str());
      continue;
    }
  }
  if (!g_enable_group_commit) {
    g_commit_group_sz = 0;
  }
  if (g_buf_type == PGBUF) {
    g_pagebuf_num_slots = PageBufferManager::CalculateNumSlots(g_total_buf_sz);
  }
  if (g_buf_type == HYBRBUF) {
    g_pagebuf_num_slots = PageBufferManager::CalculateNumSlots(g_bottom_tree_buf_sz);
  }

  if (g_workingset_partition_num != 0) {
    g_scan_zipf_partition_sz = g_partition_covering_lock_unit_sz;
  }
  DB_CONFIGS(PRINT_DB_CONFIG2, PRINT_DB_CONFIG3, PRINT_DB_CONFIG4, PRINT_DB_CONFIG5)
}

OID ARDB::CreateTable(std::string tbl_name, ISchema * schema) {
  OID tbl_id = table_cnt_++;
  auto table = NEW(ITable)(std::move(tbl_name), tbl_id, schema);
  tables_.push_back(table);
  return tbl_id;
}

OID ARDB::CreateIndex(OID tbl_id, OID col, IndexType tpe, BufferType buff_tpe) {
  return tables_[tbl_id]->CreateIndex(col, tpe, buff_tpe);
}

RC ARDB::InsertTuple(SearchKey& pkey, OID tbl, char *data, size_t sz, ITxn * txn) {
  return txn->InsertTuple(pkey, tables_[tbl], data, sz);
}

RC ARDB::GetTuple(OID tbl_id, SearchKey key, std::string &data) {
  // index look up tuple
  if (g_buf_type == NOBUF || g_buf_type == OBJBUF) {
    return g_data_store->Read(GetTable(tbl_id)->GetStorageId(), key, data);
  }
  LOG_ERROR("not supported yet.");
}

RC ARDB::GetTuple(OID tbl_id, OID idx_id, SearchKey key, char *&data, size_t &sz,
                  AccessType ac, ITxn *txn, bool blind) {
  // index look up tuple
  auto table = tables_[tbl_id];
  // if idx_id = 0, default is searching from primary index
  auto rc = txn->GetTuple(table, idx_id, key, ac, data, sz, blind);
  return rc;
}

RC ARDB::GetTuples(OID tbl_id, OID idx_id, SearchKey low_key, SearchKey high_key, ITxn *txn) {
  // index look up tuple
  auto table = tables_[tbl_id];
  // if idx_id = 0, default is searching from primary index
  return txn->GetTuples(table, idx_id, low_key, high_key);
}

ITxn *ARDB::StartTxn(ITxn *prev_txn, bool retry) {
  Worker::IncrMaxThdTxnId();
  auto txn_id = g_num_worker_threads * Worker::GetMaxThdTxnId() + Worker::GetThdId();
  // only a retry continues the previous txn's user-visible start time; a
  // fresh query must not inherit it (user latency would chain across txns)
  auto new_txn = new (MemoryAllocator::Alloc(sizeof(ITxn)))
      ITxn(txn_id, this, retry ? prev_txn : nullptr);
  // reclaim the worker's previous txn here, not in CommitTask: (a) the
  // destructor must run — DEALLOC alone strands the heap buffers of every
  // vector member, ~2 KB per txn, which OOMs all-hit runs (~690 MB/s at
  // 368k txn/s); (b) aborted txns never went through CommitTask at all and
  // leaked whole; (c) workers read txn stats after CommitTxn returns, so
  // freeing inside CommitTask was a use-after-free. The ITxn ctor above
  // reads prev_txn->GetStartTime, hence construct-then-free. If commit ever
  // becomes async again (commit_thd_pool_), this must wait for the flush.
  if (prev_txn) {
    prev_txn->~ITxn();
    DEALLOC(prev_txn);
  }
  return new_txn;
}

RC ARDB::CommitTxn(ITxn * txn, RC rc) {
  if (rc == ABORT) {
    txn->Abort();
    return ABORT;
  }
  txn->PreCommit();
  // must finish the commit even when g_terminate_exec is set: PreCommit has
  // already applied the write set to the buffer and appended the log record,
  // and no undo path exists that could roll that back. The old early-return
  // here reported such transactions as aborted while skipping Commit's dirty
  // flagging and lock releases (and leaked the txn), leaving applied-but-
  // unflagged rows behind - caught by the TPC-C consistency check as
  // districts whose cursor was visible remotely but whose last order rows
  // were not. WaitForFlush polls g_terminate_exec, so no shutdown hang.
  auto commit_thd = Worker::AssignCommitThd();
  // commit_thd_pool_.SubmitTask(commit_thd, ARDB::CommitTask, txn);
  CommitTask(txn);
  // txn->Commit();
  // DEALLOC(txn);
  return RC::OK;
}


void ARDB::Terminate(std::vector<std::thread> & threads) {
  g_terminate_exec = true;
  LOG_INFO("Terminating execution.");
  // join commit threads
  commit_thd_pool_.Join();
  LOG_INFO("Joined all commit threads.");
  std::for_each(threads.begin(), threads.end(), std::mem_fn(&std::thread::join));
  LOG_INFO("Joined all worker threads.");
  g_terminate_evict = true;

  if (g_batch_eviction) {
    if (g_buf_type == OBJBUF) {
      ((ObjectBufferManager *)g_buf_mgr)->StopEvictThds();
    } else if (g_buf_type == HYBRBUF) {
      ((ObjectBufferManager *)g_top_buf_mgr)->StopEvictThds();
    } else {
      //TODO: need to implement periodic (batch) eviction for page buffer
    }
    LOG_INFO("Joined all eviction threads.");
  }


  if (g_buf_type == PGBUF) {
    // bufferd_idx_pgs_ is otherwise a thread-0 sample taken whenever thread 0
    // happened to load/evict an index page; publish the live end-of-run
    // count instead so the stat is truthful
    g_stats->db_stats_[0].set_bufferd_idx_pgs_(
        ((PageBufferManager *) g_buf_mgr)->GetIdxPageNum());
  }
  g_stats->SumUp();
  g_stats->Print();
  g_data_store->PrintReadHist();
  // Throttling, reported unconditionally. Without it a 429 backoff is
  // indistinguishable from service latency: the sleep happens inside the
  // client call, so it lands in remote_wr_time_/remote_rd_time_ and reads as
  // "the store is slow". These counters say how much of the time charged to
  // remote I/O is really the retry loop, and which table caused it.
  {
    auto rd = g_thr429_rd.load(), wr = g_thr429_wr.load(), rw = g_thr429_rw.load();
    auto ns = g_thr_backoff_ns.load();
    auto io_ms = g_stats->sum_db_stats_.remote_io_time_ms_;
    LOG_INFO("throttling: 429s rd %lu wr %lu rw %lu, backoff %.1f s of thread "
             "time = %.1f %% of the %.1f s charged to remote_io_time_",
             rd, wr, rw, ns / 1e9,
             io_ms > 0 ? ns / 1e4 / io_ms : 0.0, io_ms / 1000.0);
    for (int i = 0; i < kThrottleSlots; i++) {
      auto c = g_thr429_tbl[i].load();
      if (c) LOG_INFO("throttling: %-9s %2d  429s %lu",
                      i >= 32 ? "IDX of tbl" : "tbl", i >= 32 ? i - 32 : i, c);
    }
    if (g_save_output) {
      g_out_str << "\"thr429_rd_\": " << rd << ",";
      g_out_str << "\"thr429_wr_\": " << wr << ",";
      g_out_str << "\"thr429_rw_\": " << rw << ",";
      g_out_str << "\"thr_backoff_ns_\": " << ns << ",";
    }
  }
  // PGBUF eviction path: the global-mutex queueing that no other counter sees.
  {
    auto ev = g_pgbuf_evicts.load(), st = g_pgbuf_evict_steps.load();
    auto ens = g_pgbuf_evict_ns.load(), lns = g_pgbuf_lockwait_ns.load();
    auto acq = g_pgbuf_lock_acqs.load();
    if (ev || acq) {
      auto worker_ms = g_stats->sum_db_stats_.remote_io_time_ms_;
      LOG_INFO("pgbuf evict: %lu evictions, %.1f clock steps each, %.3f ms each "
               "(%.1f s total scan under the global mutex); %lu mutex acquires, "
               "%.1f s waiting (%.3f ms each)",
               ev, ev ? st * 1.0 / ev : 0.0, ev ? ens / 1e6 / ev : 0.0, ens / 1e9,
               acq, lns / 1e9, acq ? lns / 1e6 / acq : 0.0);
      LOG_INFO("pgbuf evict: for reference remote_io_time_ is %.1f s", worker_ms / 1000.0);
      if (g_save_output) {
        g_out_str << "\"pgbuf_evicts_\": " << ev << ",";
        g_out_str << "\"pgbuf_evict_steps_\": " << st << ",";
        g_out_str << "\"pgbuf_evict_ns_\": " << ens << ",";
        g_out_str << "\"pgbuf_lockwait_ns_\": " << lns << ",";
        g_out_str << "\"pgbuf_lock_acqs_\": " << acq << ",";
      }
    }
  }
  if (g_evict_policy_bench) {
    auto rec = g_evict_records.load(), tasks = g_evict_tasks.load();
    LOG_INFO("evict-policy bench: records evicted %lu, tasks %lu (%.1f rec/task), "
             "task time %.2f us/task, hand steps %lu (%.2f/victim), lock wait %.1f ms total, "
             "lru moves %lu, evict threads %zu",
             rec, tasks, tasks ? rec * 1.0 / tasks : 0.0,
             tasks ? g_evict_task_ns.load() / 1000.0 / tasks : 0.0,
             g_evict_hand_steps.load(), rec ? g_evict_hand_steps.load() * 1.0 / rec : 0.0,
             g_evict_lock_wait_ns.load() / 1e6, g_evict_lru_moves.load(),
             (size_t) g_num_evict_thds);
    if (g_save_output) {
      g_out_str << "\"evict_records_\": " << rec << ",";
      g_out_str << "\"evict_tasks_\": " << tasks << ",";
      g_out_str << "\"evict_task_ns_\": " << g_evict_task_ns.load() << ",";
      g_out_str << "\"evict_hand_steps_\": " << g_evict_hand_steps.load() << ",";
      g_out_str << "\"evict_lock_wait_ns_\": " << g_evict_lock_wait_ns.load() << ",";
      g_out_str << "\"evict_lru_moves_\": " << g_evict_lru_moves.load() << ",";
    }
  }
  // The eviction write-back, reported unconditionally: rows pushed to remote
  // storage and the thread-time spent inside those writes. No other counter
  // covers this path -- BatchStoreSync/WriteBatchAsync do not add to
  // remote_io_time_, so without these a run can look I/O-light while the
  // write-back is setting the throughput ceiling. Deliberately outside the
  // g_evict_policy_bench gate: that flag also changes buffer accounting,
  // so ordinary measurement runs must be able to read these without it.
  {
    auto wr = g_evict_wr_rows.load();
    if (g_save_output) {
      g_out_str << "\"evict_wr_rows_\": " << wr << ",";
      g_out_str << "\"evict_wr_ns_\": " << g_evict_wr_ns.load() << ",";
      g_out_str << "\"batch_write_fanout_\": " << g_batch_write_fanout << ",";
      g_out_str << "\"null_tuple_skips_\": " << g_null_tuple_skips.load() << ",";
    }
    if (wr) {
      LOG_INFO("eviction write-back: %lu rows, %.1f s of thread time, "
               "%.2f ms/row, fanout %zu", wr, g_evict_wr_ns.load() / 1e9,
               g_evict_wr_ns.load() / 1e6 / wr, (size_t) g_batch_write_fanout);
    }
  }
  // Range-scan buffer coverage: whether the rows a scan fetches are genuinely
  // absent from the buffer or whether the fetch refetches rows the buffer
  // already holds -- i.e. whether issuing the exact holes (or per-key point
  // reads) would beat one bounding range query.
  {
    auto n = g_scan_calls.load();
    if (n) {
      auto buf = g_scan_rows_buf.load(), fet = g_scan_rows_fetched.load(),
           wst = g_scan_rows_wasted.load();
      auto tot = buf + fet - wst;
      LOG_INFO("scan coverage: %lu scans | from buffer %lu (%.1f%% of useful) | "
               "fetched %lu, of which ALREADY cached %lu (%.1f%% of fetched) | "
               "useful %lu | per scan: buf %.1f fetched %.1f wasted %.1f",
               n, buf, tot ? buf * 100.0 / tot : 0.0, fet, wst,
               fet ? wst * 100.0 / fet : 0.0, tot,
               buf * 1.0 / n, fet * 1.0 / n, wst * 1.0 / n);
      LOG_INFO("scan gaps: %.2f true holes/scan -> %.2f remote queries/scan "
               "(g_scan_gap_merge = %zu resident keys)",
               g_scan_gaps_raw.load() * 1.0 / n, g_scan_gaps_merged.load() * 1.0 / n,
               (size_t) g_scan_gap_merge);
      auto egt = g_egt_unparsed.load();
      if (egt)
        LOG_INFO("EGT: %lu batches executed by Cosmos (202) whose multipart "
                 "response the SDK could not parse -- swallowed by design", egt);
      auto fails = g_scan_query_fail.load();
      if (fails)
        LOG_INFO("**SCAN QUERIES FAILED: %lu** -- each returned an empty range "
                 "to its transaction; this run is not a valid measurement", fails);
      if (g_save_output) {
        g_out_str << "\"scan_calls_\": " << n << ",";
        g_out_str << "\"scan_rows_buf_\": " << buf << ",";
        g_out_str << "\"scan_rows_fetched_\": " << fet << ",";
        g_out_str << "\"scan_rows_wasted_\": " << wst << ",";
        g_out_str << "\"scan_gaps_raw_\": " << g_scan_gaps_raw.load() << ",";
        g_out_str << "\"scan_gaps_merged_\": " << g_scan_gaps_merged.load() << ",";
        g_out_str << "\"scan_query_fail_\": " << g_scan_query_fail.load() << ",";
      }
    }
  }
  // B-tree node latch accounting. Latch waits land in no other counter
  // (cc_time_ covers the lock table, not node latches), so a latch-bound
  // run would otherwise read as CPU-bound.
  {
    auto exc = g_latch_ex_cnt.load(), shc = g_latch_sh_cnt.load();
    if (g_save_output) {
      g_out_str << "\"latch_ex_cnt_\": " << exc << ",";
      g_out_str << "\"latch_sh_cnt_\": " << shc << ",";
      g_out_str << "\"latch_wait_ns_ex_\": " << g_latch_wait_ns_ex.load() << ",";
      g_out_str << "\"latch_wait_ns_sh_\": " << g_latch_wait_ns_sh.load() << ",";
      g_out_str << "\"latch_hold_ns_ex_\": " << g_latch_hold_ns_ex.load() << ",";
    }
    if (exc || shc) {
      LOG_INFO("latch: EX %lu acq, wait %.1f s (%.1f us/acq), hold %.1f s "
               "(%.1f us/acq) | SH %lu acq, wait %.1f s (%.1f us/acq)",
               exc, g_latch_wait_ns_ex.load() / 1e9,
               exc ? g_latch_wait_ns_ex.load() / 1e3 / exc : 0.0,
               g_latch_hold_ns_ex.load() / 1e9,
               exc ? g_latch_hold_ns_ex.load() / 1e3 / exc : 0.0,
               shc, g_latch_wait_ns_sh.load() / 1e9,
               shc ? g_latch_wait_ns_sh.load() / 1e3 / shc : 0.0);
    }
  }
  if (g_buf_type == PGBUF && (g_pgbuf_debug || g_pgbuf_census))
    ((PageBufferManager *) g_buf_mgr)->PrintDebugCounters();
  if (g_buf_type == OBJBUF && g_pgbuf_debug) {
    // record-buffer census: real resident bytes vs the nominal budget
    // (is the OBJBUF slot accounting size-blind?)
    size_t tot_reg = 0, tot_tomb = 0, tot_sent = 0, tot_bytes = 0;
    for (auto tbl : tables_) {
      auto idx = (IndexBTreeObject *) tbl->GetPrimaryIdx();
      if (!idx) continue;
      size_t reg, tomb, sent, bytes;
      idx->CensusResident(tbl->GetTotalTupleSize(), reg, tomb, sent, bytes);
      if (reg + tomb + sent)
        LOG_INFO("objbuf census: tbl %2u tuple_sz %4zu regular %9zu tombstone %7zu "
                 "sentinel %5zu bytes %12zu | write-once %8zu dirty %8zu ref-set %8zu",
                 tbl->GetTableId(), tbl->GetTotalTupleSize(), reg, tomb, sent, bytes,
                 idx->census_write_once_, idx->census_dirty_, idx->census_ref_);
      tot_reg += reg; tot_tomb += tomb; tot_sent += sent; tot_bytes += bytes;
    }
    auto obm = (ObjectBufferManager *) g_buf_mgr;
    LOG_INFO("objbuf census: TOTAL regular %zu tombstone %zu sentinel %zu -> resident "
             "record bytes %zu vs budget g_total_buf_sz %zu (%.2fx); accounted "
             "slots %zu of %zu (entry sz %zu); index-node bytes %zu",
             tot_reg, tot_tomb, tot_sent, tot_bytes, (size_t) g_total_buf_sz,
             tot_bytes * 1.0 / g_total_buf_sz, obm->GetAllocated(),
             obm->GetBufferSize(), (size_t) g_buf_entry_sz, obm->GetOtherAllocated());
    if (g_save_output) {
      g_out_str << "\"objbuf_resident_records_\": " << tot_reg << ",";
      g_out_str << "\"objbuf_resident_bytes_\": " << tot_bytes << ",";
      g_out_str << "\"objbuf_index_bytes_\": " << obm->GetOtherAllocated() << ",";
    }
  }
}

void ARDB::RestoreTable(OID tbl_id,  uint64_t num_rows, uint64_t data_domain_sz) {
  tables_[tbl_id]->RestoreTable(num_rows, data_domain_sz);
}

} // arboretum