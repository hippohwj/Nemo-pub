#include "gtest/gtest.h"
#include "common/Common.h"
#include "local/ITuple.h"
#include "remote/IDataStore.h"
#include "remote/SimNetClient.h"
#include "remote/SimNetProtocol.h"
#include "remote/SimNetServer.h"
#include "remote/SimTableClient.h"

#include <atomic>
#include <chrono>
#include <filesystem>

using namespace arboretum;

namespace {

std::string Key(uint64_t k) { return SearchKey(k).ToString(); }

std::string Payload(uint64_t k, size_t sz = 32) {
  std::string s = "value-" + std::to_string(k) + "-";
  while (s.size() < sz) s += 'x';
  return s.substr(0, sz);
}

} // namespace

// every test runs in both modes: inproc = SimTableClient (the published
// path), net = SimNetClient against an embedded SimNetServer on a free port
// (one fresh server per test, like one fresh server per experiment run)
class SimStoreModeTest : public ::testing::TestWithParam<bool> {
 protected:
  void SetUp() override {
    net_ = GetParam();
    StartStore();
  }

  void TearDown() override {
    delete client_;
    if (server_ != nullptr) {
      server_->Stop();
      delete server_;
      server_ = nullptr;
    }
    g_sim_server_addr[0] = '\0';
  }

  // drops all in-memory state, keeping only snapshot files — the analog of
  // a new experiment run
  void RestartStore() {
    delete client_;
    if (server_ != nullptr) {
      server_->Stop();
      delete server_;
      server_ = nullptr;
    }
    StartStore();
  }

  ISimStoreClient *client_{nullptr};
  SimNetServer *server_{nullptr};
  bool net_{false};

 private:
  void StartStore() {
    if (net_) {
      server_ = new SimNetServer();
      auto port = server_->Start(0);
      snprintf(g_sim_server_addr, sizeof(g_sim_server_addr), "127.0.0.1:%d",
               port);
      client_ = new SimNetClient();
    } else {
      g_sim_server_addr[0] = '\0';
      client_ = new SimTableClient();
    }
  }
};

INSTANTIATE_TEST_SUITE_P(Modes, SimStoreModeTest,
                         ::testing::Values(false, true),
                         [](const ::testing::TestParamInfo<bool> &info) {
                           return info.param ? std::string("net")
                                             : std::string("inproc");
                         });

TEST_P(SimStoreModeTest, PointReadWrite) {
  auto &client = *client_;
  client.CreateTable("t1");
  auto v = Payload(7);
  client.StoreSync("t1", "0", Key(7), v.data(), v.size());
  std::string out;
  EXPECT_EQ(client.LoadSync("t1", "0", Key(7), out), RC::OK);
  EXPECT_EQ(out, v);
  // missing key, missing partition, and never-created table all miss cleanly
  EXPECT_EQ(client.LoadSync("t1", "0", Key(8), out), RC::NOT_FOUND);
  EXPECT_EQ(client.LoadSync("t1", "9", Key(7), out), RC::NOT_FOUND);
  EXPECT_EQ(client.LoadSync("nvr", "0", Key(7), out), RC::NOT_FOUND);
  // overwrite
  auto v2 = Payload(7, 64);
  client.StoreSync("t1", "0", Key(7), v2.data(), v2.size());
  EXPECT_EQ(client.LoadSync("t1", "0", Key(7), out), RC::OK);
  EXPECT_EQ(out, v2);
}

TEST_P(SimStoreModeTest, NumericMetadata) {
  auto &client = *client_;
  client.CreateTable("t1");
  client.StoreNumericSync("t1", "metadata", "row_cnt_", 424242);
  EXPECT_EQ(client.LoadNumericSync("t1", "metadata", "row_cnt_"), 424242UL);
}

TEST_P(SimStoreModeTest, StoreAndLoad) {
  auto &client = *client_;
  client.CreateTable("t1");
  auto v1 = Payload(1);
  client.StoreSync("t1", "0", Key(1), v1.data(), v1.size());
  auto v2 = Payload(2);
  std::string out;
  client.StoreAndLoadSync("t1", "0", Key(2), v2.data(), v2.size(),
                          "0", Key(1), out);
  EXPECT_EQ(out, v1);
  EXPECT_EQ(client.LoadSync("t1", "0", Key(2), out), RC::OK);
  EXPECT_EQ(out, v2);
}

TEST_P(SimStoreModeTest, BatchAndAsync) {
  auto &client = *client_;
  client.CreateTable("t1");
  std::multimap<std::string, std::string> batch;
  for (uint64_t k = 100; k < 110; k++) batch.emplace(Key(k), Payload(k));
  client.BatchStoreSync("t1", "0", batch);
  std::string out;
  for (uint64_t k = 100; k < 110; k++) {
    ASSERT_EQ(client.LoadSync("t1", "0", Key(k), out), RC::OK);
    EXPECT_EQ(out, Payload(k));
  }

  // async batch with completion flag
  std::multimap<std::string, std::string> batch2;
  for (uint64_t k = 200; k < 210; k++) batch2.emplace(Key(k), Payload(k));
  volatile bool is_done = false;
  client.BatchStoreAsync("t1", "0", batch2, is_done);
  while (!is_done) usleep(100);
  ASSERT_EQ(client.LoadSync("t1", "0", Key(205), out), RC::OK);

  // async batch with callback (the batched-eviction pattern)
  std::multimap<std::string, std::string> batch3;
  for (uint64_t k = 300; k < 310; k++) batch3.emplace(Key(k), Payload(k));
  std::atomic<int> cb_cnt(0);
  client.WriteBatchAsync("t1", "0", batch3, [&cb_cnt] { cb_cnt++; });
  while (cb_cnt.load() == 0) usleep(100);
  ASSERT_EQ(client.LoadSync("t1", "0", Key(303), out), RC::OK);

  // async single write with RC flag (the log-force pattern)
  auto v = Payload(400);
  volatile RC rc = RC::PENDING;
  client.StoreAsync("t1", "0", Key(400), v.data(), v.size(), &rc);
  while (rc != RC::OK) usleep(100);
  ASSERT_EQ(client.LoadSync("t1", "0", Key(400), out), RC::OK);
  EXPECT_EQ(out, v);
}

TEST_P(SimStoreModeTest, RangeScanBufferLayout) {
  auto &client = *client_;
  client.CreateTable("t1");
  const size_t payload_sz = 40;
  const size_t total_tuple_sz = sizeof(ITuple) + payload_sz;
  // sparse keys: only even keys in [10, 30) exist
  for (uint64_t k = 10; k < 30; k += 2) {
    auto v = Payload(k, payload_sz);
    client.StoreSync("t1", "0", Key(k), v.data(), v.size());
  }
  std::string tbl = "t1", part = "0";
  char *tuples = nullptr;
  size_t res_cnt = 0;
  client.LoadRangePython(tbl, part, Key(12), Key(20), &tuples, total_tuple_sz,
                         res_cnt);
  // keys 12, 14, 16, 18, 20
  ASSERT_EQ(res_cnt, 5UL);
  ASSERT_NE(tuples, nullptr);
  for (size_t i = 0; i < res_cnt; i++) {
    auto expect = Payload(12 + 2 * i, payload_sz);
    EXPECT_EQ(std::string(tuples + i * total_tuple_sz + sizeof(ITuple),
                          payload_sz), expect);
  }
  DEALLOC(tuples);

  // empty range
  char *empty = nullptr;
  res_cnt = 42;
  client.LoadRangePython(tbl, part, Key(31), Key(99), &empty, total_tuple_sz,
                         res_cnt);
  EXPECT_EQ(res_cnt, 0UL);
  EXPECT_EQ(empty, nullptr);
}

TEST_P(SimStoreModeTest, LatencyInjection) {
  auto &client = *client_;
  client.CreateTable("t1");
  auto v = Payload(1);
  client.StoreSync("t1", "0", Key(1), v.data(), v.size());
  g_sim_rd_latency_us = 2000;
  const int trials = 5;
  std::string out;
  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < trials; i++) {
    client.LoadSync("t1", "0", Key(1), out);
  }
  auto us = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now() - start).count() / trials;
  g_sim_rd_latency_us = 0;
  EXPECT_GE(us, 2000);
  EXPECT_LT(us, 4000); // generous upper bound for loaded CI machines
}

TEST_P(SimStoreModeTest, SnapshotRoundTrip) {
  auto dir = std::filesystem::temp_directory_path() / "simstore_test_snap";
  std::filesystem::remove_all(dir);
  strcpy(g_sim_snapshot_dir, dir.c_str());
  {
    auto &writer = *client_;
    writer.CreateTable("snap-0");
    writer.CreateTable("snap-log");
    for (uint64_t k = 0; k < 100; k++) {
      auto v = Payload(k);
      writer.StoreSync("snap-0", std::to_string(k / 10), Key(k), v.data(),
                       v.size());
    }
    writer.StoreNumericSync("snap-0", "metadata", "row_cnt_", 100);
    auto lv = Payload(1);
    writer.StoreSync("snap-log", "0", Key(1), lv.data(), lv.size());
    writer.Checkpoint();
  }
  EXPECT_TRUE(std::filesystem::exists(dir / "snap-0.simtbl"));
  // log tables are not snapshotted
  EXPECT_FALSE(std::filesystem::exists(dir / "snap-log.simtbl"));

  RestartStore();
  auto &reader = *client_;
  std::string out;
  for (uint64_t k = 0; k < 100; k++) {
    ASSERT_EQ(reader.LoadSync("snap-0", std::to_string(k / 10), Key(k), out),
              RC::OK) << "key " << k;
    EXPECT_EQ(out, Payload(k));
  }
  EXPECT_EQ(reader.LoadNumericSync("snap-0", "metadata", "row_cnt_"), 100UL);
  EXPECT_EQ(reader.LoadSync("snap-log", "0", Key(1), out), RC::NOT_FOUND);
  std::filesystem::remove_all(dir);
  strcpy(g_sim_snapshot_dir, "simstore_snapshots");
}

// hammer sync ops from many threads plus async batches on the shared
// channel: catches races in the connection handling / reply multiplexing
TEST_P(SimStoreModeTest, ConcurrentMixedOps) {
  auto &client = *client_;
  client.CreateTable("t1");
  const int thds = 8, ops = 50;
  std::vector<std::thread> workers;
  std::atomic<int> cb_cnt(0);
  for (int t = 0; t < thds; t++) {
    workers.emplace_back([&client, &cb_cnt, t] {
      for (int i = 0; i < ops; i++) {
        uint64_t k = (uint64_t) t * 1000 + i;
        auto v = Payload(k);
        client.StoreSync("t1", std::to_string(t), Key(k), v.data(), v.size());
        std::string out;
        ASSERT_EQ(client.LoadSync("t1", std::to_string(t), Key(k), out),
                  RC::OK);
        EXPECT_EQ(out, v);
        if (i % 10 == 0) {
          std::multimap<std::string, std::string> batch;
          for (uint64_t b = 0; b < 5; b++) {
            auto key = (uint64_t) t * 1000 + 500 + i + b;
            batch.emplace(Key(key), Payload(key));
          }
          client.WriteBatchAsync("t1", std::to_string(t), batch,
                                 [&cb_cnt] { cb_cnt++; });
        }
      }
    });
  }
  for (auto &w : workers) w.join();
  while (cb_cnt.load() < thds * (ops / 10)) usleep(100);
  std::string out;
  for (int t = 0; t < thds; t++) {
    ASSERT_EQ(client.LoadSync("t1", std::to_string(t),
                              Key((uint64_t) t * 1000 + 500 + 40), out),
              RC::OK);
  }
}

// cross-partition range reads go through IDataStore::ReadRange, which splits
// at g_partition_sz boundaries and merges the per-partition results; in net
// mode this exercises the real IDataStore -> SimNetClient dispatch
TEST_P(SimStoreModeTest, DataStoreCrossPartitionRange) {
  // deleted before TearDown stops the server: a net-mode client must not
  // outlive the server it is connected to
  std::unique_ptr<IDataStore> store(new IDataStore());
  const size_t payload_sz = 24;
  const size_t total_tuple_sz = sizeof(ITuple) + payload_sz;
  // g_partition_sz defaults to 10000: keys 9995..10004 straddle the boundary
  for (uint64_t k = 9995; k < 10005; k++) {
    auto v = Payload(k, payload_sz);
    SearchKey key(k);
    store->Write("xp", key, v.data(), v.size());
  }
  char *tuples = nullptr;
  size_t res_cnt = 0;
  store->ReadRange("xp", SearchKey(9995), SearchKey(10004), &tuples,
                   total_tuple_sz, res_cnt);
  ASSERT_EQ(res_cnt, 10UL);
  ASSERT_NE(tuples, nullptr);
  for (size_t i = 0; i < res_cnt; i++) {
    EXPECT_EQ(std::string(tuples + i * total_tuple_sz + sizeof(ITuple),
                          payload_sz), Payload(9995 + i, payload_sz));
  }
  DEALLOC(tuples);
}

// server-side EGT enforcement (g_sim_enforce_batch_limit): a >100-row batch
// is rejected wholesale with ST_BATCH_TOO_LARGE, like real Cosmos; asserted
// over the raw protocol because the client turns the status into LOG_ERROR
TEST(SimNetServerTest, BatchLimitEnforced) {
  using namespace simnet;
  SimNetServer server;
  auto port = server.Start(0);
  g_sim_enforce_batch_limit = true;

  snprintf(g_sim_server_addr, sizeof(g_sim_server_addr), "127.0.0.1:%d", port);
  SimNetClient probe; // for HELLO-validated setup + verification reads
  probe.CreateTable("egt");

  struct sockaddr_in addr {};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons((uint16_t) port);
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_EQ(connect(fd, (struct sockaddr *) &addr, sizeof(addr)), 0);

  auto send_batch = [&](uint32_t rows, uint64_t base) {
    std::string req;
    PutStr(req, "egt");
    PutStr(req, "0");
    PutU32(req, rows);
    for (uint32_t i = 0; i < rows; i++) {
      PutStr(req, Key(base + i));
      PutStr(req, Payload(base + i));
    }
    EXPECT_TRUE(SendFrame(fd, base, OP_BATCH_STORE, req));
    uint64_t id;
    uint8_t status;
    std::string resp;
    EXPECT_TRUE(RecvFrame(fd, id, status, resp));
    EXPECT_EQ(id, base);
    return status;
  };

  EXPECT_EQ(send_batch(101, 1000), ST_BATCH_TOO_LARGE);
  std::string out;
  // rejected wholesale: nothing was applied
  EXPECT_EQ(probe.LoadSync("egt", "0", Key(1000), out), RC::NOT_FOUND);
  // exactly at the limit passes
  EXPECT_EQ(send_batch(100, 2000), (uint8_t) ST_OK);
  EXPECT_EQ(probe.LoadSync("egt", "0", Key(2099), out), RC::OK);
  // with enforcement off (the default), oversized batches pass as before
  g_sim_enforce_batch_limit = false;
  EXPECT_EQ(send_batch(101, 3000), (uint8_t) ST_OK);
  EXPECT_EQ(probe.LoadSync("egt", "0", Key(3100), out), RC::OK);

  close(fd);
  g_sim_server_addr[0] = '\0';
  // scope exit destroys probe before server: a client must never outlive
  // the server, or its receiver thread treats the closed channel as fatal
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
