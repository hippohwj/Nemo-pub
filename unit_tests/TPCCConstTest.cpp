#include "gtest/gtest.h"
#include "tpcc/TPCCConst.h"

using namespace arboretum;
using namespace arboretum::tpcc;

TEST(TPCCConstTest, KeyDomainsFitEightDecimalDigits) {
  g_num_wh = 104; // documented maximum with O_CAP = 6000
  for (OID t = 0; t < T_TABLE_CNT; t++) {
    EXPECT_LT(TableKeyDomain(t), 100000000UL) << "table " << t;
  }
  g_num_wh = 10;
}

TEST(TPCCConstTest, KeyEncodingIsInjectivePerTable) {
  // spot-check decode(encode) round trips
  for (uint64_t w = 0; w < g_num_wh; w += 3) {
    for (uint64_t d = 0; d < DIST_PER_WH; d++) {
      auto did = Did(w, d);
      EXPECT_EQ(did / DIST_PER_WH, w);
      EXPECT_EQ(did % DIST_PER_WH, d);
      auto ck = CustKey(w, d, 2999);
      EXPECT_EQ(ck / CUST_PER_DIST, did);
      EXPECT_EQ(ck % CUST_PER_DIST, 2999UL);
      auto ok = OrderKey(did, g_o_cap - 1);
      EXPECT_EQ(ok / g_o_cap, did);
      auto olk = OlKey(ok, 15);
      EXPECT_EQ(olk / MAX_OL_PER_ORDER, ok);
      EXPECT_EQ(olk % MAX_OL_PER_ORDER, 15UL);
    }
  }
}

TEST(TPCCConstTest, OrderLineScanBlocksNeverCrossPartitions) {
  // 16 divides the 10000-key storage partition, so a single order's
  // order-line block [okey*16, okey*16+15] stays in one partition
  constexpr uint64_t kPartition = 10000;
  for (uint64_t okey = 0; okey < 2000000; okey += 331) {
    auto lo = OlKey(okey, 0);
    auto hi = OlKey(okey, MAX_OL_PER_ORDER - 1);
    EXPECT_EQ(lo / kPartition, hi / kPartition) << "okey " << okey;
  }
}

TEST(TPCCConstTest, OlCntWithinSpecRange) {
  uint64_t counts[16] = {0};
  for (uint64_t did = 0; did < 100; did++) {
    for (uint64_t o = 0; o < 6000; o += 7) {
      auto c = OlCnt(did, o);
      ASSERT_GE(c, 5UL);
      ASSERT_LE(c, 15UL);
      counts[c]++;
    }
  }
  // rough uniformity: every value in [5, 15] occurs
  for (int c = 5; c <= 15; c++) EXPECT_GT(counts[c], 0UL);
}

TEST(TPCCConstTest, LastnameCoversAllNamesAndStaysInRange) {
  uint64_t did = 42;
  for (uint64_t c = 0; c < NUM_LASTNAMES; c++) {
    EXPECT_EQ(LastnameOf(did, c), c); // first 1000 customers cover each name
  }
  for (uint64_t c = NUM_LASTNAMES; c < CUST_PER_DIST; c++) {
    EXPECT_LT(LastnameOf(did, c), NUM_LASTNAMES);
  }
}

TEST(TPCCConstTest, InitOrderPermutationIsBijectiveWithInverse) {
  for (uint64_t did = 0; did < 20; did++) {
    std::vector<bool> seen(CUST_PER_DIST, false);
    for (uint64_t o = 0; o < CUST_PER_DIST; o++) {
      auto c = InitOrderCust(did, o);
      ASSERT_LT(c, CUST_PER_DIST);
      ASSERT_FALSE(seen[c]) << "duplicate customer " << c;
      seen[c] = true;
      ASSERT_EQ(InitCustLastOrder(did, c), o);
    }
  }
}

TEST(TPCCConstTest, ByNameBucketsBoundedBySlots) {
  // NURand skew must not overflow the slots per (district, lastname);
  // covers the full 104-warehouse district domain
  for (uint64_t did = 0; did < 1040; did++) {
    uint64_t bucket[NUM_LASTNAMES] = {0};
    for (uint64_t c = 0; c < CUST_PER_DIST; c++) {
      bucket[LastnameOf(did, c)]++;
    }
    for (uint64_t ln = 0; ln < NUM_LASTNAMES; ln++) {
      ASSERT_GE(bucket[ln], 1UL);
      ASSERT_LE(bucket[ln], BYNAME_SLOTS) << "did " << did << " ln " << ln;
    }
  }
}

TEST(TPCCConstTest, TableRowCountsMatchScale) {
  EXPECT_EQ(TableRowCnt(T_WAREHOUSE), g_num_wh);
  EXPECT_EQ(TableRowCnt(T_CUSTOMER), g_num_wh * DIST_PER_WH * CUST_PER_DIST);
  EXPECT_EQ(TableRowCnt(T_ORDER_LINE),
            g_num_wh * DIST_PER_WH * g_o_cap * MAX_OL_PER_ORDER);
  EXPECT_EQ(TableRowCnt(T_ITEM), MAX_ITEMS);
  EXPECT_EQ(TableRowCnt(T_STOCK), g_num_wh * MAX_ITEMS);
}
