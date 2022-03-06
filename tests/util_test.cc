#include <cerrno>
#include <cstdint>

#include "gtest/gtest.h"

#include "util/sequence.h"
#include "util/number.h"

class UtilTest : public testing::Test {
public:
  void DoComputeMovedSteps(uint64_t start,
                           uint64_t end,
                           const std::vector<uint64_t>& removedIndices,
                           uint64_t expectedStart,
                           uint64_t expectedEnd,
                           const std::vector<Block>& expectedBlocks) {
    ASSERT_EQ(GetBlocks(start, end, removedIndices), expectedBlocks);
    EXPECT_EQ(start, expectedStart);
    EXPECT_EQ(end, expectedEnd);
  }
};

TEST_F(UtilTest, TestComputeMovedSteps) {
  DoComputeMovedSteps(1, 10, {3, 7},
                      2, 9, {{1, 2, 1}, {4, 6, 0}, {8, 10, -1}});
  DoComputeMovedSteps(1, 10, {1},
                      2, 10, {{2, 10, 0}});
  DoComputeMovedSteps(1, 10, {10},
                      1, 9, {{1, 9, 0}});
  DoComputeMovedSteps(1, 10, {1, 5, 10},
                      3, 9, {{2, 4, 1}, {6, 9, 0}});
  DoComputeMovedSteps(1, 5, {1, 2, 4, 5},
                      3, 3, {{3, 3}});
  DoComputeMovedSteps(1, 5, {1, 2, 3, 4, 5},
                      6, 5, {});
  DoComputeMovedSteps(1, 100, {1, 2, 4, 6, 7, 8, 20, 50, 60, 99, 100},
                      10, 98,  {{3, 3, 7}, {5, 5, 6}, {9, 19, 3}, {21, 49, 2}, {51, 59, 1}, {61, 98, 0}});
}

TEST_F(UtilTest, Slice2Int64) {
  int64_t n;
  ASSERT_EQ(SliceToInt64("123", n), 0);
  ASSERT_EQ(n, 123);
  ASSERT_EQ(SliceToInt64("0", n), 0);
  ASSERT_EQ(n, 0);
  ASSERT_EQ(SliceToInt64("-123", n), 0);
  ASSERT_EQ(n, -123);
  ASSERT_EQ(SliceToInt64("9223372036854775807", n), 0);
  ASSERT_EQ(n, 9223372036854775807);
  ASSERT_EQ(SliceToInt64("9223372036854775808", n), ERANGE);
  ASSERT_EQ(n, 0);
  ASSERT_EQ(SliceToInt64("-9223372036854775808", n), 0);
  ASSERT_EQ(n, -9223372036854775808U);
  ASSERT_EQ(SliceToInt64("-9223372036854775809", n), ERANGE);
  ASSERT_EQ(n, 0);
  ASSERT_EQ(SliceToInt64("", n), EINVAL);
  ASSERT_EQ(n, 0);
  ASSERT_EQ(SliceToInt64("1xx", n), EINVAL);
  ASSERT_EQ(n, 0);
  ASSERT_EQ(SliceToInt64("1.2", n), EINVAL);
  ASSERT_EQ(n, 0);
}