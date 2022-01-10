#include "gtest/gtest.h"
#include "util/sequence.h"

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
