#ifndef MERODIS_TESTUTIL_H
#define MERODIS_TESTUTIL_H

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace merodis {
namespace test {

MATCHER(IsOK, "") { return arg.ok(); }
MATCHER(IsNotFound, "") { return arg.IsNotFound(); }
MATCHER(IsInvalidArgument, "") { return arg.IsInvalidArgument(); }
MATCHER(IsNotSupported, "") { return arg.IsNotSupportedError(); }

#define EXPECT_MERODIS_OK(expression) \
  EXPECT_THAT(expression, merodis::test::IsOK())
#define ASSERT_MERODIS_OK(expression) \
  ASSERT_THAT(expression, merodis::test::IsOK())
#define EXPECT_MERODIS_IS_NOT_FOUND(expression) \
  EXPECT_THAT(expression, merodis::test::IsNotFound())
#define ASSERT_MERODIS_IS_NOT_FOUND(expression) \
  ASSERT_THAT(expression, merodis::test::IsNotFound())
#define EXPECT_MERODIS_IS_INVALID_ARGUMENT(expression) \
  EXPECT_THAT(expression, merodis::test::IsInvalidArgument())
#define ASSERT_MERODIS_IS_INVALID_ARGUMENT(expression) \
  ASSERT_THAT(expression, merodis::test::IsInvalidArgument())
#define EXPECT_MERODIS_IS_NOT_SUPPORTED(expression) \
  EXPECT_THAT(expression, merodis::test::IsNotSupported())
#define ASSERT_MERODIS_IS_NOT_SUPPORTED(expression) \
  ASSERT_THAT(expression, merodis::test::IsNotSupported())

#define LIST(...) \
  std::vector<std::string>({ __VA_ARGS__ })
#define SET(...) \
  std::set<std::string>({ __VA_ARGS__ })
#define UINTS(...) \
  std::vector<uint64_t>({ __VA_ARGS__ })
#define BOOLEANS(...) \
  std::vector<bool>({ __VA_ARGS__ })
#define KVS(...) \
  (std::map<std::string, std::string>({ __VA_ARGS__ }))
#define PAIR(s, i) \
  (std::pair<std::string, int64_t>({ s, i }))
#define PAIRS(...) \
  (std::vector<std::pair<std::string, int64_t>>({ __VA_ARGS__ }))
}
}

#endif //MERODIS_TESTUTIL_H
