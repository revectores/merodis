#ifndef MERODIS_TESTUTIL_H
#define MERODIS_TESTUTIL_H

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace merodis {
namespace test {

MATCHER(IsOK, "") { return arg.ok(); }
MATCHER(IsNotFound, "") { return arg.IsNotFound(); }
MATCHER(IsInvalidArgument, "") { return arg.IsInvalidArgument(); }

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

}
}

#endif //MERODIS_TESTUTIL_H
