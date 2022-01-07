#ifndef MERODIS_TESTUTIL_H
#define MERODIS_TESTUTIL_H

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace merodis {
namespace test {

MATCHER(IsOK, "") { return arg.ok(); }
MATCHER(IsNotFound, "") { return arg.IsNotFound(); }

#define EXPECT_MERODIS_OK(expression) \
  EXPECT_THAT(expression, merodis::test::IsOK())
#define ASSERT_MERODIS_OK(expression) \
  ASSERT_THAT(expression, merodis::test::IsOK())
#define EXPECT_MERODIS_ISNOTFOUND(expression) \
  EXPECT_THAT(expression, merodis::test::IsNotFound())
#define ASSERT_MERODIS_ISNOTFOUND(expression) \
  ASSERT_THAT(expression, merodis::test::IsNotFound())

}
}

#endif //MERODIS_TESTUTIL_H
