#include <cstdio>

#include "gtest/gtest.h"
#include "merodis/merodis.h"
#include "common.h"


class StringTest : public RedisTest {};

TEST_F(StringTest, StringRW) {
  s = db.Set("key", "value");
  ASSERT_EQ(s.ok(), true);
  std::string value;
  s = db.Get("key", &value);
  ASSERT_EQ(s.ok(), true);
  EXPECT_EQ(value, "value");
}
