#include <cstdint>

#include "gtest/gtest.h"
#include "merodis/merodis.h"
#include "common.h"

class ListTest : public RedisTest {};

TEST_F(ListTest, LPush) {
  uint64_t len;
  std::string value;
  s = db.LLen("key", &len);
  ASSERT_EQ(s.ok(), true);
  ASSERT_EQ(len, 0);

  // ["0"] => [""]
  s = db.LPush("key", "0");
  ASSERT_EQ(s.ok(), true);
  s = db.LLen("key", &len);
  ASSERT_EQ(s.ok(), true);
  ASSERT_EQ(len, 1);
  s = db.LIndex("key", 0, &value);
  ASSERT_EQ(value, "0");

  // ["0"] => ["1", "0"]
  s = db.LPush("key", "1");
  ASSERT_EQ(s.ok(), true);
  s = db.LLen("key", &len);
  ASSERT_EQ(s.ok(), true);
  ASSERT_EQ(len, 2);
  s = db.LIndex("key", 0, &value);
  ASSERT_EQ(s.ok(), true);
  ASSERT_EQ(value, "1");
  s = db.LIndex("key", 1, &value);
  ASSERT_EQ(s.ok(), true);
  ASSERT_EQ(value, "0");
  s = db.LIndex("key", 2, &value);
  ASSERT_EQ(s.IsNotFound(), true);
}
