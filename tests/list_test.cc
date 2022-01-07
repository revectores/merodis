#include <cstdint>

#include "gtest/gtest.h"
#include "merodis/merodis.h"
#include "common.h"
#include "testutil.h"

class ListTest : public RedisTest {};

TEST_F(ListTest, LPush) {
  uint64_t len;
  std::string value;
  ASSERT_MERODIS_OK(db.LLen("key", &len));
  ASSERT_EQ(len, 0);

  // ["0"] => [""]
  ASSERT_MERODIS_OK(db.LPush("key", "0"));
  ASSERT_MERODIS_OK(db.LLen("key", &len));
  ASSERT_EQ(len, 1);
  ASSERT_MERODIS_OK(db.LIndex("key", 0, &value));
  ASSERT_EQ(value, "0");

  // ["0"] => ["1", "0"]
  ASSERT_MERODIS_OK(db.LPush("key", "1"));
  ASSERT_MERODIS_OK(db.LLen("key", &len));
  ASSERT_EQ(len, 2);
  ASSERT_MERODIS_OK(db.LIndex("key", 0, &value));
  ASSERT_EQ(value, "1");
  ASSERT_MERODIS_OK(db.LIndex("key", 1, &value));
  ASSERT_EQ(value, "0");
  ASSERT_MERODIS_OK(db.LIndex("key", -1, &value));
  ASSERT_EQ(value, "0");
  ASSERT_MERODIS_OK(db.LIndex("key", -2, &value));
  ASSERT_EQ(value, "1");
  ASSERT_MERODIS_ISNOTFOUND(db.LIndex("key", 2, &value));
}
