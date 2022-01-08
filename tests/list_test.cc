#include <cstdint>
#include <vector>

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

TEST_F(ListTest, LRange) {
  ASSERT_MERODIS_OK(db.LPush("key", "2"));
  ASSERT_MERODIS_OK(db.LPush("key", "1"));
  ASSERT_MERODIS_OK(db.LPush("key", "0"));
  std::vector<std::string> values;
  ASSERT_MERODIS_OK(db.LRange("key", 0, 0, &values));
  ASSERT_EQ(values, std::vector<std::string>({"0"}));
  values.clear();
  ASSERT_MERODIS_OK(db.LRange("key", 0, 2, &values));
  ASSERT_EQ(values, std::vector<std::string>({"0", "1", "2"}));
  values.clear();
  ASSERT_MERODIS_OK(db.LRange("key", -3, 1, &values));
  ASSERT_EQ(values, std::vector<std::string>({"0", "1"}));
  values.clear();
  ASSERT_MERODIS_OK(db.LRange("key", -3, -1, &values));
  ASSERT_EQ(values, std::vector<std::string>({"0", "1", "2"}));
  values.clear();
  ASSERT_MERODIS_OK(db.LRange("key", -100, 100, &values));
  ASSERT_EQ(values, std::vector<std::string>({"0", "1", "2"}));
  values.clear();
  ASSERT_MERODIS_OK(db.LRange("key", 5, 10, &values));
  ASSERT_EQ(values, std::vector<std::string>());
  values.clear();
  ASSERT_MERODIS_OK(db.LRange("key", -3, -6, &values));
  ASSERT_EQ(values, std::vector<std::string>());
}

TEST_F(ListTest, LPop) {
  ASSERT_MERODIS_OK(db.LPush("key", "2"));
  ASSERT_MERODIS_OK(db.LPush("key", "1"));
  ASSERT_MERODIS_OK(db.LPush("key", "0"));
  std::vector<std::string> values;
  ASSERT_MERODIS_OK(db.LRange("key", 0, -1, &values));
  ASSERT_EQ(values, std::vector<std::string>({"0", "1", "2"}));
  values.clear();

  // ["0", "1", "2"] => ["1", "2"]
  ASSERT_MERODIS_OK(db.LPop("key", 1, &values));
  ASSERT_EQ(values, std::vector<std::string>({"0"}));
  values.clear();
  ASSERT_MERODIS_OK(db.LRange("key", 0, -1, &values));
  ASSERT_EQ(values, std::vector<std::string>({"1", "2"}));
  values.clear();

  // ["1", "2"] => []
  ASSERT_MERODIS_OK(db.LPop("key", 2, &values));
  ASSERT_EQ(values, std::vector<std::string>({"1", "2"}));
  values.clear();
  ASSERT_MERODIS_OK(db.LRange("key", 0, -1, &values));
  ASSERT_EQ(values, std::vector<std::string>());
  values.clear();

  // [] => []
  ASSERT_MERODIS_OK(db.LPop("key", 3, &values));
  ASSERT_EQ(values, std::vector<std::string>());
  values.clear();
  ASSERT_MERODIS_OK(db.LRange("key", 0, -1, &values));
  ASSERT_EQ(values, std::vector<std::string>());
  values.clear();

  // Make sure LPush can be successfully done after LPop.
  // [] => ["value-0", "value-1"]
  ASSERT_MERODIS_OK(db.LPush("key", "value-1"));
  ASSERT_MERODIS_OK(db.LPush("key", "value-0"));
  ASSERT_MERODIS_OK(db.LRange("key", 0, -1, &values));
  ASSERT_EQ(values, std::vector<std::string>({"value-0", "value-1"}));
  values.clear();
}
