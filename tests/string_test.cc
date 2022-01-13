#include "gtest/gtest.h"
#include "merodis/merodis.h"
#include "common.h"
#include "testutil.h"


namespace merodis {
namespace test {

class StringTest : public RedisTest {
public:
  StringTest() : key_("key") {}

  std::string Get(const Slice& key) {
    std::string value;
    EXPECT_MERODIS_OK(db.Get(key, &value));
    return value;
  }
  void Set(const Slice& key, const Slice& value) {
    EXPECT_MERODIS_OK(db.Set(key, value));
  }

  std::string Get() { return Get(key_); }
  void Set(const Slice& value) { Set(key_, value); }
  void SetKey(const Slice& key) { key_ = key; }

private:
  Slice key_;
};

TEST_F(StringTest, SET_GET) {
  Set("value");
  ASSERT_EQ(Get(), "value");
  Set("123");
  ASSERT_EQ(Get(), "123");
  Set("0");
  ASSERT_EQ(Get(), "0");
  Set("-123");
  ASSERT_EQ(Get(), "-123");
  Set("9223372036854775807");
  ASSERT_EQ(Get(), "9223372036854775807");
  Set("9223372036854775808");
  ASSERT_EQ(Get(), "9223372036854775808");
  Set("-9223372036854775808");
  ASSERT_EQ(Get(), "-9223372036854775808");
  Set("-9223372036854775809");
  ASSERT_EQ(Get(), "-9223372036854775809");
  Set("");
  ASSERT_EQ(Get(), "");
  Set("1xx");
  ASSERT_EQ(Get(), "1xx");
  Set("1.2");
  ASSERT_EQ(Get(), "1.2");
}

}
}