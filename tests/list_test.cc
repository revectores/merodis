#include <cstdint>
#include <vector>

#include "gtest/gtest.h"
#include "merodis/merodis.h"
#include "common.h"
#include "testutil.h"

namespace merodis {
namespace test {

#define LIST(...) \
  std::vector<std::string>({ __VA_ARGS__ })

class ListTest : public RedisTest {
public:
  ListTest() : key_("key") {}

  uint64_t LLen(const Slice& key) {
    uint64_t len;
    db.LLen(key, &len);
    return len;
  }
  std::string LIndex(const Slice& key, int64_t index) {
    std::string value;
    EXPECT_MERODIS_OK(db.LIndex(key, index, &value));
    return value;
  }
  std::vector<std::string> LRange(const Slice& key, int64_t from, int64_t to) {
    std::vector<std::string> values;
    EXPECT_MERODIS_OK(db.LRange(key, from, to, &values));
    return values;
  }
  std::vector<std::string> List(const Slice& key) {
    return LRange(key, 0, -1);
  }
  std::string LPop(const Slice& key) {
    std::string value;
    EXPECT_MERODIS_OK(db.LPop(key, &value));
    return value;
  }
  std::vector<std::string> LPop(const Slice& key, uint64_t count) {
    std::vector<std::string> values;
    EXPECT_MERODIS_OK(db.LPop(key, count, &values));
    return values;
  }

  uint64_t LLen() { return LLen(key_); }
  std::string LIndex(int64_t index) { return LIndex(key_, index); };
  std::vector<std::string> LRange(int64_t from, int64_t to) { return LRange(key_, from, to); };
  std::vector<std::string> List() { return List(key_); };
  std::string LPop() { return LPop(key_); };
  std::vector<std::string> LPop(uint64_t count) { return LPop(key_, count); };
  void SetKey(const Slice& key) { key_ = key; }

private:
  Slice key_;
};

TEST_F(ListTest, LINDEX) {
  std::string value;
  ASSERT_EQ(LLen(), 0);

  // [] => ["1"]
  ASSERT_MERODIS_OK(db.LPush("key", "1"));
  ASSERT_EQ(LLen(), 1);

  // ["1"] => ["0", "1"]
  ASSERT_MERODIS_OK(db.LPush("key", "0"));
  ASSERT_EQ(LLen(), 2);
  ASSERT_EQ(LIndex(0), "0");
  ASSERT_EQ(LIndex(1), "1");
  ASSERT_EQ(LIndex(-1), "1");
  ASSERT_EQ(LIndex(-2), "0");
  ASSERT_MERODIS_ISNOTFOUND(db.LIndex("key", 2, &value));
  ASSERT_MERODIS_ISNOTFOUND(db.LIndex("key", -3, &value));
}

TEST_F(ListTest, LRANGE) {
  // [] => ["0", "1", "2"]
  ASSERT_MERODIS_OK(db.LPush("key", "2"));
  ASSERT_MERODIS_OK(db.LPush("key", "1"));
  ASSERT_MERODIS_OK(db.LPush("key", "0"));
  ASSERT_EQ(LRange(0, 0), LIST("0"));
  ASSERT_EQ(LRange(-3, 1), LIST("0", "1"));
  ASSERT_EQ(LRange(-3, -1), LIST("0", "1", "2"));
  ASSERT_EQ(LRange(-100, 100), LIST("0", "1", "2"));
  ASSERT_EQ(LRange(5, 10), LIST());
  ASSERT_EQ(LRange(-3, -6), LIST());
}

TEST_F(ListTest, LPOP_SINGLE) {
  std::string value;
  std::vector<std::string> values;
  ASSERT_MERODIS_OK(db.LPush("key", "1"));
  ASSERT_MERODIS_OK(db.LPush("key", "0"));
  ASSERT_EQ(List(), LIST("0", "1"));

  // ["0", "1"] => ["1"]
  ASSERT_EQ(LPop(), "0");
  ASSERT_EQ(List(), LIST("1"));

  // ["1"] => []
  ASSERT_EQ(LPop(), "1");
  ASSERT_EQ(List(), LIST());

  // [] => []
  ASSERT_EQ(LPop(), "");
  ASSERT_EQ(List(), LIST());
}

TEST_F(ListTest, LPOP_MULTIPLE) {
  ASSERT_MERODIS_OK(db.LPush("key", "2"));
  ASSERT_MERODIS_OK(db.LPush("key", "1"));
  ASSERT_MERODIS_OK(db.LPush("key", "0"));
  ASSERT_EQ(List(), LIST("0", "1", "2"));

  // ["0", "1", "2"] => ["1", "2"]
  ASSERT_EQ(LPop(1), LIST("0"));
  ASSERT_EQ(List(), LIST("1", "2"));

  // ["1", "2"] => []
  ASSERT_EQ(LPop(2), LIST("1", "2"));
  ASSERT_EQ(List(), LIST());

  // [] => []
  ASSERT_EQ(LPop(3), LIST());
  ASSERT_EQ(List(), LIST());

  // Make sure LPUSH can be successfully done after LPOP.
  // [] => ["value-0", "value-1"]
  ASSERT_MERODIS_OK(db.LPush("key", "value-1"));
  ASSERT_MERODIS_OK(db.LPush("key", "value-0"));
  ASSERT_EQ(List(), LIST("value-0", "value-1"));
}

}
}
