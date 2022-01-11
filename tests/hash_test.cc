#include "gtest/gtest.h"
#include "merodis/merodis.h"
#include "common.h"
#include "testutil.h"

namespace merodis {
namespace test {

class HashTest : public RedisTest {
public:
  HashTest() : key_("key") {}
  uint64_t HLen(const Slice& key) {
    uint64_t len;
    EXPECT_MERODIS_OK(db.HLen(key, &len));
    return len;
  }
  std::string HGet(const Slice& key, const Slice& hashKey) {
    std::string value;
    EXPECT_MERODIS_OK(db.HGet(key, hashKey, &value));
    return value;
  }
  void HSet(const Slice& key, const Slice& hashKey, const Slice& value) {
    EXPECT_MERODIS_OK(db.HSet(key, hashKey, value));
  }

  uint64_t HLen() { return HLen(key_); }
  std::string HGet(const Slice& hashKey) { return HGet(key_, hashKey); }
  void HSet(const Slice& hashKey, const Slice& value) { return HSet(key_, hashKey, value); }

private:
  Slice key_;
};

TEST_F(HashTest, HSET) {
  HSet("k0", "v0");
  ASSERT_EQ(HGet("k0"), "v0");

  HSet("k1", "v1");
  ASSERT_EQ(HGet("k1"), "v1");

  HSet("k0", HGet("k1"));
  ASSERT_EQ(HGet("k0"), "v1");
}

TEST_F(HashTest, HLEN) {
  ASSERT_EQ(HLen(), 0);
  HSet("k0", "v0");
  ASSERT_EQ(HLen(), 1);
  HSet("k1", "v1");
  ASSERT_EQ(HLen(), 2);
  HSet("k1", "v2");
  ASSERT_EQ(HLen(), 2);
}

}
}