#include "gtest/gtest.h"
#include "merodis/merodis.h"
#include "common.h"
#include "testutil.h"

namespace merodis {
namespace test {

class HashTest : public RedisTest {
public:
  HashTest() : key_("key") {}
  std::string HGet(const Slice& key, const Slice& hashKey) {
    std::string value;
    EXPECT_MERODIS_OK(db.HGet(key, hashKey, &value));
    return value;
  }
  void HSet(const Slice& key, const Slice& hashKey, const Slice& value) {
    EXPECT_MERODIS_OK(db.HSet(key, hashKey, value));
  }

  std::string HGet(const Slice& hashKey) { return HGet(key_, hashKey); }
  void HSet(const Slice& hashKey, const Slice& value) { return HSet(key_, hashKey, value); }

private:
  Slice key_;
};

TEST_F(HashTest, HSet) {
  HSet("k0", "v0");
  ASSERT_EQ(HGet("k0"), "v0");

  HSet("k1", "v1");
  ASSERT_EQ(HGet("k1"), "v1");

  HSet("k0", HGet("k1"));
  ASSERT_EQ(HGet("k0"), "v1");
}

}
}