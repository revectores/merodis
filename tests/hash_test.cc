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
  std::map<std::string, std::string> HGetAll(const Slice& key) {
    std::map<std::string, std::string> kvs;
    EXPECT_MERODIS_OK(db.HGetAll(key, &kvs));
    return kvs;
  }
  std::vector<std::string> HKeys(const Slice& key) {
    std::vector<std::string> keys;
    EXPECT_MERODIS_OK(db.HKeys(key, &keys));
    return keys;
  }
  std::vector<std::string> HVals(const Slice& key) {
    std::vector<std::string> values;
    EXPECT_MERODIS_OK(db.HVals(key, &values));
    return values;
  }
  bool HExists(const Slice& key, const Slice& hashKey) {
    bool exists;
    EXPECT_MERODIS_OK(db.HExists(key, hashKey, &exists));
    return exists;
  }
  void HSet(const Slice& key, const Slice& hashKey, const Slice& value) {
    EXPECT_MERODIS_OK(db.HSet(key, hashKey, value));
  }

  uint64_t HLen() { return HLen(key_); }
  std::string HGet(const Slice& hashKey) { return HGet(key_, hashKey); }
  std::map<std::string, std::string> HGetAll() { return HGetAll(key_); }
  std::vector<std::string> HKeys() { return HKeys(key_); }
  std::vector<std::string> HVals() { return HVals(key_); }
  bool HExists(const Slice& hashKey) { return HExists(key_, hashKey); }
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

TEST_F(HashTest, HExists) {
  ASSERT_EQ(HExists("k0"), false);
  HSet("k0", "v0");
  ASSERT_EQ(HExists("k0"), true);
  HSet("k0", "v1");
  ASSERT_EQ(HExists("k0"), true);
}

TEST_F(HashTest, HGetAll) {
  ASSERT_EQ(HGetAll(), KVS());
  HSet("k0", "v0");
  ASSERT_EQ(HGetAll(), KVS({"k0", "v0"}));
  HSet("k1", "v1");
  ASSERT_EQ(HGetAll(), KVS({"k0", "v0"}, {"k1", "v1"}));
  HSet("k1", "v2");
  ASSERT_EQ(HGetAll(), KVS({"k0", "v0"}, {"k1", "v2"}));
}

TEST_F(HashTest, HKeys) {
  ASSERT_EQ(HKeys(), LIST());
  HSet("k0", "v0");
  ASSERT_EQ(HKeys(), LIST("k0"));
  HSet("k1", "v1");
  ASSERT_EQ(HKeys(), LIST("k0", "k1"));
  HSet("k1", "v2");
  ASSERT_EQ(HKeys(), LIST("k0", "k1"));
}

TEST_F(HashTest, HVals) {
  ASSERT_EQ(HVals(), LIST());
  HSet("k0", "v0");
  ASSERT_EQ(HVals(), LIST("v0"));
  HSet("k1", "v1");
  ASSERT_EQ(HVals(), LIST("v0", "v1"));
  HSet("k1", "v2");
  ASSERT_EQ(HVals(), LIST("v0", "v2"));
}

}
}