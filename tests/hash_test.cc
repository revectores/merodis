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
  std::vector<std::string> HMGet(const Slice& key, const std::vector<Slice>& hashKeys) {
    std::vector<std::string> values;
    EXPECT_MERODIS_OK(db.HMGet(key, hashKeys, &values));
    return values;
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
  uint64_t HSet(const Slice& key, const Slice& hashKey, const Slice& value) {
    uint64_t count = 0;
    EXPECT_MERODIS_OK(db.HSet(key, hashKey, value, &count));
    return count;
  }
  uint64_t HSet(const Slice& key, const std::map<Slice, Slice>& kvs) {
    uint64_t count = 0;
    EXPECT_MERODIS_OK(db.HSet(key, kvs, &count));
    return count;
  }
  uint64_t HDel(const Slice& key, const Slice& hashKey) {
    uint64_t count = 0;
    EXPECT_MERODIS_OK(db.HDel(key, hashKey, &count));
    return count;
  }
  uint64_t HDel(const Slice& key, const std::set<Slice>& hashKeys) {
    uint64_t count = 0;
    EXPECT_MERODIS_OK(db.HDel(key, hashKeys, &count));
    return count;
  }

  uint64_t HLen() { return HLen(key_); }
  std::string HGet(const Slice& hashKey) { return HGet(key_, hashKey); }
  std::vector<std::string> HMGet(const std::vector<Slice>& hashKeys) { return HMGet(key_, hashKeys); }
  std::map<std::string, std::string> HGetAll() { return HGetAll(key_); }
  std::vector<std::string> HKeys() { return HKeys(key_); }
  std::vector<std::string> HVals() { return HVals(key_); }
  bool HExists(const Slice& hashKey) { return HExists(key_, hashKey); }
  uint64_t HSet(const Slice& hashKey, const Slice& value) { return HSet(key_, hashKey, value); }
  uint64_t HSet(const std::map<Slice, Slice>& kvs) { return HSet(key_, kvs); }
  uint64_t HDel(const Slice& hashKey) { return HDel(key_, hashKey); }
  uint64_t HDel(const std::set<Slice>& hashKeys) { return HDel(key_, hashKeys); };

  virtual void TestHSet();
  virtual void TestHLen();
  virtual void TestHExists();
  virtual void TestHMGet();
  virtual void TestHGetAll();
  virtual void TestHKeys();
  virtual void TestHVals();
  virtual void TestHDel();

private:
  Slice key_;
};

class HashBasicImplTest: public HashTest {
public:
  HashBasicImplTest() {
    options.hash_impl = kHashBasicImpl;
    db.Open(options, db_path);
  }
};

void HashTest::TestHSet() {
  ASSERT_EQ(HSet("k0", "v0"), 1);
  ASSERT_EQ(HGetAll(), KVS({"k0", "v0"}));
  ASSERT_EQ(HSet({{"k1", "v1"}}), 1);
  ASSERT_EQ(HGetAll(), KVS({"k0", "v0"}, {"k1", "v1"}));
  ASSERT_EQ(HSet({{"k2", "v2"}, {"k3", "v3"}}), 2);
  ASSERT_EQ(HGetAll(), KVS({"k0", "v0"}, {"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}));
  ASSERT_EQ(HSet({{"k0", "vx"}, {"k3", "vx"}}), 0);
  ASSERT_EQ(HGetAll(), KVS({"k0", "vx"}, {"k1", "v1"}, {"k2", "v2"}, {"k3", "vx"}));
  ASSERT_EQ(HSet({{"k#", "v#"}, {"k1", "vx"}, {"k2", "vx"}, {"k4", "v4"}}), 2);
  ASSERT_EQ(HGetAll(), KVS({"k#", "v#"}, {"k0", "vx"}, {"k1", "vx"}, {"k2", "vx"}, {"k3", "vx"}, {"k4", "v4"}));
  ASSERT_EQ(HDel("k0"), 1);
  ASSERT_EQ(HDel("k3"), 1);
  ASSERT_EQ(HGetAll(), KVS({"k#", "v#"}, {"k1", "vx"}, {"k2", "vx"}, {"k4", "v4"}));
  ASSERT_EQ(HSet({{"k0", "v0"}, {"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}}), 2);
  ASSERT_EQ(HGetAll(), KVS({"k#", "v#"}, {"k0", "v0"}, {"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}, {"k4", "v4"}));
}

void HashTest::TestHLen() {
  ASSERT_EQ(HLen(), 0);
  HSet("k0", "v0");
  ASSERT_EQ(HLen(), 1);
  HSet("k1", "v1");
  ASSERT_EQ(HLen(), 2);
  HSet("k1", "v2");
  ASSERT_EQ(HLen(), 2);
}

void HashTest::TestHExists() {
  ASSERT_EQ(HExists("k0"), false);
  HSet("k0", "v0");
  ASSERT_EQ(HExists("k0"), true);
  HSet("k0", "v1");
  ASSERT_EQ(HExists("k0"), true);
}

void HashTest::TestHMGet() {
  ASSERT_EQ(HSet({{"k0", "v0"}, {"k1", "v1"}, {"k2", "v2"}}), 3);
  ASSERT_EQ(HMGet({"k0", "k2"}), LIST("v0", "v2"));
  ASSERT_EQ(HMGet({"k1"}), LIST("v1"));
  ASSERT_EQ(HMGet({"k2", "k0"}), LIST("v2", "v0"));
  ASSERT_EQ(HMGet({"k2", "k1", "k1", "k2"}), LIST("v2", "v1", "v1", "v2"));
}

void HashTest::TestHGetAll() {
  ASSERT_EQ(HGetAll(), KVS());
  HSet("k0", "v0");
  ASSERT_EQ(HGetAll(), KVS({"k0", "v0"}));
  HSet("k1", "v1");
  ASSERT_EQ(HGetAll(), KVS({"k0", "v0"}, {"k1", "v1"}));
  HSet("k1", "v2");
  ASSERT_EQ(HGetAll(), KVS({"k0", "v0"}, {"k1", "v2"}));
}

void HashTest::TestHKeys() {
  ASSERT_EQ(HKeys(), LIST());
  HSet("k0", "v0");
  ASSERT_EQ(HKeys(), LIST("k0"));
  HSet("k1", "v1");
  ASSERT_EQ(HKeys(), LIST("k0", "k1"));
  HSet("k1", "v2");
  ASSERT_EQ(HKeys(), LIST("k0", "k1"));
}

void HashTest::TestHVals() {
  ASSERT_EQ(HVals(), LIST());
  HSet("k0", "v0");
  ASSERT_EQ(HVals(), LIST("v0"));
  HSet("k1", "v1");
  ASSERT_EQ(HVals(), LIST("v0", "v1"));
  HSet("k1", "v2");
  ASSERT_EQ(HVals(), LIST("v0", "v2"));
}

void HashTest::TestHDel() {
  HSet({{"k0", "v0"}, {"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}});
  ASSERT_EQ(HLen(), 4);
  ASSERT_EQ(HKeys(), LIST("k0", "k1", "k2", "k3"));
  ASSERT_EQ(HDel("k3"), 1);
  ASSERT_EQ(HLen(), 3);
  ASSERT_EQ(HKeys(), LIST("k0", "k1", "k2"));
  ASSERT_EQ(HDel({"k2", "k3"}), 1);
  ASSERT_EQ(HLen(), 2);
  ASSERT_EQ(HKeys(), LIST("k0", "k1"));
  ASSERT_EQ(HDel({"k2", "k3"}), 0);
  ASSERT_EQ(HLen(), 2);
  ASSERT_EQ(HKeys(), LIST("k0", "k1"));
  ASSERT_EQ(HDel({"k0", "k1"}), 2);
  ASSERT_EQ(HLen(), 0);
  ASSERT_EQ(HKeys(), LIST());
}

TEST_F(HashBasicImplTest, HSet) {
  TestHSet();
}

TEST_F(HashBasicImplTest, HLen) {
  TestHLen();
}

TEST_F(HashBasicImplTest, HExists) {
  TestHExists();
}

TEST_F(HashBasicImplTest, HMGet) {
  TestHMGet();
}

TEST_F(HashBasicImplTest, HGetAll) {
  TestHGetAll();
}

TEST_F(HashBasicImplTest, HKeys) {
  TestHKeys();
}

TEST_F(HashBasicImplTest, HVals) {
  TestHVals();
}

TEST_F(HashBasicImplTest, HDel) {
  TestHDel();
}

}
}