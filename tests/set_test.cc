#include "gtest/gtest.h"
#include "merodis/merodis.h"
#include "common.h"
#include "testutil.h"

namespace merodis {
namespace test {

class SetTest : public RedisTest {
public:
  SetTest() : key_("key") {}
  uint64_t SCard(const Slice& key) {
    uint64_t len;
    EXPECT_MERODIS_OK(db.SCard(key, &len));
    return len;
  }
  bool SIsMember(const Slice& key, const Slice& setKey) {
    bool isMember;
    EXPECT_MERODIS_OK(db.SIsMember(key, setKey, &isMember));
    return isMember;
  }
  std::vector<bool> SMIsMember(const Slice& key, const std::set<Slice>& keys) {
    std::vector<bool> isMembers;
    EXPECT_MERODIS_OK(db.SMIsMember(key, keys, &isMembers));
    return isMembers;
  }
  std::vector<std::string> SMembers(const Slice& key) {
    std::vector<std::string> keys;
    EXPECT_MERODIS_OK(db.SMembers(key, &keys));
    return keys;
  }
  uint64_t SAdd(const Slice& key, const Slice& setKey) {
    uint64_t count;
    EXPECT_MERODIS_OK(db.SAdd(key, setKey, &count));
    return count;
  }
  uint64_t SAdd(const Slice& key, const std::set<Slice>& keys) {
    uint64_t count;
    EXPECT_MERODIS_OK(db.SAdd(key, keys, &count));
    return count;
  }

  uint64_t SCard() { return SCard(key_); }
  bool SIsMember(const Slice& setKey) { return SIsMember(key_, setKey); }
  std::vector<bool> SMIsMember(const std::set<Slice>& keys) { return SMIsMember(key_, keys); }
  std::vector<std::string> SMembers() { return SMembers(key_); }
  uint64_t SAdd(const Slice& setKey) { return SAdd(key_, setKey); }
  uint64_t SAdd(const std::set<Slice>& keys) { return SAdd(key_, keys); }

  virtual void TestSAdd();

private:
  Slice key_;
};

class SetBasicImplTest: public SetTest {
public:
  SetBasicImplTest() {
    options.set_impl = kSetBasicImpl;
    db.Open(options, db_path);
  }
};

void SetTest::TestSAdd() {
  ASSERT_EQ(SAdd("k0"), 1);
  ASSERT_EQ(SCard(), 1);
  ASSERT_EQ(SAdd("k1"), 1);
  ASSERT_EQ(SCard(), 2);
  ASSERT_EQ(SAdd("k0"), 0);
  ASSERT_EQ(SCard(), 2);
  ASSERT_EQ(SMembers(), LIST("k0", "k1"));
  ASSERT_EQ(SIsMember("k0"), true);
  ASSERT_EQ(SIsMember("k1"), true);
  ASSERT_EQ(SIsMember("k2"), false);
  ASSERT_EQ(SMIsMember({"k0", "k1", "k2"}), BOOLEANS(true, true, false));

  ASSERT_EQ(SAdd({"k0", "k1", "k2"}), 1);
  ASSERT_EQ(SMembers(), LIST("k0", "k1", "k2"));
  ASSERT_EQ(SIsMember("k2"), true);

  ASSERT_EQ(SAdd({"k3", "k4"}), 2);
  ASSERT_EQ(SMembers(), LIST("k0", "k1", "k2", "k3", "k4"));
  ASSERT_EQ(SMIsMember({"k3", "k4", "k5"}), BOOLEANS(true, true, false));
}

TEST_F(SetBasicImplTest, SAdd) {
  TestSAdd();
}

}
}
