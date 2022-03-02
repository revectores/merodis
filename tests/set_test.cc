#include <vector>
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
  std::string SRandMember(const Slice& key) {
    std::string member;
    EXPECT_MERODIS_OK(db.SRandMember(key, &member));
    return member;
  }
  std::vector<std::string> SRandMember(const Slice& key, int64_t count) {
    std::vector<std::string> members;
    EXPECT_MERODIS_OK(db.SRandMember(key, count, &members));
    return members;
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
  uint64_t SRem(const Slice& key, const Slice& member) {
    uint64_t count;
    EXPECT_MERODIS_OK(db.SRem(key, member, &count));
    return count;
  }
  uint64_t SRem(const Slice& key, const std::set<Slice>& members) {
    uint64_t count;
    EXPECT_MERODIS_OK(db.SRem(key, members, &count));
    return count;
  }
  std::string SPop(const Slice& key) {
    std::string member;
    EXPECT_MERODIS_OK(db.SPop(key, &member));
    return member;
  }
  std::vector<std::string> SPop(const Slice& key, uint64_t count) {
    std::vector<std::string> members;
    EXPECT_MERODIS_OK(db.SPop(key, count, &members));
    return members;
  }
  uint64_t SMove(const Slice& srcKey, const Slice& dstKey, const Slice& member) {
    uint64_t count;
    EXPECT_MERODIS_OK(db.SMove(srcKey, dstKey, member, &count));
    return count;
  }

  uint64_t SCard() { return SCard(key_); }
  bool SIsMember(const Slice& setKey) { return SIsMember(key_, setKey); }
  std::vector<bool> SMIsMember(const std::set<Slice>& keys) { return SMIsMember(key_, keys); }
  std::vector<std::string> SMembers() { return SMembers(key_); }
  std::string SRandMember() {return SRandMember(key_); }
  std::vector<std::string> SRandMember(int64_t count) { return SRandMember(key_, count); }
  uint64_t SAdd(const Slice& setKey) { return SAdd(key_, setKey); }
  uint64_t SAdd(const std::set<Slice>& keys) { return SAdd(key_, keys); }
  uint64_t SRem(const Slice& member) { return SRem(key_, member); }
  uint64_t SRem(const std::set<Slice>& members) { return SRem(key_, members); }
  std::string SPop() { return SPop(key_); }
  std::vector<std::string> SPop(uint64_t count) { return SPop(key_, count); }

  virtual void TestSAdd();
  virtual void TestSRandMember();
  virtual void TestSRem();
  virtual void TestSPop();
  virtual void TestSMove();

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

void SetTest::TestSRandMember() {
  ASSERT_EQ(SAdd({"k0", "k1", "k2"}), 3);
  ASSERT_EQ(SIsMember(SRandMember()), true);

  std::vector<int64_t> counts;

  counts = {-4, -3, -2, -1};
  for (auto count: counts) {
    std::vector<std::string> members = SRandMember(count);
    for (const auto& member: members) ASSERT_TRUE(SIsMember(member));
    ASSERT_EQ(members.size(), abs(count));
  }

  counts = {1, 2};
  for (auto count: counts) {
    std::vector<std::string> members = SRandMember(count);
    std::set<std::string> memberSet(members.begin(), members.end());
    for (const auto& member: members) ASSERT_TRUE(SIsMember(member));
    ASSERT_EQ(members.size(), count);
    ASSERT_EQ(members.size(), memberSet.size());
  }

  counts = {3, 4};
  for (auto count: counts) {
    std::vector<std::string> members = SRandMember(count);
    std::set<std::string> memberSet(members.begin(), members.end());
    ASSERT_EQ(members.size(), SCard());
    ASSERT_EQ(memberSet, SET("k0", "k1", "k2"));
  }
}

void SetTest::TestSRem() {
}

void SetTest::TestSPop() {
}

void SetTest::TestSMove() {
}

TEST_F(SetBasicImplTest, SAdd) {
  TestSAdd();
}

TEST_F(SetBasicImplTest, SRandMember) {
  TestSRandMember();
}

TEST_F(SetBasicImplTest, SRem) {
  TestSRem();
}

TEST_F(SetBasicImplTest, SPop) {
  TestSPop();
}

TEST_F(SetBasicImplTest, SMove) {
  TestSMove();
}

}
}
