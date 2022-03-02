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
  std::vector<std::string> SUnion(const std::vector<Slice>& keys) {
    std::vector<std::string> members;
    EXPECT_MERODIS_OK(db.SUnion(keys, &members));
    return members;
  }
  std::vector<std::string> SInter(const std::vector<Slice>& keys) {
    std::vector<std::string> members;
    EXPECT_MERODIS_OK(db.SInter(keys, &members));
    return members;
  }
  std::vector<std::string> SDiff(const std::vector<Slice>& keys) {
    std::vector<std::string> members;
    EXPECT_MERODIS_OK(db.SDiff(keys, &members));
    return members;
  }
  uint64_t SUnionStore(const std::vector<Slice>& keys, const Slice& dstKey) {
    uint64_t count;
    EXPECT_MERODIS_OK(db.SUnionStore(keys, dstKey, &count));
    return count;
  }
  uint64_t SInterStore(const std::vector<Slice>& keys, const Slice& dstKey) {
    uint64_t count;
    EXPECT_MERODIS_OK(db.SInterStore(keys, dstKey, &count));
    return count;
  }
  uint64_t SDiffStore(const std::vector<Slice>& keys, const Slice& dstKey) {
    uint64_t count;
    EXPECT_MERODIS_OK(db.SDiffStore(keys, dstKey, &count));
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
  virtual void TestUnion();
  virtual void TestInter();
  virtual void TestDiff();

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
  ASSERT_EQ(SAdd({"k0", "k1", "k2"}), 3);
  ASSERT_EQ(SRem("kk"), 0);
  ASSERT_EQ(SCard(), 3);
  ASSERT_EQ(SRem("k0"), 1);
  ASSERT_EQ(SCard(), 2);
  ASSERT_EQ(SRem("k0"), 0);
  ASSERT_EQ(SCard(), 2);
  ASSERT_EQ(SRem("k1"), 1);
  ASSERT_EQ(SRem("k2"), 1);

  ASSERT_EQ(SAdd({"k0", "k1", "k2"}), 3);
  ASSERT_EQ(SRem({"k0", "k2"}), 2);
  ASSERT_EQ(SCard(), 1);
  ASSERT_EQ(SRem({"k1", "k2", "kk"}), 1);
  ASSERT_EQ(SCard(), 0);
  ASSERT_EQ(SRem({"k0", "k1", "k2"}), 0);
  ASSERT_EQ(SCard(), 0);
}

void SetTest::TestSPop() {
  Status s;
  std::string member;
  std::vector<std::string> members;
  std::set<Slice> keys = {"k0", "k1", "k2"};
  ASSERT_EQ(SAdd(keys), 3);

  for (int c = 3; c > 0; c--) {
    ASSERT_EQ(SCard(), c);
    member = SPop();
    ASSERT_EQ(SCard(), c - 1);
    ASSERT_FALSE(SIsMember(member));
    ASSERT_TRUE(keys.find(member) != keys.end());
  }
  s = db.SPop("key", &member);
  ASSERT_MERODIS_IS_NOT_FOUND(s);
  ASSERT_EQ(s.ToString(), "NotFound: empty set");
  ASSERT_EQ(SCard(), 0);

  ASSERT_EQ(SAdd(keys), 3);
  ASSERT_EQ(SCard(), 3);
  members = SPop(2);
  ASSERT_EQ(members.size(), 2);
  ASSERT_EQ(SCard(), 1);
  for (const auto& m: members) ASSERT_FALSE(SIsMember(m));

  members = SPop(2);
  ASSERT_EQ(members.size(), 1);
  ASSERT_EQ(SCard(), 0);
  for (const auto& m: members) ASSERT_FALSE(SIsMember(m));
}

void SetTest::TestSMove() {
  ASSERT_EQ(SAdd("s1", {"0", "1"}), 2);
  ASSERT_EQ(SAdd("s2", {"1", "2"}), 2);

  ASSERT_EQ(SMove("s1", "s2", "0"), 1);
  ASSERT_EQ(SCard("s1"), 1);
  ASSERT_EQ(SCard("s2"), 3);
  ASSERT_EQ(SMembers("s1"), LIST("1"));
  ASSERT_EQ(SMembers("s2"), LIST("0", "1", "2"));

  ASSERT_EQ(SMove("s1", "s2", "1"), 1);
  ASSERT_EQ(SCard("s1"), 0);
  ASSERT_EQ(SCard("s2"), 3);
  ASSERT_EQ(SMembers("s1"), LIST());
  ASSERT_EQ(SMembers("s2"), LIST("0", "1", "2"));

  ASSERT_EQ(SMove("s2", "s1", "2"), 1);
  ASSERT_EQ(SCard("s1"), 1);
  ASSERT_EQ(SCard("s2"), 2);
  ASSERT_EQ(SMembers("s1"), LIST("2"));
  ASSERT_EQ(SMembers("s2"), LIST("0", "1"));

  ASSERT_EQ(SMove("s2", "s1", "2"), 0);
  ASSERT_EQ(SCard("s1"), 1);
  ASSERT_EQ(SCard("s2"), 2);
  ASSERT_EQ(SMembers("s1"), LIST("2"));
  ASSERT_EQ(SMembers("s2"), LIST("0", "1"));

  ASSERT_EQ(SAdd("s2", "2"), 1);
  ASSERT_EQ(SMembers("s2"), LIST("0", "1", "2"));
  ASSERT_EQ(SMove("s2", "s1", "2"), 1);
  ASSERT_EQ(SCard("s1"), 1);
  ASSERT_EQ(SCard("s2"), 2);
  ASSERT_EQ(SMembers("s1"), LIST("2"));
  ASSERT_EQ(SMembers("s2"), LIST("0", "1"));
}

void SetTest::TestUnion() {
  ASSERT_EQ(SAdd("s1", {"0", "1"}), 2);
  ASSERT_EQ(SAdd("s2", {"1", "2"}), 2);
  ASSERT_EQ(SAdd("s3", {"0", "3"}), 2);
  ASSERT_EQ(SAdd("s4", {"0", "1", "2"}), 3);
  ASSERT_EQ(SUnion({"s1", "s2"}), LIST("0", "1", "2"));
  ASSERT_EQ(SUnion({"s2", "s3"}), LIST("0", "1", "2", "3"));
  ASSERT_EQ(SUnion({"s1", "s4"}), LIST("0", "1", "2"));
  ASSERT_EQ(SUnion({"s2", "s4"}), LIST("0", "1", "2"));
  ASSERT_EQ(SUnion({"s3", "s4"}), LIST("0", "1", "2", "3"));
}

void SetTest::TestInter() {
}

void SetTest::TestDiff() {
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

TEST_F(SetBasicImplTest, SUnion) {
  TestUnion();
}

TEST_F(SetBasicImplTest, SInter) {
  TestInter();
}

TEST_F(SetBasicImplTest, SDiff) {
  TestDiff();
}

}
}
