#include <vector>
#include "gtest/gtest.h"
#include "merodis/merodis.h"
#include "common.h"
#include "testutil.h"

namespace merodis {
namespace test {

const int64_t minInt64 = std::numeric_limits<int64_t>::min();
const int64_t maxInt64 = std::numeric_limits<int64_t>::max();

class ZSetTest: public RedisTest {
public:
  ZSetTest(): key_("key") {}
  uint64_t ZCard(const Slice& key) {
    uint64_t len;
    EXPECT_MERODIS_OK(db.ZCard(key, &len));
    return len;
  }
  int64_t ZScore(const Slice& key, const Slice& member) {
    int64_t score;
    EXPECT_MERODIS_OK(db.ZScore(key, member, &score));
    return score;
  }
  ScoreOpts ZMScore(const Slice& key, const std::vector<Slice>& members) {
    ScoreOpts scores;
    EXPECT_MERODIS_OK(db.ZMScore(key, members, &scores));
    return scores;
  }
  uint64_t ZRank(const Slice& key, const Slice& member) {
    uint64_t rank;
    EXPECT_MERODIS_OK(db.ZRank(key, member, &rank));
    return rank;
  }
  uint64_t ZRevRank(const Slice& key, const Slice& member) {
    uint64_t rank;
    EXPECT_MERODIS_OK(db.ZRevRank(key, member, &rank));
    return rank;
  }
  uint64_t ZCount(const Slice& key, int64_t minScore, int64_t maxScore) {
    uint64_t count;
    EXPECT_MERODIS_OK(db.ZCount(key, minScore, maxScore, &count));
    return count;
  }
  uint64_t ZLexCount(const Slice& key, const Slice& minLex, const Slice& maxLex) {
    uint64_t count;
    EXPECT_MERODIS_OK(db.ZLexCount(key, minLex, maxLex, &count));
    return count;
  }
  Members ZRange(const Slice& key, int64_t minRank, int64_t maxRank) {
    Members members;
    EXPECT_MERODIS_OK(db.ZRange(key, minRank, maxRank, &members));
    return members;
  }
  Members ZRangeByScore(const Slice& key, int64_t minScore, int64_t maxScore) {
    Members members;
    EXPECT_MERODIS_OK(db.ZRangeByScore(key, minScore, maxScore, &members));
    return members;
  }
  Members ZRangeByLex(const Slice& key, const Slice& minLex, const Slice& maxLex) {
    Members members;
    EXPECT_MERODIS_OK(db.ZRangeByLex(key, minLex, maxLex, &members));
    return members;
  }
  ScoredMembers ZRangeWithScores(const Slice& key, int64_t minRank, int64_t maxRank) {
    ScoredMembers scoredMembers;
    EXPECT_MERODIS_OK(db.ZRangeWithScores(key, minRank, maxRank, &scoredMembers));
    return scoredMembers;
  }
  ScoredMembers ZRangeByScoreWithScores(const Slice& key, int64_t minScore, int64_t maxScore) {
    ScoredMembers scoredMembers;
    EXPECT_MERODIS_OK(db.ZRangeByScoreWithScores(key, minScore, maxScore, &scoredMembers));
    return scoredMembers;
  }
  ScoredMembers ZRangeByLexWithScores(const Slice& key, const Slice& minLex, const Slice& maxLex) {
    ScoredMembers scoredMembers;
    EXPECT_MERODIS_OK(db.ZRangeByLexWithScores(key, minLex, maxLex, &scoredMembers));
    return scoredMembers;
  }
  Members ZRevRange(const Slice& key, int64_t minRank, int64_t maxRank) {
    Members members;
    EXPECT_MERODIS_OK(db.ZRevRange(key, minRank, maxRank, &members));
    return members;
  }
  Members ZRevRangeByScore(const Slice& key, int64_t minScore, int64_t maxScore) {
    Members members;
    EXPECT_MERODIS_OK(db.ZRevRangeByScore(key, minScore, maxScore, &members));
    return members;
  }
  Members ZRevRangeByLex(const Slice& key, const Slice& minLex, const Slice& maxLex) {
    Members members;
    EXPECT_MERODIS_OK(db.ZRevRangeByLex(key, minLex, maxLex, &members));
    return members;
  }
  ScoredMembers ZRevRangeWithScores(const Slice& key, int64_t minRank, int64_t maxRank) {
    ScoredMembers scoredMembers;
    EXPECT_MERODIS_OK(db.ZRevRangeWithScores(key, minRank, maxRank, &scoredMembers));
    return scoredMembers;
  }
  ScoredMembers ZRevRangeByScoreWithScores(const Slice& key, int64_t minScore, int64_t maxScore) {
    ScoredMembers scoredMembers;
    EXPECT_MERODIS_OK(db.ZRevRangeByScoreWithScores(key, minScore, maxScore, &scoredMembers));
    return scoredMembers;
  }
  ScoredMembers ZRevRangeByLexWithScores(const Slice& key, const Slice& minLex, const Slice& maxLex) {
    ScoredMembers scoredMembers;
    EXPECT_MERODIS_OK(db.ZRevRangeByLexWithScores(key, minLex, maxLex, &scoredMembers));
    return scoredMembers;
  }
  uint64_t ZAdd(const Slice& key, const std::pair<Slice, int64_t>& scoredMember) {
    uint64_t count;
    EXPECT_MERODIS_OK(db.ZAdd(key, scoredMember, &count));
    return count;
  }
  uint64_t ZAdd(const Slice& key, const std::map<Slice, int64_t>& scoredMembers) {
    uint64_t count;
    EXPECT_MERODIS_OK(db.ZAdd(key, scoredMembers, &count));
    return count;
  }
  uint64_t ZRem(const Slice& key, const Slice& member) {
    uint64_t count;
    EXPECT_MERODIS_OK(db.ZRem(key, member, &count));
    return count;
  }
  uint64_t ZRem(const Slice& key, const std::set<Slice>& members) {
    uint64_t count;
    EXPECT_MERODIS_OK(db.ZRem(key, members, &count));
    return count;
  }
  ScoredMember ZPopMax(const Slice& key) {
    ScoredMember scoredMember;
    EXPECT_MERODIS_OK(db.ZPopMax(key, &scoredMember));
    return scoredMember;
  }
  ScoredMember ZPopMin(const Slice& key) {
    ScoredMember scoredMember;
    EXPECT_MERODIS_OK(db.ZPopMin(key, &scoredMember));
    return scoredMember;
  }
  uint64_t ZRemRangeByRank(const Slice& key, int64_t minRank, int64_t maxRank) {
    uint64_t count;
    EXPECT_MERODIS_OK(db.ZRemRangeByRank(key, minRank, maxRank, &count));
    return count;
  }
  uint64_t ZRemRangeByScore(const Slice& key, int64_t minScore, int64_t maxScore) {
    uint64_t count;
    EXPECT_MERODIS_OK(db.ZRemRangeByScore(key, minScore, maxScore, &count));
    return count;
  }
  uint64_t ZRemRangeByLex(const Slice& key, const Slice& minLex, const Slice& maxLex) {
    uint64_t count;
    EXPECT_MERODIS_OK(db.ZRemRangeByLex(key, minLex, maxLex, &count));
    return count;
  }
  Members ZUnion(const std::vector<Slice>& keys) {
    Members members;
    EXPECT_MERODIS_OK(db.ZUnion(keys, &members));
    return members;
  }
  Members ZInter(const std::vector<Slice>& keys) {
    Members members;
    EXPECT_MERODIS_OK(db.ZInter(keys, &members));
    return members;
  }
  Members ZDiff(const std::vector<Slice>& keys) {
    Members members;
    EXPECT_MERODIS_OK(db.ZDiff(keys, &members));
    return members;
  }
  ScoredMembers ZUnionWithScores(const std::vector<Slice>& keys) {
    ScoredMembers scoredMembers;
    EXPECT_MERODIS_OK(db.ZUnionWithScores(keys, &scoredMembers));
    return scoredMembers;
  }
  ScoredMembers ZInterWithScores(const std::vector<Slice>& keys) {
    ScoredMembers scoredMembers;
    EXPECT_MERODIS_OK(db.ZInterWithScores(keys, &scoredMembers));
    return scoredMembers;
  }
  ScoredMembers ZDiffWithScores(const std::vector<Slice>& keys) {
    ScoredMembers scoredMembers;
    EXPECT_MERODIS_OK(db.ZDiffWithScores(keys, &scoredMembers));
    return scoredMembers;
  }
  uint64_t ZUnionStore(const std::vector<Slice>& keys, const Slice& dstKey) {
    uint64_t count;
    EXPECT_MERODIS_OK(db.ZUnionStore(keys, dstKey, &count));
    return count;
  }
  uint64_t ZInterStore(const std::vector<Slice>& keys, const Slice& dstKey) {
    uint64_t count;
    EXPECT_MERODIS_OK(db.ZInterStore(keys, dstKey, &count));
    return count;
  }
  uint64_t ZDiffStore(const std::vector<Slice>& keys, const Slice& dstKey) {
    uint64_t count;
    EXPECT_MERODIS_OK(db.ZDiffStore(keys, dstKey, &count));
    return count;
  }
  uint64_t ZCard() { return ZCard(key_); }
  int64_t ZScore(const Slice& member) { return ZScore(key_, member); }
  ScoreOpts ZMScore(const std::vector<Slice>& members) { return ZMScore(key_, members); }
  uint64_t ZRank(const Slice& member) { return ZRank(key_, member); }
  uint64_t ZRevRank(const Slice& member) { return ZRevRank(key_, member); }
  uint64_t ZCount(int64_t minScore, int64_t maxScore) { return ZCount(key_, minScore, maxScore); }
  uint64_t ZLexCount(const Slice& minLex, const Slice& maxLex) { return ZLexCount(key_, minLex, maxLex); }
  Members ZRange(int64_t minRank, int64_t maxRank) { return ZRange(key_, minRank, maxRank); }
  Members ZRangeByScore(int64_t minScore, int64_t maxScore) { return ZRangeByScore(key_, minScore, maxScore); }
  Members ZRangeByLex(const Slice& minLex, const Slice& maxLex) { return ZRangeByLex(key_, minLex, maxLex); }
  ScoredMembers ZRangeWithScores(int64_t minRank, int64_t maxRank) { return ZRangeWithScores(key_, minRank, maxRank); }
  ScoredMembers ZRangeByScoreWithScores(int64_t minScore, int64_t maxScore) { return ZRangeByScoreWithScores(key_, minScore, maxScore); }
  ScoredMembers ZRangeByLexWithScores(const Slice& minLex, const Slice& maxLex) { return ZRangeByLexWithScores(key_, minLex, maxLex); }
  Members ZRevRange(int64_t minRank, int64_t maxRank) { return ZRevRange(key_, minRank, maxRank); }
  Members ZRevRangeByScore(int64_t minScore, int64_t maxScore) { return ZRevRangeByScore(key_, minScore, maxScore); }
  Members ZRevRangeByLex(const Slice& minLex, const Slice& maxLex) { return ZRevRangeByLex(key_, minLex, maxLex); }
  ScoredMembers ZRevRangeWithScores(int64_t minRank, int64_t maxRank) { return ZRevRangeWithScores(key_, minRank, maxRank); }
  ScoredMembers ZRevRangeByScoreWithScores(int64_t minScore, int64_t maxScore) { return ZRevRangeByScoreWithScores(key_, minScore, maxScore); }
  ScoredMembers ZRevRangeByLexWithScores(const Slice& minLex, const Slice& maxLex) { return ZRevRangeByLexWithScores(key_, minLex, maxLex); }
  uint64_t ZAdd(const std::pair<Slice, int64_t>& scoredMember) { return ZAdd(key_, scoredMember); }
  uint64_t ZAdd(const std::map<Slice, int64_t>& scoredMembers) { return ZAdd(key_, scoredMembers); }
  uint64_t ZRem(const Slice& member) { return ZRem(key_, member); }
  uint64_t ZRem(const std::set<Slice>& members) { return ZRem(key_, members); }
  ScoredMember ZPopMax() { return ZPopMax(key_); }
  ScoredMember ZPopMin() { return ZPopMin(key_); }
  uint64_t ZRemRangeByRank(int64_t minRank, int64_t maxRank) { return ZRemRangeByRank(key_, minRank, maxRank); }
  uint64_t ZRemRangeByScore(int64_t minScore, int64_t maxScore) { return ZRemRangeByScore(key_, minScore, maxScore); }
  uint64_t ZRemRangeByLex(const Slice& minLex, const Slice& maxLex) { return ZRemRangeByLex(key_, minLex, maxLex); }

  virtual void TestZMScore();
  virtual void TestZRank();
  virtual void TestZCount();
  virtual void TestZLexCount();
  virtual void TestZRange();
  virtual void TestZRangeByScore();
  virtual void TestZRangeByLex();
  virtual void TestZAdd();
  virtual void TestZAddN();
  virtual void TestZRem();
  virtual void TestZRemN();
  virtual void TestZPopMax();
  virtual void TestZPopMin();
  virtual void TestZRemRangeByRank();
  virtual void TestZRemRangeByScore();
  virtual void TestZRemRangeByLex();
  virtual void TestZUnion();
  virtual void TestZInter();
  virtual void TestZDiff();

private:
  Slice key_;
};

class ZSetBasicImplTest: public ZSetTest {
public:
  ZSetBasicImplTest() {
    options.zset_impl = kZSetBasicImpl;
    db.Open(options, db_path);
  }
};

void ZSetTest::TestZMScore() {
  ASSERT_EQ(ZAdd({"-1", -1}), 1);
  ASSERT_EQ(ZAdd({"0", 0}), 1);
  ASSERT_EQ(ZAdd({"1", 1}), 1);

  ASSERT_EQ(ZMScore({"0"}), (ScoreOpts{0}));
  ASSERT_EQ(ZMScore({"-1", "1"}), (ScoreOpts{-1, 1}));
  ASSERT_EQ(ZMScore({"-1", "0", "1"}), (ScoreOpts{-1, 0, 1}));
  ASSERT_EQ(ZMScore({"0", "2"}), (ScoreOpts{0, std::nullopt}));
  ASSERT_EQ(ZMScore({"-2", "2"}), (ScoreOpts{std::nullopt, std::nullopt}));
}

void ZSetTest::TestZRank() {
  ASSERT_EQ(ZAdd({"0", 0}), 1);
  ASSERT_EQ(ZAdd({"1", 1}), 1);
  ASSERT_EQ(ZRank("0"), 0);
  ASSERT_EQ(ZRank("1"), 1);
  ASSERT_EQ(ZRevRank("0"), 1);
  ASSERT_EQ(ZRevRank("1"), 0);

  ASSERT_EQ(ZAdd({"-1", -1}), 1);
  ASSERT_EQ(ZRank("-1"), 0);
  ASSERT_EQ(ZRank("0"), 1);
  ASSERT_EQ(ZRank("1"), 2);
  ASSERT_EQ(ZRevRank("-1"), 2);
  ASSERT_EQ(ZRevRank("0"), 1);
  ASSERT_EQ(ZRevRank("1"), 0);

  ASSERT_EQ(ZAdd({"0", 2}), 0);
  ASSERT_EQ(ZRank("-1"), 0);
  ASSERT_EQ(ZRank("0"), 2);
  ASSERT_EQ(ZRank("1"), 1);
  ASSERT_EQ(ZRevRank("-1"), 2);
  ASSERT_EQ(ZRevRank("0"), 0);
  ASSERT_EQ(ZRevRank("1"), 1);

  ASSERT_EQ(ZAdd({"0", -2}), 0);
  ASSERT_EQ(ZRank({"-1"}), 1);
  ASSERT_EQ(ZRank({"0"}), 0);
  ASSERT_EQ(ZRank({"1"}), 2);
  ASSERT_EQ(ZRevRank({"-1"}), 1);
  ASSERT_EQ(ZRevRank({"0"}), 2);
  ASSERT_EQ(ZRevRank({"1"}), 0);

  Status s;
  uint64_t rank;
  s = db.ZRank("key", "2", &rank);
  ASSERT_MERODIS_IS_NOT_FOUND(s);
}

void ZSetTest::TestZCount() {
  ASSERT_EQ(ZAdd({"-1", -1}), 1);
  ASSERT_EQ(ZAdd({"0", 0}), 1);
  ASSERT_EQ(ZAdd({"1", 1}), 1);

  ASSERT_EQ(ZCount(-1, -2), 0);
  ASSERT_EQ(ZCount(-2, -1), 1);
  ASSERT_EQ(ZCount(-1, -1), 1);
  ASSERT_EQ(ZCount(-1, 0), 2);
  ASSERT_EQ(ZCount(-1, 1), 3);
  ASSERT_EQ(ZCount(-128, 127), 3);
  ASSERT_EQ(ZCount(minInt64, maxInt64), 3);
  ASSERT_EQ(ZCount(0, 1), 2);
  ASSERT_EQ(ZCount(1, 1), 1);
  ASSERT_EQ(ZCount(1, 2), 1);
  ASSERT_EQ(ZCount(2, 1), 0);
}

void ZSetTest::TestZLexCount() {
  ASSERT_EQ(ZAdd({"a", 0}), 1);
  ASSERT_EQ(ZAdd({"b", 0}), 1);
  ASSERT_EQ(ZAdd({"c", 0}), 1);

  ASSERT_EQ(ZLexCount("a", "0"), 0);
  ASSERT_EQ(ZLexCount("0", "a"), 1);
  ASSERT_EQ(ZLexCount("a", "a"), 1);
  ASSERT_EQ(ZLexCount("a", "b"), 2);
  ASSERT_EQ(ZLexCount("a", "c"), 3);
  ASSERT_EQ(ZLexCount("0", "z"), 3);
  ASSERT_EQ(ZLexCount(std::string(1, 0x00), std::string(1, 0xff)), 3);
  ASSERT_EQ(ZLexCount("b", "c"), 2);
  ASSERT_EQ(ZLexCount("c", "c"), 1);
  ASSERT_EQ(ZLexCount("c", "d"), 1);
  ASSERT_EQ(ZLexCount("d", "c"), 0);
}

void ZSetTest::TestZRange() {
  ASSERT_EQ(ZAdd({{"-1", -1}, {"0", 0}, {"1", 1}}), 3);

  ASSERT_EQ(ZRange(0, -4), LIST());
  ASSERT_EQ(ZRange(-4, 0), LIST("-1"));
  ASSERT_EQ(ZRange(0, 0), LIST("-1"));
  ASSERT_EQ(ZRange(0, 1), LIST("-1", "0"));
  ASSERT_EQ(ZRange(0, 2), LIST("-1", "0", "1"));
  ASSERT_EQ(ZRange(-128, 127), LIST("-1", "0", "1"));
  ASSERT_EQ(ZRange(minInt64, maxInt64), LIST("-1", "0", "1"));
  ASSERT_EQ(ZRange(-3, -1), LIST("-1", "0", "1"));
  ASSERT_EQ(ZRange(-2, -1), LIST("0", "1"));
  ASSERT_EQ(ZRange(-1, -1), LIST("1"));
  ASSERT_EQ(ZRange(-1, 4), LIST("1"));
  ASSERT_EQ(ZRange(-1, -2), LIST());

  ASSERT_EQ(ZRevRange(0, -1), LIST("1", "0", "-1"));
  ASSERT_EQ(ZRangeWithScores(0, -1), PAIRS({"-1", -1}, {"0", 0}, {"1", 1}));
  ASSERT_EQ(ZRevRangeWithScores(0, -1), PAIRS({"1", 1}, {"0", 0}, {"-1", -1}));
}

void ZSetTest::TestZRangeByScore() {
  ASSERT_EQ(ZAdd({{"-1", -1}, {"0", 0}, {"1", 1}}), 3);

  ASSERT_EQ(ZRangeByScore(-1, -2), LIST());
  ASSERT_EQ(ZRangeByScore(-2, -1), LIST("-1"));
  ASSERT_EQ(ZRangeByScore(-1, -1), LIST("-1"));
  ASSERT_EQ(ZRangeByScore(-1, 0), LIST("-1", "0"));
  ASSERT_EQ(ZRangeByScore(-1, 1), LIST("-1", "0", "1"));
  ASSERT_EQ(ZRangeByScore(-128, 127), LIST("-1", "0", "1"));
  ASSERT_EQ(ZRangeByScore(minInt64, maxInt64), LIST("-1", "0", "1"));
  ASSERT_EQ(ZRangeByScore(0, 1), LIST("0", "1"));
  ASSERT_EQ(ZRangeByScore(1, 1), LIST("1"));
  ASSERT_EQ(ZRangeByScore(1, 2), LIST("1"));
  ASSERT_EQ(ZRangeByScore(2, 1), LIST());

  ASSERT_EQ(ZRevRangeByScore(-1, 1), LIST("1", "0", "-1"));
  ASSERT_EQ(ZRangeByScoreWithScores(-1, 1), PAIRS({"-1", -1}, {"0", 0}, {"1", 1}));
  ASSERT_EQ(ZRevRangeByScoreWithScores(-1, 1), PAIRS({"1", 1}, {"0", 0}, {"-1", -1}));
}

void ZSetTest::TestZRangeByLex() {
  ASSERT_EQ(ZAdd({{"a", 0}, {"b", 0}, {"c", 0}}), 3);

  ASSERT_EQ(ZRangeByLex("a", "0"), LIST());
  ASSERT_EQ(ZRangeByLex("0", "a"), LIST("a"));
  ASSERT_EQ(ZRangeByLex("a", "a"), LIST("a"));
  ASSERT_EQ(ZRangeByLex("a", "b"), LIST("a", "b"));
  ASSERT_EQ(ZRangeByLex("a", "c"), LIST("a", "b", "c"));
  ASSERT_EQ(ZRangeByLex("0", "z"), LIST("a", "b", "c"));
  ASSERT_EQ(ZRangeByLex(std::string(1, 0x00), std::string(1, 0xff)), LIST("a", "b", "c"));
  ASSERT_EQ(ZRangeByLex("b", "c"), LIST("b", "c"));
  ASSERT_EQ(ZRangeByLex("c", "c"), LIST("c"));
  ASSERT_EQ(ZRangeByLex("c", "d"), LIST("c"));
  ASSERT_EQ(ZRangeByLex("d", "c"), LIST());

  ASSERT_EQ(ZRevRangeByLex("a", "c"), LIST("c", "b", "a"));
  ASSERT_EQ(ZRangeByLexWithScores("a", "c"), PAIRS({"a", 0}, {"b", 0}, {"c", 0}));
  ASSERT_EQ(ZRevRangeByLexWithScores("a", "c"), PAIRS({"c", 0}, {"b", 0}, {"a", 0}));
}

void ZSetTest::TestZAdd() {
  ASSERT_EQ(ZCard(), 0);

  ASSERT_EQ(ZAdd({"0", 0}), 1);
  ASSERT_EQ(ZCard(), 1);
  ASSERT_EQ(ZScore("0"), 0);

  ASSERT_EQ(ZAdd({"1", 1}), 1);
  ASSERT_EQ(ZCard(), 2);
  ASSERT_EQ(ZScore("1"), 1);

  ASSERT_EQ(ZAdd({"-1", -1}), 1);
  ASSERT_EQ(ZCard(), 3);
  ASSERT_EQ(ZScore("-1"), -1);

  ASSERT_EQ(ZAdd({"0", 2}), 0);
  ASSERT_EQ(ZCard(), 3);
  ASSERT_EQ(ZScore("0"), 2);

  ASSERT_EQ(ZAdd({"0", 2}), 0);
  ASSERT_EQ(ZCard(), 3);
  ASSERT_EQ(ZScore("0"), 2);
}

void ZSetTest::TestZAddN() {
  ASSERT_EQ(ZCard(), 0);

  ASSERT_EQ(ZAdd(std::map<Slice, int64_t>{}), 0);
  ASSERT_EQ(ZAdd({{"0", 0}}), 1);
  ASSERT_EQ(ZAdd({{"-1", -1}, {"1", 1}}), 2);
  ASSERT_EQ(ZAdd({{"0", 1}, {"2", 2}}), 1);
  ASSERT_EQ(ZAdd({{"1", -1}, {"-1", 1}}), 0);
  ASSERT_EQ(ZMScore({"-1", "0", "1", "2"}), (ScoreOpts{1, 1, -1, 2}));
}

void ZSetTest::TestZRem() {
  ASSERT_EQ(ZAdd({{"-1", -1}, {"0", 0}, {"1", 1}}), 3);
  ASSERT_EQ(ZCard(), 3);

  ASSERT_EQ(ZRem("0"), 1);
  ASSERT_EQ(ZCard(), 2);
  ASSERT_EQ(ZRange(0, -1), LIST("-1", "1"));

  ASSERT_EQ(ZRem("0"), 0);
  ASSERT_EQ(ZCard(), 2);
  ASSERT_EQ(ZRange(0, -1), LIST("-1", "1"));

  ASSERT_EQ(ZRem("-1"), 1);
  ASSERT_EQ(ZCard(), 1);
  ASSERT_EQ(ZRange(0, -1), LIST("1"));

  ASSERT_EQ(ZRem("1"), 1);
  ASSERT_EQ(ZCard(), 0);
  ASSERT_EQ(ZRange(0, -1), LIST());

  ASSERT_EQ(ZRem("1"), 0);
  ASSERT_EQ(ZCard(), 0);
  ASSERT_EQ(ZRange(0, -1), LIST());
}

void ZSetTest::TestZRemN() {
  ASSERT_EQ(ZAdd({{"-2", -2}, {"-1", -1}, {"0", 0}, {"1", 1}, {"2", 2}}), 5);
  ASSERT_EQ(ZCard(), 5);
  ASSERT_EQ(ZRange(0, -1), LIST("-2", "-1", "0", "1", "2"));

  ASSERT_EQ(ZRem(std::set<Slice>{}), 0);
  ASSERT_EQ(ZCard(), 5);
  ASSERT_EQ(ZRange(0, -1), LIST("-2", "-1", "0", "1", "2"));

  ASSERT_EQ(ZRem(std::set<Slice>{"0"}), 1);
  ASSERT_EQ(ZCard(), 4);
  ASSERT_EQ(ZRange(0, -1), LIST("-2", "-1", "1", "2"));

  ASSERT_EQ(ZRem({"-1", "1"}), 2);
  ASSERT_EQ(ZCard(), 2);
  ASSERT_EQ(ZRange(0, -1), LIST("-2", "2"));

  ASSERT_EQ(ZRem({"-2", "0", "2"}), 2);
  ASSERT_EQ(ZCard(), 0);
  ASSERT_EQ(ZRange(0, -1), LIST());

  ASSERT_EQ(ZRem({"-2", "0", "2"}), 0);
  ASSERT_EQ(ZCard(), 0);
  ASSERT_EQ(ZRange(0, -1), LIST());
}

void ZSetTest::TestZPopMax() {
  ASSERT_EQ(ZAdd({{"-1", -1}, {"0", 0}, {"1", 1}}), 3);
  ASSERT_EQ(ZCard(), 3);
  ASSERT_EQ(ZRange(0, -1), LIST("-1", "0", "1"));

  ASSERT_EQ(ZPopMax(), PAIR("1", 1));
  ASSERT_EQ(ZCard(), 2);
  ASSERT_EQ(ZRange(0, -1), LIST("-1", "0"));

  ASSERT_EQ(ZPopMax(), PAIR("0", 0));
  ASSERT_EQ(ZCard(), 1);
  ASSERT_EQ(ZRange(0, -1), LIST("-1"));

  ASSERT_EQ(ZPopMax(), PAIR("-1", -1));
  ASSERT_EQ(ZCard(), 0);
  ASSERT_EQ(ZRange(0, -1), LIST());

  ASSERT_EQ(ZPopMax(), (std::pair<std::string, int64_t>()));
  ASSERT_EQ(ZCard(), 0);
  ASSERT_EQ(ZRange(0, -1), LIST());
}

void ZSetTest::TestZPopMin() {
  ASSERT_EQ(ZAdd({{"-1", -1}, {"0", 0}, {"1", 1}}), 3);
  ASSERT_EQ(ZCard(), 3);
  ASSERT_EQ(ZRange(0, -1), LIST("-1", "0", "1"));

  ASSERT_EQ(ZPopMin(), PAIR("-1", -1));
  ASSERT_EQ(ZCard(), 2);
  ASSERT_EQ(ZRange(0, -1), LIST("0", "1"));

  ASSERT_EQ(ZPopMin(), PAIR("0", 0));
  ASSERT_EQ(ZCard(), 1);
  ASSERT_EQ(ZRange(0, -1), LIST("1"));

  ASSERT_EQ(ZPopMin(), PAIR("1", 1));
  ASSERT_EQ(ZCard(), 0);
  ASSERT_EQ(ZRange(0, -1), LIST());

  ASSERT_EQ(ZPopMin(), (std::pair<std::string, int64_t>()));
  ASSERT_EQ(ZCard(), 0);
  ASSERT_EQ(ZRange(0, -1), LIST());
}

void ZSetTest::TestZRemRangeByRank() {
  ASSERT_EQ(ZAdd({{"-2", -2}, {"-1", -1}, {"0", 0}, {"1", 1}, {"2", 2}}), 5);
  ASSERT_EQ(ZCard(), 5);
  ASSERT_EQ(ZRange(0, -1), LIST("-2", "-1", "0", "1", "2"));

  ASSERT_EQ(ZRemRangeByRank(1, 0), 0);
  ASSERT_EQ(ZCard(), 5);
  ASSERT_EQ(ZRange(0, -1), LIST("-2", "-1", "0", "1", "2"));

  ASSERT_EQ(ZRemRangeByRank(2, 2), 1);
  ASSERT_EQ(ZCard(), 4);
  ASSERT_EQ(ZRange(0, -1), LIST("-2", "-1", "1", "2"));

  ASSERT_EQ(ZRemRangeByRank(1, 2), 2);
  ASSERT_EQ(ZCard(), 2);
  ASSERT_EQ(ZRange(0, -1), LIST("-2", "2"));

  ASSERT_EQ(ZRemRangeByRank(0, 1), 2);
  ASSERT_EQ(ZCard(), 0);
  ASSERT_EQ(ZRange(0, -1), LIST());

  ASSERT_EQ(ZRemRangeByRank(0, 1), 0);
  ASSERT_EQ(ZCard(), 0);
  ASSERT_EQ(ZRange(0, -1), LIST());
}

void ZSetTest::TestZRemRangeByScore() {
  ASSERT_EQ(ZAdd({{"-2", -2}, {"-1", -1}, {"0", 0}, {"1", 1}, {"2", 2}}), 5);
  ASSERT_EQ(ZCard(), 5);
  ASSERT_EQ(ZRange(0, -1), LIST("-2", "-1", "0", "1", "2"));

  ASSERT_EQ(ZRemRangeByScore(3, 3), 0);
  ASSERT_EQ(ZCard(), 5);
  ASSERT_EQ(ZRange(0, -1), LIST("-2", "-1", "0", "1", "2"));

  ASSERT_EQ(ZRemRangeByScore(0, 0), 1);
  ASSERT_EQ(ZCard(), 4);
  ASSERT_EQ(ZRange(0, -1), LIST("-2", "-1", "1", "2"));

  ASSERT_EQ(ZRemRangeByScore(-1, 1), 2);
  ASSERT_EQ(ZCard(), 2);
  ASSERT_EQ(ZRange(0, -1), LIST("-2", "2"));

  ASSERT_EQ(ZRemRangeByScore(-2, 2), 2);
  ASSERT_EQ(ZCard(), 0);
  ASSERT_EQ(ZRange(0, -1), LIST());

  ASSERT_EQ(ZRemRangeByScore(-2, 2), 0);
  ASSERT_EQ(ZCard(), 0);
  ASSERT_EQ(ZRange(0, -1), LIST());
}

void ZSetTest::TestZRemRangeByLex() {
  ASSERT_EQ(ZAdd({{"a", 0}, {"b", 0}, {"c", 0}, {"d", 0}, {"e", 0}}), 5);
  ASSERT_EQ(ZCard(), 5);
  ASSERT_EQ(ZRange(0, -1), LIST("a", "b", "c", "d", "e"));

  ASSERT_EQ(ZRemRangeByLex("f", "f"), 0);
  ASSERT_EQ(ZCard(), 5);
  ASSERT_EQ(ZRange(0, -1), LIST("a", "b", "c", "d", "e"));

  ASSERT_EQ(ZRemRangeByLex("c", "c"), 1);
  ASSERT_EQ(ZCard(), 4);
  ASSERT_EQ(ZRange(0, -1), LIST("a", "b", "d", "e"));

  ASSERT_EQ(ZRemRangeByLex("b", "d"), 2);
  ASSERT_EQ(ZCard(), 2);
  ASSERT_EQ(ZRange(0, -1), LIST("a", "e"));

  ASSERT_EQ(ZRemRangeByLex("a", "e"), 2);
  ASSERT_EQ(ZCard(), 0);
  ASSERT_EQ(ZRange(0, -1), LIST());

  ASSERT_EQ(ZRemRangeByLex("a", "e"), 0);
  ASSERT_EQ(ZCard(), 0);
  ASSERT_EQ(ZRange(0, -1), LIST());
}

void ZSetTest::TestZUnion() {
  ASSERT_EQ(ZAdd("z0", {{"a", 0}, {"b", 1}}), 2);
  ASSERT_EQ(ZAdd("z1", {{"a", 0}, {"b", 1}, {"c", 2}}), 3);
  ASSERT_EQ(ZAdd("z2", {{"b", 0}, {"c", 1}, {"d", 2}}), 3);
  ASSERT_EQ(ZAdd("z3", {{"c", 0}, {"d", 1}}), 2);

  ASSERT_EQ(ZUnion({"z0", "zx"}), LIST("a", "b"));
  ASSERT_EQ(ZUnion({"z0", "z1"}), LIST("a", "b", "c"));
  ASSERT_EQ(ZUnion({"z0", "z2"}), LIST("a", "b", "c", "d"));
  ASSERT_EQ(ZUnion({"z0", "z3"}), LIST("a", "c", "b", "d"));
  ASSERT_EQ(ZUnion({"z0", "z1", "z2", "z3"}), LIST("a", "b", "c", "d"));
  ASSERT_EQ(ZUnionWithScores({"z0", "zx"}), PAIRS({"a", 0}, {"b", 1}));
  ASSERT_EQ(ZUnionWithScores({"z0", "z1"}), PAIRS({"a", 0}, {"b", 2}, {"c", 2}));
  ASSERT_EQ(ZUnionWithScores({"z0", "z2"}), PAIRS({"a", 0}, {"b", 1}, {"c", 1}, {"d", 2}));
  ASSERT_EQ(ZUnionWithScores({"z0", "z3"}), PAIRS({"a", 0}, {"c", 0}, {"b", 1}, {"d", 1}));
  ASSERT_EQ(ZUnionWithScores({"z0", "z1", "z2", "z3"}), PAIRS({"a", 0}, {"b", 2}, {"c", 3}, {"d", 3}));

  ASSERT_EQ(ZUnionStore({"z0", "z2"}, "zx"), 4);
  ASSERT_EQ(ZRangeWithScores("zx", 0, -1), PAIRS({"a", 0}, {"b", 1}, {"c", 1}, {"d", 2}));
  ASSERT_EQ(ZUnionStore({"z0", "z3"}, "z0"), 4);
  ASSERT_EQ(ZRangeWithScores("z0", 0, -1), PAIRS({"a", 0}, {"c", 0}, {"b", 1}, {"d", 1}));
}

void ZSetTest::TestZInter() {
  ASSERT_EQ(ZAdd("z0", {{"a", 0}, {"b", 1}}), 2);
  ASSERT_EQ(ZAdd("z1", {{"a", 0}, {"b", 1}, {"c", 2}}), 3);
  ASSERT_EQ(ZAdd("z2", {{"b", 0}, {"c", 1}, {"d", 2}}), 3);
  ASSERT_EQ(ZAdd("z3", {{"c", 0}, {"d", 1}}), 2);

  ASSERT_EQ(ZInter({"z0", "zx"}), LIST());
  ASSERT_EQ(ZInter({"z0", "z1"}), LIST("a", "b"));
  ASSERT_EQ(ZInter({"z0", "z2"}), LIST("b"));
  ASSERT_EQ(ZInter({"z0", "z3"}), LIST());
  ASSERT_EQ(ZInter({"z0", "z1", "z2", "z3"}), LIST());
  ASSERT_EQ(ZInterWithScores({"z0", "zx"}), PAIRS());
  ASSERT_EQ(ZInterWithScores({"z0", "z1"}), PAIRS({"a", 0}, {"b", 2}));
  ASSERT_EQ(ZInterWithScores({"z0", "z2"}), PAIRS({"b", 1}));
  ASSERT_EQ(ZInterWithScores({"z0", "z3"}), PAIRS());
  ASSERT_EQ(ZInterWithScores({"z0", "z1", "z2", "z3"}), PAIRS());
}

void ZSetTest::TestZDiff() {
  ASSERT_EQ(ZAdd("z0", {{"a", 0}, {"b", 1}}), 2);
  ASSERT_EQ(ZAdd("z1", {{"a", 0}, {"b", 1}, {"c", 2}}), 3);
  ASSERT_EQ(ZAdd("z2", {{"b", 0}, {"c", 1}, {"d", 2}}), 3);
  ASSERT_EQ(ZAdd("z3", {{"c", 0}, {"d", 1}}), 2);

  ASSERT_EQ(ZDiff({"z0", "zx"}), LIST("a", "b"));
  ASSERT_EQ(ZDiff({"z0", "z1"}), LIST());
  ASSERT_EQ(ZDiff({"z0", "z2"}), LIST("a"));
  ASSERT_EQ(ZDiff({"z0", "z3"}), LIST("a", "b"));
  ASSERT_EQ(ZDiff({"z0", "z1", "z2", "z3"}), LIST());
  ASSERT_EQ(ZDiffWithScores({"z0", "zx"}), PAIRS({"a", 0}, {"b", 1}));
  ASSERT_EQ(ZDiffWithScores({"z0", "z1"}), PAIRS());
  ASSERT_EQ(ZDiffWithScores({"z0", "z2"}), PAIRS({"a", 0}));
  ASSERT_EQ(ZDiffWithScores({"z0", "z3"}), PAIRS({"a", 0}, {"b", 1}));
  ASSERT_EQ(ZDiffWithScores({"z0", "z1", "z2", "z3"}), PAIRS());
}

TEST_F(ZSetBasicImplTest, ZMScore) {
  TestZMScore();
}

TEST_F(ZSetBasicImplTest, ZRank) {
  TestZRank();
}

TEST_F(ZSetBasicImplTest, ZCount) {
  TestZCount();
}

TEST_F(ZSetBasicImplTest, ZLexCount) {
  TestZLexCount();
}

TEST_F(ZSetBasicImplTest, ZRange) {
  TestZRange();
}

TEST_F(ZSetBasicImplTest, ZRangeByScore) {
  TestZRangeByScore();
}

TEST_F(ZSetBasicImplTest, ZRangeByLex) {
  TestZRangeByLex();
}

TEST_F(ZSetBasicImplTest, ZAdd) {
  TestZAdd();
}

TEST_F(ZSetBasicImplTest, ZAddN) {
  TestZAddN();
}

TEST_F(ZSetBasicImplTest, ZRem) {
  TestZRem();
}

TEST_F(ZSetBasicImplTest, ZRemN) {
  TestZRemN();
}

TEST_F(ZSetBasicImplTest, ZPopMax) {
  TestZPopMax();
}

TEST_F(ZSetBasicImplTest, ZPopMin) {
  TestZPopMin();
}

TEST_F(ZSetBasicImplTest, ZRemRangeByRank) {
  TestZRemRangeByRank();
}

TEST_F(ZSetBasicImplTest, ZRemRangeByScore) {
  TestZRemRangeByScore();
}

TEST_F(ZSetBasicImplTest, ZRemRangeByLex) {
  TestZRemRangeByLex();
}

TEST_F(ZSetBasicImplTest, ZUnion) {
  TestZUnion();
}

TEST_F(ZSetBasicImplTest, ZInter) {
  TestZInter();
}

TEST_F(ZSetBasicImplTest, ZDiff) {
  TestZDiff();
}

}
}