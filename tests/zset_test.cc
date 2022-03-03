#include <vector>
#include "gtest/gtest.h"
#include "merodis/merodis.h"
#include "common.h"
#include "testutil.h"

namespace merodis {
namespace test {

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
  std::vector<int64_t> ZMScore(const Slice& key, const std::vector<Slice>& members) {
    std::vector<int64_t> scores;
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
  std::vector<int64_t> ZMScore(const std::vector<Slice>& members) { return ZMScore(key_, members); }
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

}
}