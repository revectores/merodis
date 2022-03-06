#ifndef MERODIS_REDIS_ZSET_H
#define MERODIS_REDIS_ZSET_H

#include <cstdint>
#include <string>
#include <vector>
#include <map>
#include <set>
#include <utility>

#include "redis.h"
#include "util/coding.h"

namespace merodis {

class RedisZSet : public Redis {
public:
  RedisZSet() noexcept = default;
  ~RedisZSet() noexcept override = default;

  virtual Status ZCard(const Slice& key, uint64_t* len) = 0;
  virtual Status ZScore(const Slice& key, const Slice& member, int64_t* score) = 0;
  virtual Status ZMScore(const Slice& key, const std::vector<Slice>& members, ScoreOpts* scores) = 0;
  virtual Status ZRank(const Slice& key, const Slice& member, uint64_t* rank) = 0;
  virtual Status ZRevRank(const Slice& key, const Slice& member, uint64_t* rank) = 0;
  virtual Status ZCount(const Slice& key, int64_t minScore, int64_t maxScore, uint64_t* count) = 0;
  virtual Status ZLexCount(const Slice& key, const Slice& minLex, const Slice& maxLex, uint64_t* count) = 0;
  virtual Status ZRange(const Slice& key, int64_t minRank, int64_t maxRank, Members* members) = 0;
  virtual Status ZRangeByScore(const Slice& key, int64_t minScore, int64_t maxScore, Members* members) = 0;
  virtual Status ZRangeByLex(const Slice& key, const Slice& minLex, const Slice& maxLex, Members* members) = 0;
  virtual Status ZRangeWithScores(const Slice& key, int64_t minRank, int64_t maxRank, ScoredMembers* scoredMembers) = 0;
  virtual Status ZRangeByScoreWithScores(const Slice& key, int64_t minScore, int64_t maxScore, ScoredMembers* scoredMembers) = 0;
  virtual Status ZRangeByLexWithScores(const Slice& key, const Slice& minLex, const Slice& maxLex, ScoredMembers* scoredMembers) = 0;
  virtual Status ZRevRange(const Slice& key, int64_t minRank, int64_t maxRank, Members* members) = 0;
  virtual Status ZRevRangeByScore(const Slice& key, int64_t minScore, int64_t maxScore, Members* members) = 0;
  virtual Status ZRevRangeByLex(const Slice& key, const Slice& minLex, const Slice& maxLex, Members* members) = 0;
  virtual Status ZRevRangeWithScores(const Slice& key, int64_t minRank, int64_t maxRank, ScoredMembers* scoredMembers) = 0;
  virtual Status ZRevRangeByScoreWithScores(const Slice& key, int64_t minScore, int64_t maxScore, ScoredMembers* scoredMembers) = 0;
  virtual Status ZRevRangeByLexWithScores(const Slice& key, const Slice& minLex, const Slice& maxLex, ScoredMembers* scoredMembers) = 0;

  virtual Status ZAdd(const Slice& key, const std::pair<Slice, int64_t>& scoredMember, uint64_t* count) = 0;
  virtual Status ZAdd(const Slice& key, const std::map<Slice, int64_t>& scoredMembers, uint64_t* count) = 0;
  virtual Status ZRem(const Slice& key, const Slice& member, uint64_t* count) = 0;
  virtual Status ZRem(const Slice& key, const std::set<Slice>& members, uint64_t* count) = 0;
  virtual Status ZPopMax(const Slice& key, ScoredMember* scoredMember) = 0;
  virtual Status ZPopMin(const Slice& key, ScoredMember* scoredMember) = 0;
  virtual Status ZRemRangeByRank(const Slice& key, int64_t minRank, int64_t maxRank, uint64_t* count) = 0;
  virtual Status ZRemRangeByScore(const Slice& key, int64_t minScore, int64_t maxScore, uint64_t* count) = 0;
  virtual Status ZRemRangeByLex(const Slice& key, const Slice& minLex, const Slice& maxLex, uint64_t* count) = 0;

  virtual Status ZUnion(const std::vector<Slice>& keys, Members* members) = 0;
  virtual Status ZInter(const std::vector<Slice>& keys, Members* members) = 0;
  virtual Status ZDiff(const std::vector<Slice>& keys, Members* members) = 0;
  virtual Status ZUnionWithScores(const std::vector<Slice>& keys, ScoredMembers* scoredMembers) = 0;
  virtual Status ZInterWithScores(const std::vector<Slice>& keys, ScoredMembers* scoredMembers) = 0;
  virtual Status ZDiffWithScores(const std::vector<Slice>& keys, ScoredMembers* scoredMembers) = 0;
  virtual Status ZUnionStore(const std::vector<Slice>& keys, const Slice& dstKey, uint64_t* count) = 0;
  virtual Status ZInterStore(const std::vector<Slice>& keys, const Slice& dstKey, uint64_t* count) = 0;
  virtual Status ZDiffStore(const std::vector<Slice>& keys, const Slice& dstKey, uint64_t* count) = 0;
};

}


#endif //MERODIS_REDIS_ZSET_H
