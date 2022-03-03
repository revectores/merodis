#ifndef MERODIS_REDIS_ZSET_BASIC_IMPL_H
#define MERODIS_REDIS_ZSET_BASIC_IMPL_H

#include "redis_zset.h"
#include "util/coding.h"

namespace merodis {

class RedisZSetBasicImpl final : public RedisZSet {
public:
  RedisZSetBasicImpl() noexcept;
  ~RedisZSetBasicImpl() noexcept final;

  Status ZCard(const Slice& key, uint64_t* len) final;
  Status ZScore(const Slice& key, const Slice& member, int64_t* score) final;
  Status ZMScore(const Slice& key, const std::vector<Slice>& members, std::vector<int64_t>* scores) final;
  Status ZRank(const Slice& key, const Slice& member, uint64_t* rank) final;
  Status ZRevRank(const Slice& key, const Slice& member, uint64_t* rank) final;
  Status ZCount(const Slice& key, int64_t minScore, int64_t maxScore, uint64_t* count) final;
  Status ZLexCount(const Slice& key, const Slice& minLex, const Slice& maxLex, uint64_t* count) final;
  Status ZRange(const Slice& key, int64_t minRank, int64_t maxRank, Members* members) final;
  Status ZRangeByScore(const Slice& key, int64_t minScore, int64_t maxScore, Members* members) final;
  Status ZRangeByLex(const Slice& key, const Slice& minLex, const Slice& maxLex, Members* members) final;
  Status ZRangeWithScores(const Slice& key, int64_t minRank, int64_t maxRank, ScoredMembers* scoredMembers) final;
  Status ZRangeByScoreWithScores(const Slice& key, int64_t minScore, int64_t maxScore, ScoredMembers* scoredMembers) final;
  Status ZRangeByLexWithScores(const Slice& key, const Slice& minLex, const Slice& maxLex, ScoredMembers* scoredMembers) final;
  Status ZRevRange(const Slice& key, int64_t minRank, int64_t maxRank, Members* members) final;
  Status ZRevRangeByScore(const Slice& key, int64_t minScore, int64_t maxScore, Members* members) final;
  Status ZRevRangeByLex(const Slice& key, const Slice& minLex, const Slice& maxLex, Members* members) final;
  Status ZRevRangeWithScores(const Slice& key, int64_t minRank, int64_t maxRank, ScoredMembers* scoredMembers) final;
  Status ZRevRangeByScoreWithScores(const Slice& key, int64_t minScore, int64_t maxScore, ScoredMembers* scoredMembers) final;
  Status ZRevRangeByLexWithScores(const Slice& key, const Slice& minLex, const Slice& maxLex, ScoredMembers* scoredMembers) final;

  Status ZAdd(const Slice& key, const std::pair<Slice, int64_t>& scoredMember, uint64_t* count) final;
  Status ZAdd(const Slice& key, const std::map<Slice, int64_t>& scoredMembers, uint64_t* count) final;
  Status ZRem(const Slice& key, const Slice& member, uint64_t* count) final;
  Status ZRem(const Slice& key, const std::set<Slice>& members, uint64_t* count) final;
  Status ZPopMax(const Slice& key, ScoredMember* scoredMember) final;
  Status ZPopMin(const Slice& key, ScoredMember* scoredMember) final;
  Status ZRemRangeByRank(const Slice& key, int64_t minRank, int64_t maxRank, uint64_t* count) final;
  Status ZRemRangeByScore(const Slice& key, int64_t minScore, int64_t maxScore, uint64_t* count) final;
  Status ZRemRangeByLex(const Slice& key, const Slice& minLex, const Slice& maxLex, uint64_t* count) final;

  Status ZUnion(const std::vector<Slice>& keys, Members* members) final;
  Status ZInter(const std::vector<Slice>& keys, Members* members) final;
  Status ZDiff(const std::vector<Slice>& keys, Members* members) final;
  Status ZUnionWithScores(const std::vector<Slice>& keys, ScoredMembers* scoredMembers) final;
  Status ZInterWithScores(const std::vector<Slice>& keys, ScoredMembers* scoredMembers) final;
  Status ZDiffWithScores(const std::vector<Slice>& keys, ScoredMembers* scoredMembers) final;
  Status ZUnionStore(const std::vector<Slice>& keys, const Slice& dstKey, uint64_t* count) final;
  Status ZInterStore(const std::vector<Slice>& keys, const Slice& dstKey, uint64_t* count) final;
  Status ZDiffStore(const std::vector<Slice>& keys, const Slice& dstKey, uint64_t* count) final;
};

}

#endif //MERODIS_REDIS_ZSET_BASIC_IMPL_H
