#include "redis_zset_basic_impl.h"

#include "leveldb/write_batch.h"

namespace merodis {

RedisZSetBasicImpl::RedisZSetBasicImpl() noexcept = default;
RedisZSetBasicImpl::~RedisZSetBasicImpl() noexcept = default;

Status RedisZSetBasicImpl::ZCard(const Slice& key, uint64_t* len){
  std::string rawZSetMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawZSetMetaValue);
  if (s.ok()) {
    ZSetMetaValue metaValue(rawZSetMetaValue);
    *len = metaValue.len;
  } else if (s.IsNotFound()) {
    *len = 0;
    s = Status::OK();
  }
  return s;
}

Status RedisZSetBasicImpl::ZScore(const Slice& key,
                                  const Slice& member,
                                  int64_t* score){
  ZSetMemberKey memberKey(key, member);
  std::string rawMemberValue;
  Status s = db_->Get(ReadOptions(), memberKey.Encode(), &rawMemberValue);
  if (!s.ok()) return s;
  ZSetMemberValue memberValue(rawMemberValue);
  *score = memberValue.score();
  return Status::OK();
}

Status RedisZSetBasicImpl::ZMScore(const Slice& key,
                                   const std::vector<Slice>& members,
                                   ScoreOpts* scores){
  std::map<Slice, std::optional<int64_t>> member2score;
  for (auto& member: members) member2score[member] = std::nullopt;
  MemberIterator mIter(db_, key);
  if (!mIter.Valid()) {
    *scores = ScoreOpts(members.size(), std::nullopt);
    return Status::OK();
  }

  auto queryIter = member2score.begin();
  while (mIter.Valid() && queryIter != member2score.end()) {
    int r = queryIter->first.compare(mIter.member());
    if (r == 0) {
      queryIter->second = mIter.score();
      ++queryIter;
      mIter.Next();
    } else if (r > 0) {
      mIter.Next();
    } else {
      ++queryIter;
    }
  }

  scores->reserve(members.size());
  for (auto& member: members) scores->push_back(member2score[member]);
  return Status::OK();
}

Status RedisZSetBasicImpl::ZRank(const Slice& key,
                                 const Slice& member,
                                 uint64_t* rank){
  return ZRankInternal(key, member, rank, false);
}

Status RedisZSetBasicImpl::ZRevRank(const Slice& key,
                                 const Slice& member,
                                 uint64_t* rank){
  return ZRankInternal(key, member, rank, true);
}

Status RedisZSetBasicImpl::ZCount(const Slice& key,
                                  int64_t minScore,
                                  int64_t maxScore,
                                  uint64_t* count){
  *count = 0;
  if (minScore > maxScore) return Status::OK();
  ScoredMemberIterator smIter(db_, key);
  if (!smIter.Valid()) return Status::OK();
  smIter.Next();
  for (; smIter.Valid() && smIter.score() < minScore; smIter.Next());
  for (; smIter.Valid() && smIter.score() <= maxScore; smIter.Next(), *count += 1);
  return Status::OK();
}

Status RedisZSetBasicImpl::ZLexCount(const Slice& key,
                                     const Slice& minLex,
                                     const Slice& maxLex,
                                     uint64_t* count){
  *count = 0;
  if (minLex > maxLex) return Status::OK();
  MemberIterator mIter(db_, key);
  if (!mIter.Valid()) return Status::OK();
  for (; mIter.Valid() && mIter.member() < minLex; mIter.Next());
  for (; mIter.Valid() && mIter.member() <= maxLex; mIter.Next(), *count += 1);
  return Status::OK();
}

Status RedisZSetBasicImpl::ZRange(const Slice& key,
                                  int64_t minRank,
                                  int64_t maxRank,
                                  Members* members){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZRangeByScore(const Slice& key,
                                         int64_t minScore,
                                         int64_t maxScore,
                                         Members* members){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZRangeByLex(const Slice& key,
                                       const Slice& minLex,
                                       const Slice& maxLex,
                                       Members* members){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZRangeWithScores(const Slice& key,
                                            int64_t minRank,
                                            int64_t maxRank,
                                            ScoredMembers* scoredMembers){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZRangeByScoreWithScores(const Slice& key,
                                                   int64_t minScore,
                                                   int64_t maxScore,
                                                   ScoredMembers* scoredMembers){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZRangeByLexWithScores(const Slice& key,
                                                 const Slice& minLex,
                                                 const Slice& maxLex,
                                                 ScoredMembers* scoredMembers){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZRevRange(const Slice& key,
                                     int64_t minRank,
                                     int64_t maxRank,
                                     Members* members){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZRevRangeByScore(const Slice& key,
                                            int64_t minScore,
                                            int64_t maxScore,
                                            Members* members){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZRevRangeByLex(const Slice& key,
                                          const Slice& minLex,
                                          const Slice& maxLex,
                                          Members* members){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZRevRangeWithScores(const Slice& key,
                                               int64_t minRank,
                                               int64_t maxRank,
                                               ScoredMembers* scoredMembers){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZRevRangeByScoreWithScores(const Slice& key,
                                                      int64_t minScore,
                                                      int64_t maxScore,
                                                      ScoredMembers* scoredMembers){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZRevRangeByLexWithScores(const Slice& key,
                                                    const Slice& minLex,
                                                    const Slice& maxLex,
                                                    ScoredMembers* scoredMembers){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZAdd(const Slice& key,
                                const std::pair<Slice, int64_t>& scoredMember,
                                uint64_t* count){
  std::string rawZSetMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawZSetMetaValue);
  ZSetMetaValue zsetMetaValue;
  if (s.ok()) zsetMetaValue = ZSetMetaValue(rawZSetMetaValue);

  WriteBatch updates;
  const auto& [member, score] = scoredMember;
  ZSetMemberKey memberKey(key, member);
  ZSetMemberValue memberValue(score);
  ZSetScoredMemberKey scoredMemberKey(key, scoredMember);

  std::string rawMemberValue;
  s = db_->Get(ReadOptions(), memberKey.Encode(), &rawMemberValue);
  if (s.ok()) {
    *count = 0;
    ZSetMemberValue oldMemberValue(rawMemberValue);
    int64_t oldScore = oldMemberValue.score();
    if (oldScore == score) return Status::OK();
    ZSetScoredMemberKey oldScoredMemberKey(key, member, oldScore);
    updates.Delete(oldScoredMemberKey.Encode());
  } else {
    *count = 1;
    zsetMetaValue.len += 1;
    updates.Put(key, zsetMetaValue.Encode());
  }
  updates.Put(memberKey.Encode(), memberValue.Encode());
  updates.Put(scoredMemberKey.Encode(), "");
  return db_->Write(WriteOptions(), &updates);
}

Status RedisZSetBasicImpl::ZAdd(const Slice& key,
                                const std::map<Slice, int64_t>& scoredMembers,
                                uint64_t* count){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZRem(const Slice& key,
                                const Slice& member,
                                uint64_t* count){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZRem(const Slice& key,
                                const std::set<Slice>& members,
                                uint64_t* count){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZPopMax(const Slice& key,
                                   ScoredMember* scoredMember){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZPopMin(const Slice& key,
                                   ScoredMember* scoredMember){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZRemRangeByRank(const Slice& key,
                                           int64_t minRank,
                                           int64_t maxRank,
                                           uint64_t* count){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZRemRangeByScore(const Slice& key,
                                            int64_t minScore,
                                            int64_t maxScore,
                                            uint64_t* count){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZRemRangeByLex(const Slice& key,
                                          const Slice& minLex,
                                          const Slice& maxLex,
                                          uint64_t* count){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZUnion(const std::vector<Slice>& keys,
                                  Members* members){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZInter(const std::vector<Slice>& keys,
                                  Members* members){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZDiff(const std::vector<Slice>& keys,
                                 Members* members){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZUnionWithScores(const std::vector<Slice>& keys,
                                            ScoredMembers* scoredMembers){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZInterWithScores(const std::vector<Slice>& keys,
                                            ScoredMembers* scoredMembers){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZDiffWithScores(const std::vector<Slice>& keys,
                                           ScoredMembers* scoredMembers){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZUnionStore(const std::vector<Slice>& keys,
                                       const Slice& dstKey,
                                       uint64_t* count){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZInterStore(const std::vector<Slice>& keys,
                                       const Slice& dstKey,
                                       uint64_t* count){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZDiffStore(const std::vector<Slice>& keys,
                                      const Slice& dstKey,
                                      uint64_t* count){
	return Status::NotSupported("");
}



Status RedisZSetBasicImpl::ZRankInternal(const Slice& key,
                                         const Slice& member,
                                         uint64_t* rank,
                                         bool rev) {
  ZSetMemberKey memberKey(key, member);
  std::string rawMemberValue;
  Status s = db_->Get(ReadOptions(), memberKey.Encode(), &rawMemberValue);
  if (!s.ok()) return s;

  ScoredMemberIterator smIter(db_, key);
  if (!smIter.Valid()) return Status::NotFound("empty zset");
  ZSetMetaValue metaValue(smIter.value().ToString());
  smIter.Next();
  for (*rank = 0;
       smIter.Valid() && smIter.member() != member;
       smIter.Next(), ++*rank);
  if (rev) *rank = metaValue.len - 1 - *rank;
  return Status::OK();
}

}
