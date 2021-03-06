#include "redis_zset_basic_impl.h"

#include <cstdint>
#include <string>
#include <vector>
#include <map>
#include <set>
#include <utility>
#include <algorithm>

namespace merodis {

RedisZSetBasicImpl::RedisZSetBasicImpl() noexcept = default;
RedisZSetBasicImpl::~RedisZSetBasicImpl() noexcept = default;

Status RedisZSetBasicImpl::Del(const Slice& key) {
  WriteBatch updates;
  ScoredMemberIterator smIter(db_, key);
  if (!smIter.Valid()) return Status::OK();
  updates.Delete(smIter.key());

  for (smIter.Next(); smIter.Valid(); smIter.Next()) {
    updates.Delete(smIter.key());
    updates.Delete(ZSetMemberKey(key, smIter.member()).Encode());
  }
  return db_->Write(WriteOptions(), &updates);
}

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
  ScoredMemberIterator smIter(db_, key);
  if (!smIter.Valid()) return Status::OK();
  ZSetMetaValue metaValue(smIter.value().ToString());
  int64_t size = static_cast<int64_t>(metaValue.len);
  uint64_t lower = std::max((int64_t)0, minRank >= 0 ? minRank : size + minRank);
  uint64_t upper = std::min(size - 1, maxRank >= 0 ? maxRank : size + maxRank);
  uint64_t rangeSize = upper - lower + 1;
  if (rangeSize < 0) return Status::OK();
  smIter.Next();
  for (; smIter.Valid() && lower--; smIter.Next());
  for (; smIter.Valid() && rangeSize--; smIter.Next()) {
    members->push_back(smIter.member().ToString());
  }
  return Status::OK();
}

Status RedisZSetBasicImpl::ZRangeByScore(const Slice& key,
                                         int64_t minScore,
                                         int64_t maxScore,
                                         Members* members){
  if (minScore > maxScore) return Status::OK();
  ScoredMemberIterator smIter(db_, key);
  if (!smIter.Valid()) return Status::OK();
  smIter.Next();
  for (; smIter.Valid() && smIter.score() < minScore; smIter.Next());
  for (; smIter.Valid() && smIter.score() <= maxScore; smIter.Next()) {
    members->push_back(smIter.member().ToString());
  }
  return Status::OK();
}

Status RedisZSetBasicImpl::ZRangeByLex(const Slice& key,
                                       const Slice& minLex,
                                       const Slice& maxLex,
                                       Members* members){
  if (minLex > maxLex) return Status::OK();
  MemberIterator mIter(db_, key);
  if (!mIter.Valid()) return Status::OK();
  for (; mIter.Valid() && mIter.member() < minLex; mIter.Next());
  for (; mIter.Valid() && mIter.member() <= maxLex; mIter.Next()) {
    members->push_back(mIter.member().ToString());
  }
  return Status::OK();
}

Status RedisZSetBasicImpl::ZRangeWithScores(const Slice& key,
                                            int64_t minRank,
                                            int64_t maxRank,
                                            ScoredMembers* scoredMembers){
  ScoredMemberIterator smIter(db_, key);
  if (!smIter.Valid()) return Status::OK();
  ZSetMetaValue metaValue(smIter.value().ToString());
  int64_t size = static_cast<int64_t>(metaValue.len);
  uint64_t lower = std::max((int64_t)0, minRank >= 0 ? minRank : size + minRank);
  uint64_t upper = std::min(size - 1, maxRank >= 0 ? maxRank : size + maxRank);
  uint64_t rangeSize = upper - lower + 1;
  if (rangeSize < 0) return Status::OK();
  smIter.Next();
  for (; smIter.Valid() && lower--; smIter.Next());
  for (; smIter.Valid() && rangeSize--; smIter.Next()) {
    scoredMembers->push_back({smIter.member().ToString(), smIter.score()});
  }
  return Status::OK();
}

Status RedisZSetBasicImpl::ZRangeByScoreWithScores(const Slice& key,
                                                   int64_t minScore,
                                                   int64_t maxScore,
                                                   ScoredMembers* scoredMembers){
  if (minScore > maxScore) return Status::OK();
  ScoredMemberIterator smIter(db_, key);
  if (!smIter.Valid()) return Status::OK();
  smIter.Next();
  for (; smIter.Valid() && smIter.score() < minScore; smIter.Next());
  for (; smIter.Valid() && smIter.score() <= maxScore; smIter.Next()) {
    scoredMembers->push_back({smIter.member().ToString(), smIter.score()});
  }
  return Status::OK();
}

Status RedisZSetBasicImpl::ZRangeByLexWithScores(const Slice& key,
                                                 const Slice& minLex,
                                                 const Slice& maxLex,
                                                 ScoredMembers* scoredMembers){
  if (minLex > maxLex) return Status::OK();
  MemberIterator mIter(db_, key);
  if (!mIter.Valid()) return Status::OK();
  for (; mIter.Valid() && mIter.member() < minLex; mIter.Next());
  for (; mIter.Valid() && mIter.member() <= maxLex; mIter.Next()) {
    scoredMembers->push_back({mIter.member().ToString(), mIter.score()});
  }
  return Status::OK();
}

Status RedisZSetBasicImpl::ZRevRange(const Slice& key,
                                     int64_t minRank,
                                     int64_t maxRank,
                                     Members* members){
  Status s = ZRange(key, minRank, maxRank, members);
  if (!s.ok()) return s;
  std::reverse(members->begin(), members->end());
  return s;
}

Status RedisZSetBasicImpl::ZRevRangeByScore(const Slice& key,
                                            int64_t minScore,
                                            int64_t maxScore,
                                            Members* members){
  Status s = ZRangeByScore(key, minScore, maxScore, members);
  if (!s.ok()) return s;
  std::reverse(members->begin(), members->end());
  return s;
}

Status RedisZSetBasicImpl::ZRevRangeByLex(const Slice& key,
                                          const Slice& minLex,
                                          const Slice& maxLex,
                                          Members* members){
  Status s = ZRangeByLex(key, minLex, maxLex, members);
  if (!s.ok()) return s;
  std::reverse(members->begin(), members->end());
  return s;
}

Status RedisZSetBasicImpl::ZRevRangeWithScores(const Slice& key,
                                               int64_t minRank,
                                               int64_t maxRank,
                                               ScoredMembers* scoredMembers){
  Status s = ZRangeWithScores(key, minRank, maxRank, scoredMembers);
  if (!s.ok()) return s;
  std::reverse(scoredMembers->begin(), scoredMembers->end());
  return s;
}

Status RedisZSetBasicImpl::ZRevRangeByScoreWithScores(const Slice& key,
                                                      int64_t minScore,
                                                      int64_t maxScore,
                                                      ScoredMembers* scoredMembers){
  Status s = ZRangeByScoreWithScores(key, minScore, maxScore, scoredMembers);
  if (!s.ok()) return s;
  std::reverse(scoredMembers->begin(), scoredMembers->end());
  return s;
}

Status RedisZSetBasicImpl::ZRevRangeByLexWithScores(const Slice& key,
                                                    const Slice& minLex,
                                                    const Slice& maxLex,
                                                    ScoredMembers* scoredMembers){
  Status s = ZRangeByLexWithScores(key, minLex, maxLex, scoredMembers);
  if (!s.ok()) return s;
  std::reverse(scoredMembers->begin(), scoredMembers->end());
  return s;
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
  *count = 0;
  if (scoredMembers.empty()) return Status::OK();
  std::string rawZSetMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawZSetMetaValue);
  ZSetMetaValue zsetMetaValue;
  if (s.ok()) zsetMetaValue = ZSetMetaValue(rawZSetMetaValue);

  WriteBatch updates;
  MemberIterator mIter(db_, key);
  auto updatesIter = scoredMembers.cbegin();
  while (updatesIter != scoredMembers.cend()) {
    const auto [member, score] = *updatesIter;
    int r = mIter.Valid() ? updatesIter->first.compare(mIter.member()) : -1;
    if (r <= 0) {
      if (r == 0) {
        int64_t oldScore = mIter.score();
        if (oldScore == score) {
          ++updatesIter;
          mIter.Next();
          continue;
        }
        ZSetScoredMemberKey oldScoredMemberKey(key, member, oldScore);
        updates.Delete(oldScoredMemberKey.Encode());
        mIter.Next();
      } else {
        *count += 1;
      }
      ZSetMemberKey memberKey(key, member);
      ZSetMemberValue memberValue(score);
      ZSetScoredMemberKey scoredMemberKey(key, member, score);
      updates.Put(memberKey.Encode(), memberValue.Encode());
      updates.Put(scoredMemberKey.Encode(), "");
      ++updatesIter;
    } else {
      mIter.Next();
    }
  }

  if (*count) {
    zsetMetaValue.len += *count;
    updates.Put(key, zsetMetaValue.Encode());
  }
  db_->Write(WriteOptions(), &updates);
  return Status::OK();
}

Status RedisZSetBasicImpl::ZRem(const Slice& key,
                                const Slice& member,
                                uint64_t* count){
  *count = 0;
  std::string rawZSetMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawZSetMetaValue);
  if (!s.ok()) return Status::OK();
  ZSetMetaValue metaValue(rawZSetMetaValue);

  WriteBatch updates;
  ZSetMemberKey memberKey(key, member);
  std::string rawMemberValue;
  s = db_->Get(ReadOptions(), memberKey.Encode(), &rawMemberValue);
  if (!s.ok()) return Status::OK();
  ZSetMemberValue memberValue(rawMemberValue);
  ZSetScoredMemberKey scoredMemberKey(key, member, memberValue.score());
  updates.Delete(memberKey.Encode());
  updates.Delete(scoredMemberKey.Encode());
  *count = 1;
  metaValue.len -= 1;
  updates.Put(key, metaValue.Encode());
  return db_->Write(WriteOptions(), &updates);
}

Status RedisZSetBasicImpl::ZRem(const Slice& key,
                                const std::set<Slice>& members,
                                uint64_t* count){
  *count = 0;
  if (members.empty()) return Status::OK();
  std::string rawMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawMetaValue);
  ZSetMetaValue metaValue;
  if (s.ok()) metaValue = ZSetMetaValue(rawMetaValue);

  WriteBatch updates;
  MemberIterator mIter(db_, key);
  auto updatesIter = members.cbegin();
  while (mIter.Valid() && updatesIter != members.cend())  {
    const auto member = *updatesIter;
    int r = updatesIter->compare(mIter.member());
    if (r == 0) {
      ZSetMemberKey memberKey(key, member);
      ZSetScoredMemberKey scoredMemberKey(key, member, mIter.score());
      updates.Delete(memberKey.Encode());
      updates.Delete(scoredMemberKey.Encode());
      *count += 1;
      mIter.Next();
      ++updatesIter;
    } else if (r < 0) {
      ++updatesIter;
    } else {
      mIter.Next();
    }
  }
  if (*count) {
    metaValue.len -= *count;
    updates.Put(key, metaValue.Encode());
  }
  return db_->Write(WriteOptions(), &updates);
}

Status RedisZSetBasicImpl::ZPopMax(const Slice& key,
                                   ScoredMember* scoredMember){
  return ZPop(key, scoredMember, kMax);
}

Status RedisZSetBasicImpl::ZPopMin(const Slice& key,
                                   ScoredMember* scoredMember){
  return ZPop(key, scoredMember, kMin);
}

Status RedisZSetBasicImpl::ZRemRangeByRank(const Slice& key,
                                           int64_t minRank,
                                           int64_t maxRank,
                                           uint64_t* count){
  *count = 0;
  ScoredMemberIterator smIter(db_, key);
  if (!smIter.Valid()) return Status::OK();
  ZSetMetaValue metaValue(smIter.value().ToString());
  int64_t size = static_cast<int64_t>(metaValue.len);
  uint64_t lower = std::max((int64_t)0, minRank >= 0 ? minRank : size + minRank);
  uint64_t upper = std::min(size - 1, maxRank >= 0 ? maxRank : size + maxRank);
  uint64_t rangeSize = upper - lower + 1;
  if (rangeSize < 0) return Status::OK();
  smIter.Next();

  WriteBatch updates;
  for (; smIter.Valid() && lower--; smIter.Next());
  for (; smIter.Valid() && rangeSize--; smIter.Next()) {
    updates.Delete(smIter.key());
    updates.Delete(ZSetMemberKey(key, smIter.member()).Encode());
    *count += 1;
  }
  if (*count) {
    metaValue.len -= *count;
    updates.Put(key, metaValue.Encode());
  }
  return db_->Write(WriteOptions(), &updates);
}

Status RedisZSetBasicImpl::ZRemRangeByScore(const Slice& key,
                                            int64_t minScore,
                                            int64_t maxScore,
                                            uint64_t* count){
  *count = 0;
  if (minScore > maxScore) return Status::OK();
  ScoredMemberIterator smIter(db_, key);
  if (!smIter.Valid()) return Status::OK();
  ZSetMetaValue metaValue(smIter.value().ToString());
  smIter.Next();

  WriteBatch updates;
  for (; smIter.Valid() && smIter.score() < minScore; smIter.Next());
  for (; smIter.Valid() && smIter.score() <= maxScore; smIter.Next()) {
    updates.Delete(smIter.key());
    updates.Delete(ZSetMemberKey(key, smIter.member()).Encode());
    *count += 1;
  }
  if (*count) {
    metaValue.len -= *count;
    updates.Put(key, metaValue.Encode());
  }
  return db_->Write(WriteOptions(), &updates);
}

Status RedisZSetBasicImpl::ZRemRangeByLex(const Slice& key,
                                          const Slice& minLex,
                                          const Slice& maxLex,
                                          uint64_t* count){
  *count = 0;
  if (minLex > maxLex) return Status::OK();
  std::string rawMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawMetaValue);
  if (!s.ok()) return Status::OK();
  ZSetMetaValue metaValue(rawMetaValue);
  MemberIterator mIter(db_, key);
  if (!mIter.Valid()) return Status::OK();

  WriteBatch updates;
  for (; mIter.Valid() && mIter.member() < minLex; mIter.Next());
  for (; mIter.Valid() && mIter.member() <= maxLex; mIter.Next()) {
    updates.Delete(mIter.key());
    updates.Delete(ZSetScoredMemberKey(key, mIter.member(), mIter.score()).Encode());
    *count += 1;
  }
  if (*count) {
    metaValue.len -= *count;
    updates.Put(key, metaValue.Encode());
  }
  return db_->Write(WriteOptions(), &updates);
}

Status RedisZSetBasicImpl::ZUnion(const std::vector<Slice>& keys,
                                  Members* members){
  ScoredMembers scoredMembers;
  ZUnionWithScores(keys, &scoredMembers);
  for (const auto& [member, _]: scoredMembers) {
    members->push_back(member);
  }
  return Status::OK();
}

Status RedisZSetBasicImpl::ZInter(const std::vector<Slice>& keys,
                                  Members* members){
  ScoredMembers scoredMembers;
  ZInterWithScores(keys, &scoredMembers);
  for (const auto& [member, _]: scoredMembers) {
    members->push_back(member);
  }
  return Status::OK();
}

Status RedisZSetBasicImpl::ZDiff(const std::vector<Slice>& keys,
                                 Members* members){
  ScoredMembers scoredMembers;
  ZDiffWithScores(keys, &scoredMembers);
  for (const auto& [member, _]: scoredMembers) {
    members->push_back(member);
  }
  return Status::OK();
}

Status RedisZSetBasicImpl::ZUnionWithScores(const std::vector<Slice>& keys,
                                            ScoredMembers* scoredMembers){
  Member2Score member2score = ZUnionAsMap(keys);
  scoredMembers->reserve(member2score.size());
  for (const auto& [member, score]: member2score) {
    scoredMembers->push_back({member, score});
  }
  std::sort(scoredMembers->begin(), scoredMembers->end(), [](auto &left, auto &right) {
    return left.second < right.second;
  });
  return Status::OK();
}

Status RedisZSetBasicImpl::ZInterWithScores(const std::vector<Slice>& keys,
                                            ScoredMembers* scoredMembers){
  Member2Score member2score = ZInterAsMap(keys);
  scoredMembers->reserve(member2score.size());
  for (const auto& [member, score]: member2score) {
    scoredMembers->push_back({member, score});
  }
  std::sort(scoredMembers->begin(), scoredMembers->end(), [](auto &left, auto &right) {
    return left.second < right.second;
  });
  return Status::OK();
}

Status RedisZSetBasicImpl::ZDiffWithScores(const std::vector<Slice>& keys,
                                           ScoredMembers* scoredMembers){
  Member2Score member2score = ZDiffAsMap(keys);
  scoredMembers->reserve(member2score.size());
  for (const auto& [member, score]: member2score) {
    scoredMembers->push_back({member, score});
  }
  std::sort(scoredMembers->begin(), scoredMembers->end(), [](auto &left, auto &right) {
    return left.second < right.second;
  });
  return Status::OK();
}

Status RedisZSetBasicImpl::ZUnionStore(const std::vector<Slice>& keys,
                                       const Slice& dstKey,
                                       uint64_t* count){
  Member2Score member2score = ZUnionAsMap(keys);
  Status s = Del(dstKey);
  if (!s.ok()) return s;
  std::map<Slice, int64_t> memberSlice2score;
  for (const auto& [member, score]: member2score) memberSlice2score[member] = score;
  return ZAdd(dstKey, memberSlice2score, count);
}

Status RedisZSetBasicImpl::ZInterStore(const std::vector<Slice>& keys,
                                       const Slice& dstKey,
                                       uint64_t* count){
  Member2Score member2score = ZInterAsMap(keys);
  Status s = Del(dstKey);
  if (!s.ok()) return s;
  std::map<Slice, int64_t> memberSlice2score;
  for (const auto& [member, score]: member2score) memberSlice2score[member] = score;
  return ZAdd(dstKey, memberSlice2score, count);
}

Status RedisZSetBasicImpl::ZDiffStore(const std::vector<Slice>& keys,
                                      const Slice& dstKey,
                                      uint64_t* count){
  Member2Score member2score = ZDiffAsMap(keys);
  Status s = Del(dstKey);
  if (!s.ok()) return s;
  std::map<Slice, int64_t> memberSlice2score;
  for (const auto& [member, score]: member2score) memberSlice2score[member] = score;
  return ZAdd(dstKey, memberSlice2score, count);
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

Status RedisZSetBasicImpl::ZPop(const Slice& key,
                                ScoredMember* scoredMember,
                                MinOrMax minOrMax) {
  ScoredMemberIterator smIter(db_, key);
  if (!smIter.Valid()) return Status::OK();
  ZSetMetaValue metaValue(smIter.value().ToString());
  minOrMax == kMin ? smIter.Next() : smIter.SeekToLast();
  if (!smIter.Valid()) return Status::OK();

  WriteBatch updates;
  *scoredMember = ScoredMember(smIter.member().ToString(), smIter.score());
  updates.Delete(smIter.key());
  updates.Delete(ZSetMemberKey(key, smIter.member()).Encode());
  metaValue.len -= 1;
  updates.Put(key, metaValue.Encode());
  return db_->Write(WriteOptions(), &updates);
}

Member2Score RedisZSetBasicImpl::ZUnionAsMap(const std::vector<Slice>& keys){
  Member2Score member2score;
  for (const auto& key: keys) {
    MemberIterator mIter(db_, key);
    if (!mIter.Valid()) continue;
    for (; mIter.Valid(); mIter.Next()) {
      member2score[mIter.member().ToString()] += mIter.score();
    }
  }
  return member2score;
}

Member2Score RedisZSetBasicImpl::ZInterAsMap(const std::vector<Slice>& keys){
  Member2Score member2score;
  MemberIterator frontIter(db_, keys.front());
  if (!frontIter.Valid()) return {};
  for (; frontIter.Valid(); frontIter.Next()) {
    member2score[frontIter.member().ToString()] += frontIter.score();
  }

  for (auto it = std::next(keys.begin()); it != keys.end(); it++) {
    Slice key = *it;
    MemberIterator mIter(db_, key);
    if (!mIter.Valid()) return {};
    for (auto mit = member2score.cbegin(); mit != member2score.cend(); ) {
      Slice member = mit->first;
      mIter.Seek(ZSetMemberKey(key, member.ToString()).Encode());
      if (!mIter.Valid()) break;
      if (mIter.member() == mit->first) {
        member2score[member.ToString()] += mIter.score();
        mit++;
      } else {
        member2score.erase(mit++);
      }
    }
  }
  return member2score;
}

Member2Score RedisZSetBasicImpl::ZDiffAsMap(const std::vector<Slice>& keys){
  Member2Score member2score;
  MemberIterator frontIter(db_, keys.front());
  if (!frontIter.Valid()) return {};
  for (; frontIter.Valid(); frontIter.Next()) {
    member2score[frontIter.member().ToString()] += frontIter.score();
  }

  for (auto it = std::next(keys.begin()); it != keys.end(); it++) {
    Slice key = *it;
    MemberIterator mIter(db_, key);
    if (!mIter.Valid()) continue;
    for (auto mit = member2score.cbegin(); mit != member2score.cend(); ) {
      Slice member = mit->first;
      mIter.Seek(ZSetMemberKey(key, member.ToString()).Encode());
      if (!mIter.Valid()) break;
      mIter.member() == mit->first ? member2score.erase(mit++): mit++;
    }
  }
  return member2score;
}

}
