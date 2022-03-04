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
  *score = memberValue.score;
  return Status::OK();
}

Status RedisZSetBasicImpl::ZMScore(const Slice& key,
                                   const std::vector<Slice>& members,
                                   std::vector<int64_t>* scores){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZRank(const Slice& key,
                                 const Slice& member,
                                 uint64_t* rank){
  ZSetMemberKey memberKey(key, member);
  std::string rawMemberValue;
  Status s = db_->Get(ReadOptions(), memberKey.Encode(), &rawMemberValue);
  if (!s.ok()) return s;

  Iterator* iter = db_->NewIterator(ReadOptions());
  iter->Seek(key);
  if (!iter->Valid() || iter->key() != key) {
    return Status::NotFound("empty zset");
  }
  for (iter->Next(), *rank = 0;
       iter->Valid() && IsMemberKey(iter->key(), key.size());
       iter->Next(), ++*rank) {
    ZSetScoredMemberKey scoredMemberKey(iter->key(), key.size());
    if (scoredMemberKey.member() == member) {
      break;
    }
  }
  delete iter;
  return Status::OK();
}

Status RedisZSetBasicImpl::ZRevRank(const Slice& key,
                                    const Slice& member,
                                    uint64_t* rank){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZCount(const Slice& key,
                                  int64_t minScore,
                                  int64_t maxScore,
                                  uint64_t* count){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZLexCount(const Slice& key,
                                     const Slice& minLex,
                                     const Slice& maxLex,
                                     uint64_t* count){
	return Status::NotSupported("");
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
    int64_t oldScore = oldMemberValue.score;
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

bool RedisZSetBasicImpl::IsMemberKey(const Slice& iterKey, uint64_t keySize) {
  return iterKey.size() > keySize && iterKey[keySize] == 0;
}

}
