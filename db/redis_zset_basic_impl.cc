#include "redis_zset_basic_impl.h"

#include "leveldb/write_batch.h"

namespace merodis {

RedisZSetBasicImpl::RedisZSetBasicImpl() noexcept = default;
RedisZSetBasicImpl::~RedisZSetBasicImpl() noexcept = default;

Status RedisZSetBasicImpl::ZCard(const Slice& key, uint64_t* len){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZScore(const Slice& key,
                                  const Slice& member,
                                  int64_t* score){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZMScore(const Slice& key,
                                   const std::vector<Slice>& members,
                                   std::vector<int64_t>* scores){
	return Status::NotSupported("");
}

Status RedisZSetBasicImpl::ZRank(const Slice& key,
                                 const Slice& member,
                                 uint64_t* rank){
	return Status::NotSupported("");
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
	return Status::NotSupported("");
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

}
