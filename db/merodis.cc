#include "merodis/merodis.h"

#include <cstdio>
#include <string>
#include <vector>

#include "redis_string_basic_impl.h"
#include "redis_string_typed_impl.h"
#include "redis_list_array_impl.h"
#include "redis_hash_basic_impl.h"
#include "redis_set_basic_impl.h"
#include "redis_zset_basic_impl.h"

namespace merodis {

static const std::vector<std::string> databases {
  "string", "list", "hash", "set", "zset"
};

Merodis::Merodis() noexcept :
  string_db_(nullptr),
  list_db_(nullptr),
  hash_db_(nullptr),
  set_db_(nullptr),
  zset_db_(nullptr) {}

Merodis::~Merodis() noexcept {
  delete string_db_;
  delete list_db_;
  delete hash_db_;
  delete set_db_;
  delete zset_db_;
}

Status Merodis::Open(const Options& options, const std::string& db_path) noexcept {
  Status s;
  switch (options.string_impl) {
    case kStringBasicImpl:
      string_db_ = new RedisStringBasicImpl;
      break;
    case kStringTypedImpl:
    default:
      string_db_ = new RedisStringTypedImpl;
  }
  switch (options.list_impl) {
    case kListArrayImpl:
    default:
      list_db_ = new RedisListArrayImpl;
  }
  switch (options.hash_impl) {
    case kHashBasicImpl:
    default:
      hash_db_ = new RedisHashBasicImpl;
  }
  switch (options.set_impl) {
    case kSetBasicImpl:
    default:
      set_db_ = new RedisSetBasicImpl;
  }
  switch (options.zset_impl) {
    case kZSetBasicImpl:
    default:
      zset_db_ = new RedisZSetBasicImpl;
  }
  Redis* dbs_[] = {string_db_, list_db_, hash_db_, set_db_, zset_db_};
  std::string db_home(db_path + "/");
  for (int c = 0; c < databases.size(); c++) {
    s = dbs_[c]->Open(options, db_home + databases[c]);
    if (!s.ok()) return s;
  }
  return s;
}

Status Merodis::DestroyDB(const std::string& db_path, Options options) noexcept {
  Status s;
  std::string db_home(db_path + "/");
  for (const auto& db_name: databases) {
    s = DB_ENGINE::DestroyDB(db_home + db_name, options);
    if (!s.ok()) return s;
  }
  return s;
}

// String Operators
Status Merodis::Get(const Slice& key, std::string* value) noexcept {
  return string_db_->Get(key, value);
}

Status Merodis::Set(const Slice& key, const Slice& value) noexcept {
  return string_db_->Set(key, value);
}

Status Merodis::Incr(const Slice& key, int64_t* result) noexcept {
  return string_db_->Incr(key, result);
}

Status Merodis::IncrBy(const Slice& key, int64_t increment, int64_t* result) noexcept {
  return string_db_->IncrBy(key, increment, result);
}

Status Merodis::Decr(const Slice& key, int64_t* result) noexcept {
  return string_db_->Decr(key, result);
}

Status Merodis::DecrBy(const Slice& key, int64_t decrement, int64_t* result) noexcept {
  return string_db_->DecrBy(key, decrement, result);
}

// List Operators
Status Merodis::LLen(const Slice& key, uint64_t* len) noexcept {
  return list_db_->LLen(key, len);
}

Status Merodis::LIndex(const Slice& key, UserIndex index, std::string* value) noexcept {
  return list_db_->LIndex(key, index, value);
}

Status Merodis::LPos(const Slice& key, const Slice& value, int64_t rank, int64_t count, int64_t maxlen,
                     std::vector<uint64_t>& indices) noexcept {
  return list_db_->LPos(key, value, rank, count, maxlen, indices);
}

Status Merodis::LRange(const Slice& key, UserIndex from, UserIndex to, std::vector<std::string>* values) noexcept {
  return list_db_->LRange(key, from, to, values);
}

Status Merodis::LSet(const Slice& key, UserIndex index, const Slice& value) noexcept {
  return list_db_->LSet(key, index, value);
}

Status Merodis::LPush(const Slice& key, const Slice& value) noexcept {
  return list_db_->Push(key, value, true, kLeft);
}

Status Merodis::LPush(const Slice& key, const std::vector<Slice>& values) noexcept {
  return list_db_->Push(key, values, true, kLeft);
}

Status Merodis::LPushX(const Slice& key, const Slice& value) noexcept {
  return list_db_->Push(key, value, false, kLeft);
}

Status Merodis::LPushX(const Slice& key, const std::vector<Slice>& values) noexcept {
  return list_db_->Push(key, values, false, kLeft);
}

Status Merodis::LPop(const Slice& key, std::string* value) noexcept {
  return list_db_->Pop(key, value, kLeft);
}

Status Merodis::LPop(const Slice& key, uint64_t count, std::vector<std::string>* values) noexcept {
  return list_db_->Pop(key, count, values, kLeft);
}

Status Merodis::RPush(const Slice& key, const Slice& value) noexcept {
  return list_db_->Push(key, value, true, kRight);
}

Status Merodis::RPush(const Slice& key, const std::vector<Slice>& values) noexcept {
  return list_db_->Push(key, values, true, kRight);
}

Status Merodis::RPushX(const Slice& key, const Slice& value) noexcept {
  return list_db_->Push(key, value, false, kRight);
}

Status Merodis::RPushX(const Slice& key, const std::vector<Slice>& values) noexcept {
  return list_db_->Push(key, values, false, kRight);
}

Status Merodis::RPop(const Slice& key, std::string* value) noexcept {
  return list_db_->Pop(key, value, kRight);
}

Status Merodis::RPop(const Slice& key, uint64_t count, std::vector<std::string>* values) noexcept {
  return list_db_->Pop(key, count, values, kRight);
}

Status Merodis::LTrim(const Slice& key, UserIndex from, UserIndex to) noexcept {
  return list_db_->LTrim(key, from, to);
}

Status Merodis::LInsert(const Slice& key, const BeforeOrAfter& beforeOrAfter, const Slice& pivotValue, const Slice& value) noexcept {
  return list_db_->LInsert(key, beforeOrAfter, pivotValue, value);
}

Status Merodis::LRem(const Slice& key, int64_t count, const Slice& value, uint64_t* removedCount) noexcept {
  return list_db_->LRem(key, count, value, removedCount);
}

Status Merodis::LMove(const Slice& srcKey, const Slice& dstKey, enum Side srcSide, enum Side dstSide, std::string* value) noexcept {
  return list_db_->LMove(srcKey, dstKey, srcSide, dstSide, value);
}

// Hash Operators
Status Merodis::HLen(const Slice& key, uint64_t* len) {
  return hash_db_->HLen(key, len);
}

Status Merodis::HGet(const Slice& key, const Slice& hashKey, std::string* value) {
  return hash_db_->HGet(key, hashKey, value);
}

Status Merodis::HMGet(const Slice& key, const std::vector<Slice>& hashKeys, std::vector<std::string>* values) {
  return hash_db_->HMGet(key, hashKeys, values);
};

Status Merodis::HGetAll(const Slice& key, std::map<std::string, std::string>* kvs) {
  return hash_db_->HGetAll(key, kvs);
}

Status Merodis::HKeys(const Slice& key, std::vector<std::string>* keys) {
  return hash_db_->HKeys(key, keys);
}

Status Merodis::HVals(const Slice& key, std::vector<std::string>* values) {
  return hash_db_->HVals(key, values);
}

Status Merodis::HExists(const Slice& key, const Slice& hashKey, bool* exists) {
  return hash_db_->HExists(key, hashKey, exists);
}

Status Merodis::HSet(const Slice& key, const Slice& hashKey, const Slice& value, uint64_t* count) {
  return hash_db_->HSet(key, hashKey, value, count);
}

Status Merodis::HSet(const Slice& key, const std::map<Slice, Slice>& kvs, uint64_t* count) {
  return hash_db_->HSet(key, kvs, count);
}

Status Merodis::HDel(const Slice& key, const Slice& hashKey, uint64_t* count) {
  return hash_db_->HDel(key, hashKey, count);
}

Status Merodis::HDel(const Slice& key, const std::set<Slice>& hashKeys, uint64_t* count) {
  return hash_db_->HDel(key, hashKeys, count);
}

// Set Operators
Status Merodis::SCard(const Slice& key, uint64_t* len) {
  return set_db_->SCard(key, len);
}

Status Merodis::SIsMember(const Slice& key, const Slice& setKey, bool* isMember) {
  return set_db_->SIsMember(key, setKey, isMember);
}

Status Merodis::SMIsMember(const Slice& key, const std::set<Slice>& keys, std::vector<bool>* isMembers) {
  return set_db_->SMIsMember(key, keys, isMembers);
}

Status Merodis::SMembers(const Slice& key, std::vector<std::string>* keys) {
  return set_db_->SMembers(key, keys);
}

Status Merodis::SRandMember(const Slice& key, std::string* member) {
  return set_db_->SRandMember(key, member);
}

Status Merodis::SRandMember(const Slice& key, int64_t count, std::vector<std::string>* members) {
  return set_db_->SRandMember(key, count, members);
}

Status Merodis::SAdd(const Slice& key, const Slice& setKey, uint64_t* count) {
  return set_db_->SAdd(key, setKey, count);
}

Status Merodis::SAdd(const Slice& key, const std::set<Slice>& keys, uint64_t* count) {
  return set_db_->SAdd(key, keys, count);
}

Status Merodis::SRem(const Slice& key, const Slice& member, uint64_t* count) {
  return set_db_->SRem(key, member, count);
}

Status Merodis::SRem(const Slice& key, const std::set<Slice>& members, uint64_t* count) {
  return set_db_->SRem(key, members, count);
}

Status Merodis::SPop(const Slice& key, std::string* member) {
  return set_db_->SPop(key, member);
}

Status Merodis::SPop(const Slice& key, uint64_t count, std::vector<std::string>* members) {
  return set_db_->SPop(key, count, members);
}

Status Merodis::SMove(const Slice& srcKey, const Slice& dstKey, const Slice& member, uint64_t* count) {
  return set_db_->SMove(srcKey, dstKey, member, count);
}

Status Merodis::SUnion(const std::vector<Slice>& keys, std::vector<std::string>* members) {
  return set_db_->SUnion(keys, members);
}

Status Merodis::SInter(const std::vector<Slice>& keys, std::vector<std::string>* members) {
  return set_db_->SInter(keys, members);
}

Status Merodis::SDiff(const std::vector<Slice>& keys, std::vector<std::string>* members) {
  return set_db_->SDiff(keys, members);
}

Status Merodis::SUnionStore(const std::vector<Slice>& keys, const Slice& dstKey, uint64_t* count) {
  return set_db_->SUnionStore(keys, dstKey, count);
}

Status Merodis::SInterStore(const std::vector<Slice>& keys, const Slice& dstKey, uint64_t* count) {
  return set_db_->SInterStore(keys, dstKey, count);
}

Status Merodis::SDiffStore(const std::vector<Slice>& keys, const Slice& dstKey, uint64_t* count) {
  return set_db_->SDiffStore(keys, dstKey, count);
}

// ZSet Operators
Status Merodis::ZCard(const Slice& key, uint64_t* len){
  return zset_db_->ZCard(key, len);
}

Status Merodis::ZScore(const Slice& key, const Slice& member, int64_t* score){
  return zset_db_->ZScore(key, member, score);
}

Status Merodis::ZMScore(const Slice& key, const std::vector<Slice>& members, ScoreOpts* scores){
  return zset_db_->ZMScore(key, members, scores);
}

Status Merodis::ZRank(const Slice& key, const Slice& member, uint64_t* rank){
  return zset_db_->ZRank(key, member, rank);
}

Status Merodis::ZRevRank(const Slice& key, const Slice& member, uint64_t* rank){
  return zset_db_->ZRevRank(key, member, rank);
}

Status Merodis::ZCount(const Slice& key, int64_t minScore, int64_t maxScore, uint64_t* count){
  return zset_db_->ZCount(key, minScore, maxScore, count);
}

Status Merodis::ZLexCount(const Slice& key, const Slice& minLex, const Slice& maxLex, uint64_t* count){
  return zset_db_->ZLexCount(key, minLex, maxLex, count);
}

Status Merodis::ZRange(const Slice& key, int64_t minRank, int64_t maxRank, Members* members){
  return zset_db_->ZRange(key, minRank, maxRank, members);
}

Status Merodis::ZRangeByScore(const Slice& key, int64_t minScore, int64_t maxScore, Members* members){
  return zset_db_->ZRangeByScore(key, minScore, maxScore, members);
}

Status Merodis::ZRangeByLex(const Slice& key, const Slice& minLex, const Slice& maxLex, Members* members){
  return zset_db_->ZRangeByLex(key, minLex, maxLex, members);
}

Status Merodis::ZRangeWithScores(const Slice& key, int64_t minRank, int64_t maxRank, ScoredMembers* scoredMembers){
  return zset_db_->ZRangeWithScores(key, minRank, maxRank, scoredMembers);
}

Status Merodis::ZRangeByScoreWithScores(const Slice& key, int64_t minScore, int64_t maxScore, ScoredMembers* scoredMembers){
  return zset_db_->ZRangeByScoreWithScores(key, minScore, maxScore, scoredMembers);
}

Status Merodis::ZRangeByLexWithScores(const Slice& key, const Slice& minLex, const Slice& maxLex, ScoredMembers* scoredMembers){
  return zset_db_->ZRangeByLexWithScores(key, minLex, maxLex, scoredMembers);
}

Status Merodis::ZRevRange(const Slice& key, int64_t minRank, int64_t maxRank, Members* members){
  return zset_db_->ZRevRange(key, minRank, maxRank, members);
}

Status Merodis::ZRevRangeByScore(const Slice& key, int64_t minScore, int64_t maxScore, Members* members){
  return zset_db_->ZRevRangeByScore(key, minScore, maxScore, members);
}

Status Merodis::ZRevRangeByLex(const Slice& key, const Slice& minLex, const Slice& maxLex, Members* members){
  return zset_db_->ZRevRangeByLex(key, minLex, maxLex, members);
}

Status Merodis::ZRevRangeWithScores(const Slice& key, int64_t minRank, int64_t maxRank, ScoredMembers* scoredMembers){
  return zset_db_->ZRevRangeWithScores(key, minRank, maxRank, scoredMembers);
}

Status Merodis::ZRevRangeByScoreWithScores(const Slice& key, int64_t minScore, int64_t maxScore, ScoredMembers* scoredMembers){
  return zset_db_->ZRevRangeByScoreWithScores(key, minScore, maxScore, scoredMembers);
}

Status Merodis::ZRevRangeByLexWithScores(const Slice& key, const Slice& minLex, const Slice& maxLex, ScoredMembers* scoredMembers){
  return zset_db_->ZRevRangeByLexWithScores(key, minLex, maxLex, scoredMembers);
}

Status Merodis::ZAdd(const Slice& key, const std::pair<Slice, int64_t>& scoredMember, uint64_t* count){
  return zset_db_->ZAdd(key, scoredMember, count);
}

Status Merodis::ZAdd(const Slice& key, const std::map<Slice, int64_t>& scoredMembers, uint64_t* count){
  return zset_db_->ZAdd(key, scoredMembers, count);
}

Status Merodis::ZRem(const Slice& key, const Slice& member, uint64_t* count){
  return zset_db_->ZRem(key, member, count);
}

Status Merodis::ZRem(const Slice& key, const std::set<Slice>& members, uint64_t* count){
  return zset_db_->ZRem(key, members, count);
}

Status Merodis::ZPopMax(const Slice& key, ScoredMember* scoredMember){
  return zset_db_->ZPopMax(key, scoredMember);
}

Status Merodis::ZPopMin(const Slice& key, ScoredMember* scoredMember){
  return zset_db_->ZPopMin(key, scoredMember);
}

Status Merodis::ZRemRangeByRank(const Slice& key, int64_t minRank, int64_t maxRank, uint64_t* count){
  return zset_db_->ZRemRangeByRank(key, minRank, maxRank, count);
}

Status Merodis::ZRemRangeByScore(const Slice& key, int64_t minScore, int64_t maxScore, uint64_t* count){
  return zset_db_->ZRemRangeByScore(key, minScore, maxScore, count);
}

Status Merodis::ZRemRangeByLex(const Slice& key, const Slice& minLex, const Slice& maxLex, uint64_t* count){
  return zset_db_->ZRemRangeByLex(key, minLex, maxLex, count);
}

Status Merodis::ZUnion(const std::vector<Slice>& keys, Members* members){
  return zset_db_->ZUnion(keys, members);
}

Status Merodis::ZInter(const std::vector<Slice>& keys, Members* members){
  return zset_db_->ZInter(keys, members);
}

Status Merodis::ZDiff(const std::vector<Slice>& keys, Members* members){
  return zset_db_->ZDiff(keys, members);
}

Status Merodis::ZUnionWithScores(const std::vector<Slice>& keys, ScoredMembers* scoredMembers){
  return zset_db_->ZUnionWithScores(keys, scoredMembers);
}

Status Merodis::ZInterWithScores(const std::vector<Slice>& keys, ScoredMembers* scoredMembers){
  return zset_db_->ZInterWithScores(keys, scoredMembers);
}

Status Merodis::ZDiffWithScores(const std::vector<Slice>& keys, ScoredMembers* scoredMembers){
  return zset_db_->ZDiffWithScores(keys, scoredMembers);
}

Status Merodis::ZUnionStore(const std::vector<Slice>& keys, const Slice& dstKey, uint64_t* count){
  return zset_db_->ZUnionStore(keys, dstKey, count);
}

Status Merodis::ZInterStore(const std::vector<Slice>& keys, const Slice& dstKey, uint64_t* count){
  return zset_db_->ZInterStore(keys, dstKey, count);
}

Status Merodis::ZDiffStore(const std::vector<Slice>& keys, const Slice& dstKey, uint64_t* count){
  return zset_db_->ZDiffStore(keys, dstKey, count);
}

}
