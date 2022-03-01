#include "merodis/merodis.h"

#include <cstdio>
#include <string>
#include <vector>

#include "redis_string_basic_impl.h"
#include "redis_string_typed_impl.h"
#include "redis_list_array_impl.h"
#include "redis_hash_basic_impl.h"
#include "redis_set_basic_impl.h"

namespace merodis {

static const std::vector<std::string> databases {
  "string", "list", "hash", "set"
};

Merodis::Merodis() noexcept :
  string_db_(nullptr),
  list_db_(nullptr),
  hash_db_(nullptr),
  set_db_(nullptr) {}

Merodis::~Merodis() noexcept {
  delete string_db_;
  delete list_db_;
  delete hash_db_;
  delete set_db_;
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
  Redis* dbs_[] = {string_db_, list_db_, hash_db_, set_db_};
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
    s = leveldb::DestroyDB(db_home + db_name, options);
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

Status Merodis::SAdd(const Slice& key, const Slice& setKey, uint64_t* count) {
  return set_db_->SAdd(key, setKey, count);
}

Status Merodis::SAdd(const Slice& key, const std::vector<Slice>& keys, uint64_t* count) {
  return set_db_->SAdd(key, keys, count);
}

}
