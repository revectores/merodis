#include "merodis/merodis.h"

#include <cstdio>
#include <string>
#include <vector>

#include "redis_string.h"
#include "redis_list.h"

namespace merodis {

Merodis::Merodis() noexcept :
  string_db_(nullptr),
  list_db_(nullptr) {}

Merodis::~Merodis() noexcept {
  delete string_db_;
  delete list_db_;
};

Status Merodis::Open(const Options& options, const std::string& db_path) noexcept {
  Status s;
  string_db_ = new RedisString;
  list_db_ = new RedisList;
  s = string_db_->Open(options, db_path + "/string");
  if (!s.ok()) return s;
  s = list_db_->Open(options, db_path + "/list");
  return s;
}

Status Merodis::DestroyDB(const std::string& db_path, Options options) noexcept {
  Status s = leveldb::DestroyDB(db_path + "/string", options);
  if (!s.ok()) return s;
  return leveldb::DestroyDB(db_path + "/list", options);
}

Status Merodis::Get(const Slice& key, std::string* value) noexcept {
  return string_db_->Get(key, value);
}

Status Merodis::Set(const Slice& key, const Slice& value) noexcept {
  return string_db_->Set(key, value);
}

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

}
