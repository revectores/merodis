#include "redis_list.h"

#include <string>
#include <cstdio>
#include <iostream>
#include <vector>

#include "leveldb/db.h"
#include "util/coding.h"

namespace merodis {


ListMetaValue::ListMetaValue() noexcept :
  leftIndex(InitIndex),
  rightIndex(InitIndex) {}

ListMetaValue::ListMetaValue(uint64_t leftIndex, uint64_t rightIndex) noexcept :
  leftIndex(leftIndex),
  rightIndex(rightIndex) {}

ListMetaValue::ListMetaValue(const std::string& rawValue) noexcept {
  leftIndex = DecodeFixed64(rawValue.data());
  rightIndex = DecodeFixed64(rawValue.data() + sizeof(leftIndex));
}

Slice ListMetaValue::Encode() const {
  char* rawMetaValue = new char[sizeof(ListMetaValue)];
  EncodeFixed64(rawMetaValue, leftIndex);
  EncodeFixed64(rawMetaValue + sizeof(leftIndex), rightIndex);
  return Slice{ rawMetaValue, sizeof(ListMetaValue)};
}

ListNodeKey::ListNodeKey(Slice key, uint64_t index) noexcept :
  key(key),
  index(index) {}

ListNodeKey::ListNodeKey(const std::string &rawValue) noexcept :
  key(rawValue.data(), rawValue.length() - sizeof(index)) {
  index = DecodeFixed64(rawValue.data() + rawValue.length() - sizeof(index));
}

Slice ListNodeKey::Encode() const {
  char* rawListNodeKey = new char[key.size() + sizeof(index)];
  EncodeFixed64(rawListNodeKey + key.size(), index);
  return Slice{rawListNodeKey, key.size() + sizeof(index)};
}

RedisList::RedisList() noexcept = default;

RedisList::~RedisList() noexcept = default;

Status RedisList::Open(const Options& options, const std::string& db_path) noexcept {
  return leveldb::DB::Open(options, db_path, &db_);
}

Status RedisList::LLen(const Slice &key, uint64_t *len) noexcept {
  std::string rawListMetaValue;
  merodis::Status s = db_->Get(ReadOptions(), key, &rawListMetaValue);

  if (s.ok()) {
    ListMetaValue metaValue(rawListMetaValue);
    *len = metaValue.rightIndex - metaValue.leftIndex + 1;
    return merodis::Status::OK();
  } else if (s.IsNotFound()) {
    *len = 0;
    return merodis::Status::OK();
  } else {
    return s;
  }
}

Status RedisList::LIndex(const Slice &key, int64_t index, std::string* value) noexcept {
  std::string rawListMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawListMetaValue);

  if (s.ok()) {
    ListMetaValue metaValue(rawListMetaValue);
    ListNodeKey nodeKey(key, GetInternalIndex(index, metaValue));
    s = db_->Get(ReadOptions(), nodeKey.Encode(), value);
    return s;
  }
  return s;
}

Status RedisList::LRange(const Slice &key, int64_t from, int64_t to, std::vector<std::string>* values) noexcept {
  std::string rawListMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawListMetaValue);
  if (s.ok()) {
    ListMetaValue metaValue(rawListMetaValue);
    uint64_t from_ = std::max(metaValue.leftIndex, GetInternalIndex(from, metaValue));
    uint64_t to_ = std::min(metaValue.rightIndex, GetInternalIndex(to, metaValue));
    if (to_ < from_) return Status::OK();
    values->reserve(to_ - from_ + 1);
    uint64_t current_ = from_;
    leveldb::Iterator* iter = db_->NewIterator(ReadOptions());
    ListNodeKey firstKey(key, from_);
    for (iter->Seek(firstKey.Encode()); iter->Valid() && current_ <= to_; iter->Next(), current_++) {
      values->emplace_back(iter->value().ToString());
    }
    delete iter;
    return Status::OK();
  }
  return s;
}

Status RedisList::LPush(const Slice &key, const Slice &value) noexcept {
  std::string rawListMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawListMetaValue);
  if (s.ok()) {
    ListMetaValue metaValue(rawListMetaValue);
    metaValue.leftIndex--;
    s = db_->Put(WriteOptions(), key, metaValue.Encode());
    if (!s.ok()) return s;
    ListNodeKey nodeKey(key, metaValue.leftIndex);
    return db_->Put(WriteOptions(), nodeKey.Encode(), value);
  } else if (s.IsNotFound()) {
    ListMetaValue metaValue;
    s = db_->Put(WriteOptions(), key, metaValue.Encode());
    std::string v;
    if (!s.ok()) return s;
    ListNodeKey nodeKey(key, metaValue.leftIndex);
    return db_->Put(WriteOptions(), nodeKey.Encode(), value);
  }
  return s;
}

inline uint64_t RedisList::GetInternalIndex(int64_t userIndex, ListMetaValue metaValue) noexcept {
  return userIndex >= 0 ? metaValue.leftIndex + userIndex : metaValue.rightIndex + userIndex + 1;
}

}