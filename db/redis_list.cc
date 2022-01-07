#include "redis_list.h"

#include <string>
#include <cstdio>
#include <iostream>

#include "leveldb/db.h"
#include "util/coding.h"

namespace merodis {


ListMetaValue::ListMetaValue() noexcept :
  count(1),
  leftIndex(InitIndex),
  rightIndex(InitIndex) {}

ListMetaValue::ListMetaValue(int64_t count, int64_t leftIndex, int64_t rightIndex) noexcept :
  count(count),
  leftIndex(leftIndex),
  rightIndex(rightIndex) {}

ListMetaValue::ListMetaValue(const std::string& rawValue) noexcept {
  count = DecodeFixed64(rawValue.data());
  leftIndex = DecodeFixed64(rawValue.data() + sizeof(ListMetaValue::count));
  rightIndex = DecodeFixed64(rawValue.data() + sizeof(ListMetaValue::count) + sizeof(ListMetaValue::leftIndex));
}

Slice ListMetaValue::Encode() const {
  char* rawMetaValue = new char[sizeof(ListMetaValue)];
  EncodeFixed64(rawMetaValue, count);
  EncodeFixed64(rawMetaValue + sizeof(count), leftIndex);
  EncodeFixed64(rawMetaValue + sizeof(count) + sizeof(leftIndex), rightIndex);
  return Slice{ rawMetaValue, sizeof(ListMetaValue)};
}

ListNodeKey::ListNodeKey(Slice key, uint64_t index) noexcept :
  key(key),
  index(index) {}

ListNodeKey::ListNodeKey(const std::string &rawValue) noexcept :
  key(rawValue.data(), rawValue.length() - sizeof(ListNodeKey::index)) {
  index = DecodeFixed64(rawValue.data() + rawValue.length() - sizeof(ListNodeKey::index));
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
    *len = metaValue.count;
    return merodis::Status::OK();
  } else if (s.IsNotFound()) {
    *len = 0;
    return merodis::Status::OK();
  } else {
    return s;
  }
}

Status RedisList::LIndex(const Slice &key, uint64_t index, std::string* value) noexcept {
  std::string rawListMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawListMetaValue);

  if (s.ok()) {
    ListMetaValue metaValue(rawListMetaValue);
    ListNodeKey nodeKey(key, metaValue.leftIndex + index);
    s = db_->Get(ReadOptions(), nodeKey.Encode(), value);
    return s;
  }
  return s;
}

Status RedisList::LPush(const Slice &key, const Slice &value) noexcept {
  std::string rawListMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawListMetaValue);
  if (s.ok()) {
    ListMetaValue metaValue(rawListMetaValue);
    metaValue.leftIndex--;
    metaValue.count++;
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

}