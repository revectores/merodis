#include "redis_list.h"

#include <cstdio>
#include <iostream>
#include <string>
#include <vector>
#include <algorithm>

#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include "util/coding.h"

namespace merodis {

ListMetaValue::ListMetaValue() noexcept :
  leftIndex(InitIndex + 1),
  rightIndex(InitIndex) {}

ListMetaValue::ListMetaValue(uint64_t leftIndex, uint64_t rightIndex) noexcept :
  leftIndex(leftIndex),
  rightIndex(rightIndex) {}

ListMetaValue::ListMetaValue(const std::string& rawValue) noexcept {
  leftIndex = DecodeFixed64(rawValue.data());
  rightIndex = DecodeFixed64(rawValue.data() + sizeof(leftIndex));
}

uint64_t ListMetaValue::Length() const {
  return rightIndex - leftIndex + 1;
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

ListNodeKey::ListNodeKey(const Slice& rawValue) noexcept :
  key(rawValue.data(), rawValue.size() - sizeof(index)) {
  index = DecodeFixed64(rawValue.data() + rawValue.size() - sizeof(index));
}

Slice ListNodeKey::Encode() const {
  char* rawListNodeKey = new char[key.size() + sizeof(index)];
  memcpy(rawListNodeKey, key.data(), key.size());
  EncodeFixed64(rawListNodeKey + key.size(), index);
  return Slice{rawListNodeKey, key.size() + sizeof(index)};
}

RedisList::RedisList() noexcept = default;

RedisList::~RedisList() noexcept = default;

Status RedisList::Open(const Options& options, const std::string& db_path) noexcept {
  return leveldb::DB::Open(options, db_path, &db_);
}

Status RedisList::LLen(const Slice& key, uint64_t *len) noexcept {
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

Status RedisList::LIndex(const Slice& key, int64_t index, std::string* value) noexcept {
  std::string rawListMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawListMetaValue);
  if (!s.ok()) return s;
  ListMetaValue metaValue(rawListMetaValue);
  ListNodeKey nodeKey(key, GetInternalIndex(index, metaValue));
  return db_->Get(ReadOptions(), nodeKey.Encode(), value);
}

Status RedisList::LRange(const Slice& key, int64_t from, int64_t to, std::vector<std::string>* values) noexcept {
  std::string rawListMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawListMetaValue);
  if (!s.ok()) return s;
  ListMetaValue metaValue(rawListMetaValue);

  uint64_t from_ = std::max(metaValue.leftIndex, GetInternalIndex(from, metaValue));
  uint64_t to_ = std::min(metaValue.rightIndex, GetInternalIndex(to, metaValue));
  if (to_ < from_) return Status::OK();

  values->reserve(to_ - from_ + 1);
  uint64_t current_ = from_;
  Iterator* iter = db_->NewIterator(ReadOptions());
  ListNodeKey firstKey(key, from_);
  for (iter->Seek(firstKey.Encode()); iter->Valid() && current_ <= to_; iter->Next(), current_++) {
    values->emplace_back(iter->value().ToString());
  }
  delete iter;
  return Status::OK();
}

Status RedisList::LPush(const Slice& key, const Slice& value, bool createListIfNotFound) noexcept {
  return LPush(key, std::vector<const Slice>{value}, createListIfNotFound);
}

Status RedisList::LPush(const Slice& key, const std::vector<const Slice>& values, bool createListIfNotFound) noexcept {
  std::string rawListMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawListMetaValue);
  if (!(s.ok() || s.IsNotFound() && createListIfNotFound)) return s;

  ListMetaValue metaValue;
  if (s.ok()) metaValue = ListMetaValue(rawListMetaValue);

  WriteBatch updates;
  metaValue.leftIndex -= values.size();
  updates.Put(key, metaValue.Encode());
  uint64_t currentIndex = metaValue.leftIndex;
  for (auto it = values.rbegin(); it != values.rend(); it++, currentIndex++) {
    updates.Put(ListNodeKey(key, currentIndex).Encode(), *it);
  }
  return db_->Write(WriteOptions(), &updates);
}

Status RedisList::LPop(const Slice& key, std::string* value) noexcept {
  std::string rawListMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawListMetaValue);
  if (!s.ok()) return s;
  ListMetaValue metaValue(rawListMetaValue);

  if (!metaValue.Length()) return Status::OK();
  ListNodeKey nodeKey(key, metaValue.leftIndex);
  s = db_->Get(ReadOptions(), nodeKey.Encode(), value);
  if (!s.ok()) return s;
  metaValue.leftIndex += 1;
  s = db_->Put(WriteOptions(), key, metaValue.Encode());
  return s;
}

Status RedisList::LPop(const Slice& key, uint64_t count, std::vector<std::string> *values) noexcept {
  std::string rawListMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawListMetaValue);
  if (!s.ok()) return s;
  ListMetaValue metaValue(rawListMetaValue);

  s = LRange(key, 0, int64_t(count - 1), values);
  if (!s.ok()) return s;
  // We do not have to delete the values physically but adjusting the boundary.
  metaValue.leftIndex += std::min(count, metaValue.Length());
  s = db_->Put(WriteOptions(), key, metaValue.Encode());
  return Status::OK();
}

Status RedisList::LInsert(const Slice& key,
                          const BeforeOrAfter& beforeOrAfter,
                          const Slice& pivotValue,
                          const Slice& value) noexcept {
  std::string rawListMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawListMetaValue);
  if (!s.ok()) return s;
  ListMetaValue metaValue(rawListMetaValue);

  Iterator* iter = db_->NewIterator(ReadOptions());
  uint64_t current = metaValue.leftIndex;
  ListNodeKey firstKey(key, current);
  uint64_t pivotIndex = 0;
  for (iter->Seek(firstKey.Encode());
       iter->Valid() && current <= metaValue.rightIndex;
       iter->Next(), current++) {
    if (iter->value().ToString() == pivotValue) {
      ListNodeKey pivotKey(iter->key());
      pivotIndex = pivotKey.index;
      if (beforeOrAfter == kAfter) {
        pivotIndex += 1;
        iter->Next();
        current++;
      }
      break;
    }
  }
  if (!pivotIndex) return Status::NotFound("");

  WriteBatch updates;
  ListNodeKey newKey(key, pivotIndex);
  updates.Put(newKey.Encode(), value);
  for (; iter->Valid() && current <= metaValue.rightIndex; iter->Next(), current++) {
    ListNodeKey nodeKey(iter->key());
    nodeKey.index += 1;
    updates.Put(nodeKey.Encode(), iter->value());
  }
  metaValue.rightIndex += 1;
  updates.Put(key, metaValue.Encode());
  s = db_->Write(WriteOptions(), &updates);

  delete iter;
  return Status::OK();
}

inline uint64_t RedisList::GetInternalIndex(int64_t userIndex, ListMetaValue metaValue) noexcept {
  return userIndex >= 0 ? metaValue.leftIndex + userIndex : metaValue.rightIndex + userIndex + 1;
}

}