#include "redis_list.h"

#include <cstdio>
#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <memory>

#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include "util/coding.h"
#include "util/sequence.h"

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

Status RedisList::LLen(const Slice& key,
                       uint64_t *len) noexcept {
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

Status RedisList::LIndex(const Slice& key,
                         int64_t index,
                         std::string* value) noexcept {
  std::string rawListMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawListMetaValue);
  if (!s.ok()) return s;
  ListMetaValue metaValue(rawListMetaValue);
  ListNodeKey nodeKey(key, GetInternalIndex(index, metaValue));
  return db_->Get(ReadOptions(), nodeKey.Encode(), value);
}

Status RedisList::LRange(const Slice& key,
                         int64_t from,
                         int64_t to,
                         std::vector<std::string>* values) noexcept {
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

Status RedisList::Push(const Slice& key,
                       const Slice& value,
                       bool createListIfNotFound,
                       enum Side side) noexcept {
  return Push(key, std::vector<Slice>{value}, createListIfNotFound, side);
}

Status RedisList::Push(const Slice& key,
                       const std::vector<Slice>& values,
                       bool createListIfNotFound,
                       enum Side side) noexcept {
  std::string rawListMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawListMetaValue);
  if (!(s.ok() || s.IsNotFound() && createListIfNotFound)) return s;

  ListMetaValue metaValue;
  if (s.ok()) metaValue = ListMetaValue(rawListMetaValue);

  WriteBatch updates;
  if (side == kLeft) {
    metaValue.leftIndex -= values.size();
    updates.Put(key, metaValue.Encode());
    uint64_t currentIndex = metaValue.leftIndex;
    for (auto it = values.rbegin(); it != values.rend(); it++, currentIndex++) {
      updates.Put(ListNodeKey(key, currentIndex).Encode(), *it);
    }
  } else {
    uint64_t currentIndex = metaValue.rightIndex + 1;
    metaValue.rightIndex += values.size();
    updates.Put(key, metaValue.Encode());
    for (auto it = values.begin(); it != values.end(); it++, currentIndex++) {
      updates.Put(ListNodeKey(key, currentIndex).Encode(), *it);
    }
  }
  return db_->Write(WriteOptions(), &updates);
}

Status RedisList::Pop(const Slice& key,
                      std::string* value,
                      enum Side side) noexcept {
  std::string rawListMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawListMetaValue);
  if (!s.ok()) return s;
  ListMetaValue metaValue(rawListMetaValue);

  if (!metaValue.Length()) return Status::OK();
  if (side == kLeft) {
    ListNodeKey nodeKey(key, metaValue.leftIndex);
    s = db_->Get(ReadOptions(), nodeKey.Encode(), value);
    if (!s.ok()) return s;
    metaValue.leftIndex += 1;
  } else {
    ListNodeKey nodeKey(key, metaValue.rightIndex);
    s = db_->Get(ReadOptions(), nodeKey.Encode(), value);
    if (!s.ok()) return s;
    metaValue.rightIndex -= 1;
  }
  return db_->Put(WriteOptions(), key, metaValue.Encode());
}

Status RedisList::Pop(const Slice& key,
                      uint64_t count,
                      std::vector<std::string> *values,
                      enum Side side) noexcept {
  std::string rawListMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawListMetaValue);
  if (!s.ok()) return s;
  ListMetaValue metaValue(rawListMetaValue);

  if (side == kLeft) {
    s = LRange(key, 0, int64_t(count - 1), values);
    if (!s.ok()) return s;
    metaValue.leftIndex += std::min(count, metaValue.Length());
  } else {
    s = LRange(key, -int64_t(count), -1, values);
    if (!s.ok()) return s;
    metaValue.rightIndex -= std::min(count, metaValue.Length());
  }
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

Status RedisList::LRem(const Slice &key,
                       int64_t count,
                       const Slice &value,
                       uint64_t* removedCount) noexcept {
  *removedCount = 0;
  std::string rawListMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawListMetaValue);
  if (!s.ok()) return s;
  ListMetaValue metaValue(rawListMetaValue);

  Iterator* iter = db_->NewIterator(ReadOptions());
  uint64_t current;
  ListNodeKey firstKey(key, metaValue.leftIndex);
  ListNodeKey lastKey(key, metaValue.rightIndex);
  WriteBatch updates;
  std::vector<uint64_t> removedIndices;
  removedIndices.reserve(abs(count));

  if (count >= 0) {
    for (iter->Seek(firstKey.Encode()), current = metaValue.leftIndex;
         iter->Valid() && current <= metaValue.rightIndex;
         iter->Next(), current++) {
      if (iter->value().ToString() == value) {
        removedIndices.push_back(current);
        *removedCount += 1;
        if (count > 0 && count - *removedCount == 0) break;
      }
    }
  } else {
    for (iter->Seek(lastKey.Encode()), current = metaValue.rightIndex;
         iter->Valid() && current >= metaValue.leftIndex;
         iter->Prev(), current--) {
      if (iter->value().ToString() == value) {
        removedIndices.push_back(current);
        *removedCount += 1;
        if (count + *removedCount == 0) break;
      }
    }
    std::reverse(removedIndices.begin(), removedIndices.end());
  }

  const std::vector<Block> blocks = GetBlocks(metaValue.leftIndex, metaValue.rightIndex, removedIndices);
  iter->SeekToFirst();
  iter->Seek(firstKey.Encode());
  for (Block block: blocks) {
    current = block.start;
    if (block.step == 0) continue;
    for (iter->Seek(ListNodeKey(key, current).Encode());
         current <= block.end;
         current++, iter->Next()) {
      ListNodeKey nodeKey(iter->key());
      nodeKey.index += block.step;
      updates.Put(nodeKey.Encode(), iter->value());
    }
  }
  delete iter;
  updates.Put(key, metaValue.Encode());
  return db_->Write(WriteOptions(), &updates);
}

Status RedisList::LMove(const Slice& srcKey,
                        const Slice& dstKey,
                        enum Side srcSide,
                        enum Side dstSide,
                        std::string* value) noexcept {
  if (srcKey == dstKey && srcSide == dstSide) return Status::OK();

  std::string rawListMetaValue;
  Status s = db_->Get(ReadOptions(), srcKey, &rawListMetaValue);
  if (!s.ok()) return s;
  std::shared_ptr<ListMetaValue> srcMetaValue = std::make_shared<ListMetaValue>(rawListMetaValue);
  std::shared_ptr<ListMetaValue> dstMetaValue = srcMetaValue;
  if (srcKey != dstKey) {
    s = db_->Get(ReadOptions(), dstKey, &rawListMetaValue);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.ok()) dstMetaValue = std::make_shared<ListMetaValue>(rawListMetaValue);
  }

  ListNodeKey srcNodeKey(srcKey, srcSide == kLeft ? srcMetaValue->leftIndex : srcMetaValue->rightIndex);
  ListNodeKey dstNodeKey(dstKey, dstSide == kLeft ? dstMetaValue->leftIndex - 1: dstMetaValue->rightIndex + 1);
  srcSide == kLeft ? srcMetaValue->leftIndex += 1 : srcMetaValue->rightIndex -= 1;
  dstSide == kLeft ? dstMetaValue->leftIndex -= 1 : dstMetaValue->rightIndex += 1;

  s = db_->Get(ReadOptions(), srcNodeKey.Encode(), value);
  if (!s.ok()) return s;
  WriteBatch updates;
  updates.Put(dstNodeKey.Encode(), *value);
  updates.Put(srcKey, srcMetaValue->Encode());
  if (srcKey != dstKey) updates.Put(dstKey, dstMetaValue->Encode());
  return db_->Write(WriteOptions(), &updates);
}

inline uint64_t RedisList::GetInternalIndex(int64_t userIndex, ListMetaValue metaValue) noexcept {
  return userIndex >= 0 ? metaValue.leftIndex + userIndex : metaValue.rightIndex + userIndex + 1;
}

}