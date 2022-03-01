#include "redis_set_basic_impl.h"

#include "leveldb/db.h"
#include "leveldb/write_batch.h"

namespace merodis {

RedisSetBasicImpl::RedisSetBasicImpl() noexcept = default;

RedisSetBasicImpl::~RedisSetBasicImpl() noexcept = default;

Status RedisSetBasicImpl::SCard(const Slice& key,
                                uint64_t* len) {
  std::string rawSetMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawSetMetaValue);
  if (s.ok()) {
    SetMetaValue metaValue(rawSetMetaValue);
    *len = metaValue.len;
  } else if (s.IsNotFound()) {
    *len = 0;
    s = Status::OK();
  }
  return s;
}

Status RedisSetBasicImpl::SIsMember(const Slice& key,
                                    const Slice& setKey,
                                    bool* isMember) {
  std::string _;
  SetNodeKey nodeKey(key, setKey);
  *isMember = db_->Get(ReadOptions(), nodeKey.Encode(), &_).ok();
  return Status::OK();
}

Status RedisSetBasicImpl::SMIsMember(const Slice& key,
                                     const std::set<Slice>& keys,
                                     std::vector<bool>* isMembers) {
  Iterator* iter = db_->NewIterator(ReadOptions());
  iter->Seek(key);
  iter->Next();
  std::set<Slice>::const_iterator queryIter = keys.cbegin();
  while (iter->Valid() && queryIter != keys.cend()) {
    int cmp = queryIter->compare({iter->key().data() + key.size() + 1,
                                  iter->key().size() - key.size() - 1});
    if (cmp == 0) {
      ++queryIter;
      iter->Next();
      isMembers->push_back(true);
    } else if (cmp > 0) {
      iter->Next();
    } else {
      ++queryIter;
      isMembers->push_back(false);
    }
  }
  if (queryIter != keys.cend()) {
    std::vector<bool> falseVector(std::distance(queryIter, keys.cend()), false);
    isMembers->insert(isMembers->end(), falseVector.begin(), falseVector.end());
  }
  delete iter;
  return Status::OK();
}

Status RedisSetBasicImpl::SMembers(const Slice& key,
                                   std::vector<std::string>* keys) {
  Iterator* iter = db_->NewIterator(ReadOptions());
  iter->Seek(key);
  if (!iter->Valid() || iter->key() != key) {
    delete iter;
    return Status::OK();
  };
  iter->Next();
  for (; iter->Valid(); iter->Next()) {
    if (iter->key().size() <= key.size() || iter->key()[key.size()] != 0) break;
    SetNodeKey nodeKey(iter->key(), key.size());
    keys->push_back(nodeKey.setKey().ToString());
  }
  delete iter;
  return Status::OK();
}

Status RedisSetBasicImpl::SAdd(const Slice& key,
                               const Slice& setKey,
                               uint64_t* count) {
  std::string rawSetMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawSetMetaValue);
  SetMetaValue metaValue;
  if (s.ok()) metaValue = SetMetaValue(rawSetMetaValue);

  WriteBatch updates;
  SetNodeKey nodeKey(key, setKey);

  *count = 1 - CountKeyIntersection(key, nodeKey);
  if (*count == 0) return Status::OK();
  metaValue.len += 1;
  updates.Put(key, metaValue.Encode());
  updates.Put(nodeKey.Encode(), "");
  return db_->Write(WriteOptions(), &updates);
}

Status RedisSetBasicImpl::SAdd(const Slice& key,
                               const std::set<Slice>& keys,
                               uint64_t* count) {
  std::string rawSetMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawSetMetaValue);
  SetMetaValue metaValue;
  if (s.ok()) metaValue = SetMetaValue(rawSetMetaValue);
  WriteBatch updates;
  *count = 0;

  Iterator* iter = db_->NewIterator(ReadOptions());
  iter->Seek(key);
  iter->Next();
  std::set<Slice>::const_iterator updatesIter = keys.cbegin();
  while (iter->Valid() && updatesIter != keys.cend()) {
    int cmp = updatesIter->compare({iter->key().data() + key.size() + 1,
                                    iter->key().size() - key.size() - 1});
    if (cmp == 0) {
      ++updatesIter;
      iter->Next();
    } else if (cmp > 0) {
      iter->Next();
    } else {
      ++updatesIter;
      SetNodeKey nodeKey(key, *updatesIter);
      updates.Put(nodeKey.Encode(), "");
      *count += 1;
    }
  }
  while (updatesIter != keys.cend()) {
    SetNodeKey nodeKey(key, *updatesIter);
    updates.Put(nodeKey.Encode(), "");
    updatesIter++;
    *count += 1;
  }
  delete iter;

  if (*count) {
    metaValue.len += *count;
    updates.Put(key, metaValue.Encode());
  }
  return db_->Write(WriteOptions(), &updates);
}

uint64_t RedisSetBasicImpl::CountKeyIntersection(const Slice& key, const SetNodeKey& nodeKey) {
  std::string _;
  Status s = db_->Get(ReadOptions(), nodeKey.Encode(), &_);
  return s.ok();
}


}