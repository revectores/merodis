#include "redis_set_basic_impl.h"

#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include "util/random.h"

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

Status RedisSetBasicImpl::SRandMember(const Slice& key,
                                      std::string* member) {
  std::string rawSetMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawSetMetaValue);
  if (!s.ok()) return s;
  SetMetaValue metaValue = SetMetaValue(rawSetMetaValue);

  if (metaValue.len == 0) return Status::NotFound("empty set");
  uint64_t index = rand_uint64(0, metaValue.len - 1);

  Iterator* iter = db_->NewIterator(ReadOptions());
  iter->Seek(key);
  iter->Next();
  while (index--) {
    assert(iter->Valid());
    iter->Next();
  }
  SetNodeKey nodeKey(iter->key(), key.size());
  *member = nodeKey.setKey().ToString();
  delete iter;
  return Status::OK();
}

Status RedisSetBasicImpl::SRandMember(const Slice& key,
                                      int64_t count,
                                      std::vector<std::string>* members) {
  std::string rawSetMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawSetMetaValue);
  if (!s.ok()) return s;
  SetMetaValue metaValue = SetMetaValue(rawSetMetaValue);

  if (count == 0) return Status::OK();
  std::vector<std::uint64_t> indices;
  std::vector<std::uint64_t> offsets(abs(count));
  if (count > 0) {
    indices = std::vector<std::uint64_t>(metaValue.len);
    if (count > metaValue.len) offsets.resize(metaValue.len);
    std::iota(indices.begin(), indices.end(), 0);
    std::shuffle(indices.begin(), indices.end(), std::mt19937{std::random_device{}()});
    std::sort(indices.begin(), indices.begin() + (ptrdiff_t)offsets.size());
  } else {
    indices = rand_uint64(0, metaValue.len - 1, abs(count));
    std::sort(indices.begin(), indices.end());
  }
  offsets[0] = indices[0];
  for (int c = 1; c < offsets.size(); c++) {
    offsets[c] = indices[c] - indices[c - 1];
  }

  Iterator* iter = db_->NewIterator(ReadOptions());
  iter->Seek(key);
  iter->Next();
  for (auto offset: offsets) {
    while (offset) {
      assert(iter->Valid());
      iter->Next();
      offset--;
    }
    SetNodeKey nodeKey(iter->key(), key.size());
    members->push_back(nodeKey.setKey().ToString());
  }
  delete iter;
  std::shuffle(members->begin(), members->end(), std::mt19937{std::random_device{}()});

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

  std::set<Slice>::const_iterator updatesIter = keys.cbegin();
  if (metaValue.len) {
    Iterator* iter = db_->NewIterator(ReadOptions());
    iter->Seek(key);
    iter->Next();
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
    delete iter;
  }
  while (updatesIter != keys.cend()) {
    SetNodeKey nodeKey(key, *updatesIter);
    updates.Put(nodeKey.Encode(), "");
    updatesIter++;
    *count += 1;
  }

  if (*count) {
    metaValue.len += *count;
    updates.Put(key, metaValue.Encode());
  }
  return db_->Write(WriteOptions(), &updates);
}

Status RedisSetBasicImpl::SRem(const Slice& key,
                               const Slice& member,
                               uint64_t* count) {
  std::string rawSetMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawSetMetaValue);
  if (!s.ok()) return s;
  SetMetaValue metaValue = SetMetaValue(rawSetMetaValue);

  SetNodeKey nodeKey(key, member);
  std::string _;
  s = db_->Get(ReadOptions(), nodeKey.Encode(), &_);
  if (!s.ok() && !s.IsNotFound()) return s;
  if (s.IsNotFound()) {
    *count = 0;
    return Status::OK();
  }

  WriteBatch updates;
  *count = 1;
  metaValue.len -= 1;
  updates.Put(key, metaValue.Encode());
  updates.Delete(nodeKey.Encode());
  return db_->Write(WriteOptions(), &updates);
}

Status RedisSetBasicImpl::SRem(const Slice& key,
                               const std::set<Slice>& members,
                               uint64_t* count) {
  std::string rawSetMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawSetMetaValue);
  if (!s.ok()) return s;
  SetMetaValue metaValue = SetMetaValue(rawSetMetaValue);

  WriteBatch updates;
  *count = 0;

  std::set<Slice>::const_iterator updatesIter = members.cbegin();
  Iterator *iter = db_->NewIterator(ReadOptions());
  iter->Seek(key);
  iter->Next();
  while (iter->Valid() && updatesIter != members.cend()) {
    int cmp = updatesIter->compare({iter->key().data() + key.size() + 1,
                                    iter->key().size() - key.size() - 1});
    if (cmp == 0) {
      SetNodeKey nodeKey(key, *updatesIter);
      updates.Delete(nodeKey.Encode());
      *count += 1;
      ++updatesIter;
      iter->Next();
    } else if (cmp > 0) {
      iter->Next();
    } else {
      ++updatesIter;
    }
  }
  delete iter;
  if (*count) {
    metaValue.len -= *count;
    updates.Put(key, metaValue.Encode());
  }
  return db_->Write(WriteOptions(), &updates);
}

Status RedisSetBasicImpl::SPop(const Slice& key,
                               std::string* member) {
  std::string rawSetMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawSetMetaValue);
  if (!s.ok()) return s;
  SetMetaValue metaValue = SetMetaValue(rawSetMetaValue);

  s = SRandMember(key, member);
  if (!s.ok()) return s;

  WriteBatch updates;
  metaValue.len -= 1;
  updates.Put(key, metaValue.Encode());
  SetNodeKey nodeKey(key, *member);
  updates.Delete(nodeKey.Encode());
  return db_->Write(WriteOptions(), &updates);
}

Status RedisSetBasicImpl::SPop(const Slice& key,
                               uint64_t count,
                               std::vector<std::string>* members) {
  std::string rawSetMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawSetMetaValue);
  if (!s.ok()) return s;
  SetMetaValue metaValue = SetMetaValue(rawSetMetaValue);

  s = SRandMember(key, (int64_t)count, members);
  if (!s.ok()) return s;

  WriteBatch updates;
  assert(metaValue.len >= members->size());
  metaValue.len -= members->size();
  updates.Put(key, metaValue.Encode());
  for (const auto& member: *members) {
    SetNodeKey nodeKey(key, member);
    updates.Delete(nodeKey.Encode());
  }
  return db_->Write(WriteOptions(), &updates);
}

Status RedisSetBasicImpl::SMove(const Slice& srcKey,
                                const Slice& dstKey,
                                const Slice& member,
                                uint64_t* count) {
  return Status::NotSupported("");
}

uint64_t RedisSetBasicImpl::CountKeyIntersection(const Slice& key, const SetNodeKey& nodeKey) {
  std::string _;
  Status s = db_->Get(ReadOptions(), nodeKey.Encode(), &_);
  return s.ok();
}


}