#include "redis_hash_basic_impl.h"

#include "leveldb/db.h"
#include "leveldb/write_batch.h"

namespace merodis {

RedisHashBasicImpl::RedisHashBasicImpl() noexcept = default;

RedisHashBasicImpl::~RedisHashBasicImpl() noexcept = default;

Status RedisHashBasicImpl::HLen(const Slice& key,
                                uint64_t* len) {
  std::string rawHashMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawHashMetaValue);
  if (s.ok()) {
    HashMetaValue metaValue(rawHashMetaValue);
    *len = metaValue.len;
  } else if (s.IsNotFound()) {
    *len = 0;
    s = Status::OK();
  }
  return s;
}

Status RedisHashBasicImpl::HGet(const Slice& key,
                                const Slice& hashKey,
                                std::string* value) {
  return db_->Get(ReadOptions(), HashNodeKey(key, hashKey).Encode(), value);
}

Status RedisHashBasicImpl::HMGet(const Slice& key,
                                 const std::vector<Slice>& hashKeys,
                                 std::vector<std::string>* values){
  std::map<Slice, Slice> kvs;
  for (auto const& k: hashKeys) kvs[k] = "";

  Iterator* iter = db_->NewIterator(ReadOptions());
  for (auto const& [k, _] : kvs) {
    iter->Seek(HashNodeKey(key, k).Encode());
    if (!iter->Valid()) break;
    kvs[k] = iter->value();
  }
  delete iter;

  values->reserve(values->size() + hashKeys.size());
  for (const auto& k: hashKeys) values->emplace_back(kvs[k].ToString());
  return Status::OK();
}

Status RedisHashBasicImpl::HGetAll(const Slice& key, std::map<std::string, std::string>* kvs) {
  Iterator* iter = db_->NewIterator(ReadOptions());
  iter->Seek(key);
  if (!iter->Valid()) {
    delete iter;
    return Status::OK();
  }
  iter->Next();
  for (; iter->Valid(); iter->Next()) {
    if (iter->key().size() <= key.size() || iter->key()[key.size()] != 0) break;
    HashNodeKey nodeKey(iter->key(), key.size());
    kvs->insert({nodeKey.hashKey().ToString(), iter->value().ToString()});
  }
  delete iter;
  return Status::OK();
}

Status RedisHashBasicImpl::HKeys(const Slice& key, std::vector<std::string>* keys) {
  Iterator* iter = db_->NewIterator(ReadOptions());
  iter->Seek(key);
  if (!iter->Valid()) {
    delete iter;
    return Status::OK();
  }
  iter->Next();
  for (; iter->Valid(); iter->Next()) {
    if (iter->key().size() <= key.size() || iter->key()[key.size()] != 0) break;
    HashNodeKey nodeKey(iter->key(), key.size());
    keys->push_back(nodeKey.hashKey().ToString());
  }
  delete iter;
  return Status::OK();
}

Status RedisHashBasicImpl::HVals(const Slice& key, std::vector<std::string>* values) {
  Iterator* iter = db_->NewIterator(ReadOptions());
  iter->Seek(key);
  if (!iter->Valid()) {
    delete iter;
    return Status::OK();
  }
  iter->Next();
  for (; iter->Valid(); iter->Next()) {
    if (iter->key().size() <= key.size() || iter->key()[key.size()] != 0) break;
    HashNodeKey nodeKey(iter->key(), key.size());
    values->push_back(iter->value().ToString());
  }
  delete iter;
  return Status::OK();
}

Status RedisHashBasicImpl::HExists(const Slice& key, const Slice& hashKey, bool* exists) {
  std::string _;
  Status s = db_->Get(ReadOptions(), HashNodeKey(key, hashKey).Encode(), &_);
  if (s.ok()) {
    *exists = true;
  } else if (s.IsNotFound()) {
    *exists = false;
    s = Status::OK();
  }
  return s;
}

Status RedisHashBasicImpl::HSet(const Slice& key,
                                const Slice& hashKey,
                                const Slice& value,
                                uint64_t* count) {
  std::string rawHashMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawHashMetaValue);
  HashMetaValue metaValue;
  if (s.ok()) metaValue = HashMetaValue(rawHashMetaValue);

  WriteBatch updates;
  HashNodeKey nodeKey(key, hashKey);
  updates.Put(nodeKey.Encode(), value);

  *count = 1 - CountKeysIntersection(key, nodeKey);
  if (*count) {
    metaValue.len += 1;
    updates.Put(key, metaValue.Encode());
  }
  return db_->Write(WriteOptions(), &updates);
}

Status RedisHashBasicImpl::HSet(const Slice& key,
                                const std::map<Slice, Slice>& kvs,
                                uint64_t* count) {
  std::string rawHashMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawHashMetaValue);
  HashMetaValue metaValue;
  if (s.ok()) metaValue = HashMetaValue(rawHashMetaValue);
  WriteBatch updates;

  for (const auto&[k, v]: kvs) {
    HashNodeKey nodeKey(key, k);
    updates.Put(nodeKey.Encode(), v);
  }

  *count = kvs.size() - (metaValue.len ? CountKeysIntersection(key, kvs) : 0);
  if (*count) {
    metaValue.len += *count;
    updates.Put(key, metaValue.Encode());
  }
  return db_->Write(WriteOptions(), &updates);
}

Status RedisHashBasicImpl::HDel(const Slice& key,
                                const Slice& hashKey,
                                uint64_t* count) {
  std::string rawHashMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawHashMetaValue);
  HashMetaValue metaValue;
  if (s.ok()) metaValue = HashMetaValue(rawHashMetaValue);

  WriteBatch updates;
  HashNodeKey nodeKey(key, hashKey);
  updates.Delete(nodeKey.Encode());

  *count = CountKeysIntersection(key, nodeKey);
  if (*count) {
    metaValue.len -= 1;
    updates.Put(key, metaValue.Encode());
  }
  return db_->Write(WriteOptions(), &updates);
}

Status RedisHashBasicImpl::HDel(const Slice& key,
                                const std::set<Slice>& hashKeys,
                                uint64_t* count) {
  std::string rawHashMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawHashMetaValue);
  HashMetaValue metaValue;
  if (s.ok()) metaValue = HashMetaValue(rawHashMetaValue);
  WriteBatch updates;

  for (const auto& k: hashKeys) {
    HashNodeKey nodeKey(key, k);
    updates.Delete(nodeKey.Encode());
  }

  *count = metaValue.len ? CountKeysIntersection(key, hashKeys) : 0;
  if (*count) {
    metaValue.len -= *count;
    updates.Put(key, metaValue.Encode());
  }
  return db_->Write(WriteOptions(), &updates);
}

uint64_t RedisHashBasicImpl::CountKeysIntersection(const Slice& key, const HashNodeKey& nodeKey) {
  std::string _;
  Status s = db_->Get(ReadOptions(), nodeKey.Encode(), &_);
  return s.ok();
}

uint64_t RedisHashBasicImpl::CountKeysIntersection(const Slice& key, const std::set<Slice>& hashKeys) {
  uint64_t count = 0;
  Iterator* iter = db_->NewIterator(ReadOptions());
  iter->Seek(key);
  iter->Next();
  std::set<Slice>::const_iterator updatesIter = hashKeys.cbegin();
  while (iter->Valid() && updatesIter != hashKeys.cend()) {
    int cmp = updatesIter->compare({iter->key().data() + key.size() + 1,
                                    iter->key().size() - key.size() - 1});
    if (cmp == 0) {
      ++updatesIter;
      iter->Next();
      count += 1;
    } else if (cmp > 0) {
      iter->Next();
    } else {
      ++updatesIter;
    }
  }
  delete iter;
  return count;
}

uint64_t RedisHashBasicImpl::CountKeysIntersection(const Slice& key, const std::map<Slice, Slice>& kvs) {
  uint64_t count = 0;
  Iterator* iter = db_->NewIterator(ReadOptions());
  iter->Seek(key);
  iter->Next();
  std::map<Slice, Slice>::const_iterator updatesIter = kvs.cbegin();
  while (iter->Valid() && updatesIter != kvs.cend()) {
    int cmp = updatesIter->first.compare({iter->key().data() + key.size() + 1,
                                          iter->key().size() - key.size() - 1});
    if (cmp == 0) {
      ++updatesIter;
      iter->Next();
      count += 1;
    } else if (cmp > 0) {
      iter->Next();
    } else {
      ++updatesIter;
    }
  }
  delete iter;
  return count;
}

}