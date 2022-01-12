#include "redis_hash.h"

#include "leveldb/db.h"
#include "leveldb/write_batch.h"

namespace merodis {

RedisHash::RedisHash() noexcept = default;

RedisHash::~RedisHash() noexcept = default;

Status RedisHash::Open(const Options& options, const std::string& db_path) noexcept {
  return leveldb::DB::Open(options, db_path, &db_);
}

Status RedisHash::HLen(const Slice& key,
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

Status RedisHash::HGet(const Slice& key,
                       const Slice& hashKey,
                       std::string* value) {
  return db_->Get(ReadOptions(), HashNodeKey(key, hashKey).Encode(), value);
}

Status RedisHash::HGetAll(const Slice& key, std::map<std::string, std::string>* kvs) {
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

Status RedisHash::HKeys(const Slice& key, std::vector<std::string>* keys) {
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

Status RedisHash::HVals(const Slice& key, std::vector<std::string>* values) {
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

Status RedisHash::HExists(const Slice& key, const Slice& hashKey, bool* exists) {
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

Status RedisHash::HSet(const Slice& key,
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

Status RedisHash::HSet(const Slice& key,
                       const std::map<std::string, std::string>& kvs,
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

Status RedisHash::HDel(const Slice& key,
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

Status RedisHash::HDel(const Slice& key,
                       const std::set<std::string>& hashKeys,
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

uint64_t RedisHash::CountKeysIntersection(const Slice& key, const HashNodeKey& nodeKey) {
  std::string _;
  Status s = db_->Get(ReadOptions(), nodeKey.Encode(), &_);
  return s.ok();
}

uint64_t RedisHash::CountKeysIntersection(const Slice& key, const std::set<std::string>& hashKeys) {
  uint64_t count = 0;
  Iterator* iter = db_->NewIterator(ReadOptions());
  iter->Seek(key);
  iter->Next();
  std::set<std::string>::const_iterator updatesIter = hashKeys.cbegin();
  while (iter->Valid() && updatesIter != hashKeys.cend()) {
    int cmp = updatesIter->compare(0, updatesIter->size(),
                                   iter->key().data() + key.size() + 1, iter->key().size() - key.size() - 1);
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

uint64_t RedisHash::CountKeysIntersection(const Slice& key, const std::map<std::string, std::string>& kvs) {
  uint64_t count = 0;
  Iterator* iter = db_->NewIterator(ReadOptions());
  iter->Seek(key);
  iter->Next();
  std::map<std::string, std::string>::const_iterator updatesIter = kvs.cbegin();
  while (iter->Valid() && updatesIter != kvs.cend()) {
    int cmp = updatesIter->first.compare(0, updatesIter->first.size(),
                                         iter->key().data() + key.size() + 1, iter->key().size() - key.size() - 1);
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