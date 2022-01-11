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
                       const Slice& value) {
  std::string rawHashMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawHashMetaValue);
  HashMetaValue metaValue;
  if (s.ok()) metaValue = HashMetaValue(rawHashMetaValue);
  WriteBatch updates;

  std::string _;
  HashNodeKey nodeKey(key, hashKey);
  s = db_->Get(ReadOptions(), nodeKey.Encode(), &_);
  if (!s.ok()) {
    metaValue.len += 1;
    updates.Put(key, metaValue.Encode());
  }
  updates.Put(nodeKey.Encode(), value);
  return db_->Write(WriteOptions(), &updates);
}

}