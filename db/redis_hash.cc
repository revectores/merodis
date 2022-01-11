#include "redis_hash.h"

#include "leveldb/db.h"
#include "leveldb/write_batch.h"

namespace merodis {

RedisHash::RedisHash() noexcept = default;

RedisHash::~RedisHash() noexcept = default;

Status RedisHash::Open(const Options& options, const std::string& db_path) noexcept {
  return leveldb::DB::Open(options, db_path, &db_);
}

Status RedisHash::HGet(const Slice& key, const Slice& hashKey, std::string* value) {
  return db_->Get(ReadOptions(), HashNodeKey(key, hashKey).Encode(), value);
}

Status RedisHash::HSet(const Slice& key, const Slice& hashKey, const Slice& value) {
  return db_->Put(WriteOptions(), HashNodeKey(key, hashKey).Encode(), value);
}

}