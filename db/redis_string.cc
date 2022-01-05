#include "redis_string.h"

#include <string>

#include "leveldb/db.h"

namespace merodis {

RedisString::RedisString() noexcept = default;

RedisString::~RedisString() noexcept = default;

Status RedisString::Open(const Options& options, const std::string& db_path) noexcept {
  return leveldb::DB::Open(options, db_path, &db_);
}

Status RedisString::Get(const Slice& key, std::string* value) noexcept {
  return db_->Get(leveldb::ReadOptions(), key, value);
}

Status RedisString::Set(const Slice& key, const Slice& value) noexcept {
  return db_->Put(leveldb::WriteOptions(), key, value);
}

}