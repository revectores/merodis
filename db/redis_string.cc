#include "redis_string.h"

#include <string>

#include "leveldb/db.h"

namespace merodis {

RedisString::RedisString() noexcept = default;

RedisString::~RedisString() noexcept = default;

Status RedisString::Open(const std::string& db_path) noexcept {
  leveldb::Options op;
  op.create_if_missing = true;
  return leveldb::DB::Open(op, db_path, &db_);
}

Status RedisString::Get(const Slice& key, std::string* value) noexcept {
  return db_->Get(leveldb::ReadOptions(), key, value);
}

Status RedisString::Set(const Slice& key, const Slice& value) noexcept {
  return db_->Put(leveldb::WriteOptions(), key, value);
}

}