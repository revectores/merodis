#include "redis_string.h"

#include <string>

#include "leveldb/db.h"
#include "layout.h"

namespace merodis {

RedisString::RedisString() noexcept = default;

RedisString::~RedisString() noexcept = default;

Status RedisString::Open(const Options& options, const std::string& db_path) noexcept {
  return leveldb::DB::Open(options, db_path, &db_);
}

Status RedisString::Get(const Slice& key, std::string* value) noexcept {
  std::string raw;
  Status s = db_->Get(ReadOptions(), key, &raw);
  if (!s.ok()) return s;
  TypedValue typedValue;
  typedValue.parse(raw);
  *value = typedValue.ToString();
  return s;
}

Status RedisString::Set(const Slice& key, const Slice& value) noexcept {
  TypedValue typedValue(value);
  return db_->Put(WriteOptions(), key, typedValue.Encode());
}

}