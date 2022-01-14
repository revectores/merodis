#include "redis_string_typed_impl.h"

#include <string>

#include "leveldb/db.h"
#include "layout.h"

namespace merodis {

RedisStringTypedImpl::RedisStringTypedImpl() noexcept = default;

RedisStringTypedImpl::~RedisStringTypedImpl() noexcept = default;

Status RedisStringTypedImpl::Open(const Options& options, const std::string& db_path) noexcept {
  return leveldb::DB::Open(options, db_path, &db_);
}

Status RedisStringTypedImpl::Get(const Slice& key, std::string* value) noexcept {
  std::string raw;
  Status s = db_->Get(ReadOptions(), key, &raw);
  if (!s.ok()) return s;
  TypedValue typedValue;
  typedValue.parse(raw);
  *value = typedValue.ToString();
  return s;
}

Status RedisStringTypedImpl::Set(const Slice& key, const Slice& value) noexcept {
  TypedValue typedValue(value);
  return db_->Put(WriteOptions(), key, typedValue.Encode());
}

Status RedisStringTypedImpl::Incr(const Slice& key, int64_t* result) noexcept {
  return Status::NotSupported("");
}

Status RedisStringTypedImpl::IncrBy(const Slice& key, int64_t increment, int64_t* result) noexcept {
  return Status::NotSupported("");
}

Status RedisStringTypedImpl::Decr(const Slice& key, int64_t* result) noexcept {
  return Status::NotSupported("");
}

Status RedisStringTypedImpl::DecrBy(const Slice& key, int64_t decrement, int64_t* result) noexcept {
  return Status::NotSupported("");
}

}