#include "redis_string_basic_impl.h"

#include <string>

#include "leveldb/db.h"
#include "layout.h"

namespace merodis {

RedisStringBasicImpl::RedisStringBasicImpl() noexcept = default;

RedisStringBasicImpl::~RedisStringBasicImpl() noexcept = default;

Status RedisStringBasicImpl::Open(const Options& options, const std::string& db_path) noexcept {
  return leveldb::DB::Open(options, db_path, &db_);
}

Status RedisStringBasicImpl::Get(const Slice& key, std::string* value) noexcept {
  return db_->Get(ReadOptions(), key, value);
}

Status RedisStringBasicImpl::Set(const Slice& key, const Slice& value) noexcept {
  return db_->Put(WriteOptions(), key, value);
}

}