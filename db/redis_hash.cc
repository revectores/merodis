#include "redis_hash.h"

#include "leveldb/db.h"
#include "leveldb/write_batch.h"

namespace merodis {

RedisHash::RedisHash() noexcept = default;

RedisHash::~RedisHash() noexcept = default;

Status RedisHash::Open(const Options& options, const std::string& db_path) noexcept {
  return leveldb::DB::Open(options, db_path, &db_);
}

}