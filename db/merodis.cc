#include "merodis/merodis.h"

#include <cstdio>

#include "redis_string.h"
#include "redis_list.h"

namespace merodis {

Merodis::Merodis() noexcept :
  string_db_(nullptr),
  list_db_(nullptr) {}

Merodis::~Merodis() noexcept {
  delete string_db_;
  delete list_db_;
};

Status Merodis::Open(const Options& options, const std::string& db_path) noexcept {
  Status s;
  string_db_ = new RedisString;
  list_db_ = new RedisList;
  s = string_db_->Open(options, db_path + "/string");
  if (!s.ok()) return s;
  s = list_db_->Open(options, db_path + "/list");
  return s;
}

Status Merodis::DestroyDB(const std::string &db_path, Options options) noexcept {
  Status s = leveldb::DestroyDB(db_path + "/string", options);
  if (!s.ok()) return s;
  return leveldb::DestroyDB(db_path + "/list", options);
}

Status Merodis::Get(const Slice& key, std::string* value) noexcept {
  return string_db_->Get(key, value);
}

Status Merodis::Set(const Slice& key, const Slice& value) noexcept {
  return string_db_->Set(key, value);
}

Status Merodis::LLen(const Slice& key, uint64_t* len) noexcept {
  return list_db_->LLen(key, len);
}

Status Merodis::LIndex(const Slice &key, uint64_t index, std::string *value) noexcept {
  return list_db_->LIndex(key, index, value);
}

Status Merodis::LPush(const Slice& key, const Slice& value) noexcept {
  return list_db_->LPush(key, value);
}

}
