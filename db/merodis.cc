#include "merodis/merodis.h"

#include "redis_string.h"

namespace merodis {

Merodis::Merodis() noexcept :
  string_db_(nullptr) {}

Merodis::~Merodis() noexcept = default;

Status Merodis::Open(const std::string& db_path) noexcept {
  string_db_ = new RedisString;
  string_db_->Open(db_path);
  return {};
}

Status Merodis::Get(const Slice& key, std::string* value) noexcept {
  return string_db_->Get(key, value);
}

Status Merodis::Set(const Slice& key, const Slice& value) noexcept {
  return string_db_->Set(key, value);
}

}
