#include "redis_string_basic_impl.h"

#include <cerrno>
#include <cstdint>
#include <string>

#include "util/number.h"

namespace merodis {

RedisStringBasicImpl::RedisStringBasicImpl() noexcept = default;

RedisStringBasicImpl::~RedisStringBasicImpl() noexcept = default;

Status RedisStringBasicImpl::Get(const Slice& key,
                                 std::string* value) noexcept {
  return db_->Get(ReadOptions(), key, value);
}

Status RedisStringBasicImpl::Set(const Slice& key,
                                 const Slice& value) noexcept {
  return db_->Put(WriteOptions(), key, value);
}

Status RedisStringBasicImpl::Incr(const Slice& key,
                                  int64_t* result) noexcept {
  return IncrBy(key, 1, result);
}

Status RedisStringBasicImpl::IncrBy(const Slice& key,
                                    int64_t increment,
                                    int64_t* result) noexcept {
  std::string value;
  Status s = db_->Get(ReadOptions(), key, &value);
  if (!s.ok() && !s.IsNotFound()) return s;
  if (s.IsNotFound()) value = "0";

  int64_t n;
  int e = SliceToInt64(value, n);
  if (e == EINVAL) {
    return Status::InvalidArgument("Value is not an integer");
  }
  if (e == ERANGE) {
    return Status::InvalidArgument("Value is out of range");
  }

  RangeError re = CheckAdditionRangeError(n, increment);
  switch (re) {
    case RangeError::kOK:
      *result = n + increment;
      break;
    case RangeError::kOverflow:
      *result = n;
      return Status::InvalidArgument("Result overflow");
    case RangeError::kUnderflow:
      *result = n;
      return Status::InvalidArgument("Result underflow");
  }
  return db_->Put(WriteOptions(), key, std::to_string(*result));
}

Status RedisStringBasicImpl::Decr(const Slice& key,
                                  int64_t* result) noexcept {
  return IncrBy(key, -1, result);
}

Status RedisStringBasicImpl::DecrBy(const Slice& key,
                                    int64_t decrement,
                                    int64_t* result) noexcept {
  return IncrBy(key, -decrement, result);
}

}