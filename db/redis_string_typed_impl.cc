#include "redis_string_typed_impl.h"

#include <cstdint>
#include <string>

#include "layout.h"
#include "util/number.h"
#include "util/variant_helper.h"

namespace merodis {

RedisStringTypedImpl::RedisStringTypedImpl() noexcept = default;

RedisStringTypedImpl::~RedisStringTypedImpl() noexcept = default;

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
  return IncrBy(key, 1, result);
}

Status RedisStringTypedImpl::IncrBy(const Slice& key, int64_t increment, int64_t* result) noexcept {
  std::string raw;
  Status s = db_->Get(ReadOptions(), key, &raw);
  if (!s.ok() && !s.IsNotFound()) return s;
  TypedValue typedValue;
  if (s.IsNotFound()) {
    typedValue.value = 0;
  } else {
    typedValue.parse(raw);
  }
  std::visit(overloaded {
    [&s](const Slice& v) {
      int64_t n;
      int e = SliceToInt64(v, n);
      s = Status::OK();
      if (e == EINVAL) s = Status::InvalidArgument("Value is not an integer");
      if (e == ERANGE) s = Status::InvalidArgument("Value is out of range");
    },
    [&s, increment, &result](int64_t n) {
      RangeError re = CheckAdditionRangeError(n, increment);
      switch(re) {
        case RangeError::kOK:
          *result = n + increment;
          s = Status::OK();
          break;
        case RangeError::kOverflow:
          *result = n;
          s = Status::InvalidArgument("Result overflow");
          break;
        case RangeError::kUnderflow:
          *result = n;
          s = Status::InvalidArgument("Result underflow");
          break;
      }
    }
  }, typedValue.value);
  if (!s.ok()) return s;
  typedValue.value = *result;
  return db_->Put(WriteOptions(), key, typedValue.Encode());
}

Status RedisStringTypedImpl::Decr(const Slice& key, int64_t* result) noexcept {
  return IncrBy(key, -1, result);
}

Status RedisStringTypedImpl::DecrBy(const Slice& key, int64_t decrement, int64_t* result) noexcept {
  return IncrBy(key, -decrement, result);
}

}