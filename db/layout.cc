#include "layout.h"

#include <utility>

#include "util/coding.h"
#include "util/number.h"

namespace merodis {

Value::Value() noexcept: s() {}
Value::Value(Slice s) noexcept: s(s) {}
Value::Value(int64_t i64) noexcept: i64(i64) {}
Value::~Value() noexcept {}

TypedValue::TypedValue() noexcept:
  type(kString),
  value() {}

TypedValue::TypedValue(const Slice& s) noexcept {
  int64_t n;
  if (SliceToInt64(s, n) == 0) {
    type = kInt64;
    value.i64 = n;
  } else {
    type = kString;
    value.s = s;
  }
}

TypedValue::TypedValue(int64_t v) noexcept:
  type(kInt64),
  value(v) {
}

void TypedValue::parse(const std::string& raw) noexcept {
  assert(!raw.empty());
  type = (enum ValueType)raw[0];
  switch (type) {
    case kString:
      value.s = {raw.data() + 1, raw.size() - 1};
      break;
    case kInt64:
      value.i64 = DecodeFixed64<int64_t>(raw.data() + 1);
      break;
    default:
      assert(0);
  }
}

std::string TypedValue::Encode() const noexcept {
  std::string raw;
  switch (type) {
    case kString:
      raw.resize(sizeof(type) + value.s.size());
      raw.replace(1, std::string::npos, value.s.data(), value.s.size());
      break;
    case kInt64:
      raw.resize(sizeof(type) + sizeof(int64_t));
      EncodeFixed64<int64_t>(raw.data() + 1, value.i64);
      break;
    default:
      assert(0);
  }
  raw[0] = type;
  return raw;
}

std::string TypedValue::ToString() const noexcept {
  switch (type) {
    case kString:
      return value.s.ToString();
    case kInt64:
      return std::to_string(value.i64);
    default:
      assert(0);
  }
}

}
