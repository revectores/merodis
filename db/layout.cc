#include "layout.h"

#include <utility>

#include "util/coding.h"
#include "util/number.h"
#include "util/variant_helper.h"

namespace merodis {

Value::Value() noexcept: s() {}
Value::Value(Slice s) noexcept: s(s) {}
Value::Value(int64_t i64) noexcept: i64(i64) {}
Value::~Value() noexcept {}

TypedValue::TypedValue() noexcept:
  value("") {}

TypedValue::TypedValue(const Slice& s) noexcept {
  int64_t n;
  if (SliceToInt64(s, n) == 0) {
    value = n;
  } else {
    value = s;
  }
}

TypedValue::TypedValue(int64_t v) noexcept:
  value(v) {}

void TypedValue::parse(const std::string& raw) noexcept {
  assert(!raw.empty());
  enum ValueType type = (enum ValueType)raw[0];
  switch (type) {
    case kString:
      value = Slice{raw.data() + 1, raw.size() - 1};
      break;
    case kInt64:
      value = DecodeFixed64<int64_t>(raw.data() + 1);
      break;
    default:
      assert(0);
  }
}

std::string TypedValue::Encode() const noexcept {
  std::string raw;
  std::visit(overloaded {
    [&raw](const Slice& v) {
      raw.resize(1 + v.size());
      raw.replace(1, std::string::npos, v.data(), v.size());
      raw[0] = kString;
    },
    [&raw](int64_t v) {
      raw.resize(1 + sizeof(int64_t));
      EncodeFixed64<int64_t>(raw.data() + 1, v);
      raw[0] = kInt64;
    },
  }, value);
  return raw;
}

std::string TypedValue::ToString() const noexcept {
  std::string str;
  std::visit(overloaded {
    [&str](const Slice& v) { str = v.ToString(); },
    [&str](int64_t v) { str = std::to_string(v); }
  }, value);
  return str;
}

}
