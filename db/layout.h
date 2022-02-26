#ifndef MERODIS_LAYOUT_H
#define MERODIS_LAYOUT_H

#include <string>
#include <variant>
#include "merodis/merodis.h"

namespace merodis {

enum ValueType {
  kString = 0x00,
  kInt64 = 0x01,
};

union Value {
  explicit Value() noexcept;
  explicit Value(Slice s) noexcept;
  explicit Value(int64_t) noexcept;
  ~Value() noexcept;
  Slice s;
  int64_t i64;
};

struct TypedValue {
  explicit TypedValue() noexcept;
  explicit TypedValue(const Slice& s) noexcept;
  explicit TypedValue(int64_t value) noexcept;

  void parse(const std::string& raw) noexcept;

  std::string Encode() const noexcept;
  std::string ToString() const noexcept;

  std::variant<Slice, int64_t> value;
};

}

#endif //MERODIS_LAYOUT_H
