#ifndef MERODIS_NUMBER_H
#define MERODIS_NUMBER_H

#include <cstdlib>
#include <cerrno>

#include "leveldb/slice.h"

enum class RangeError {
  kOK,
  kOverflow,
  kUnderflow
};

inline int SliceToInt64(const leveldb::Slice& s, int64_t& n) {
  const int prevErrno = errno;
  errno = 0;
  char* ptr = nullptr;
  n = strtoll(s.data(), &ptr, 10);
  if (s.empty() || ptr != s.data() + s.size()) errno = EINVAL;
  const int thisErrno = errno;
  if (errno) n = 0;
  errno = prevErrno;
  return thisErrno;
}

inline enum RangeError CheckAdditionRangeError(int64_t n1, int64_t n2) {
  if (n2 > 0 && std::numeric_limits<int64_t>::max() - n2 < n1) {
    return RangeError::kOverflow;
  }
  if (n2 < 0 && std::numeric_limits<int64_t>::min() - n2 > n1) {
    return RangeError::kUnderflow;
  }
  return RangeError::kOK;
}

#endif //MERODIS_NUMBER_H
