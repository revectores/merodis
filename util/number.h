#ifndef MERODIS_NUMBER_H
#define MERODIS_NUMBER_H

#include <cstdlib>
#include <cerrno>

#include "leveldb/slice.h"

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

#endif //MERODIS_NUMBER_H
