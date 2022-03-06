#ifndef MERODIS_REDIS_LIST_H
#define MERODIS_REDIS_LIST_H

#include <cstdint>
#include <string>
#include <vector>

#include "redis.h"

namespace merodis {

class RedisList : public Redis {
public:
  RedisList() noexcept = default;
  ~RedisList() noexcept override = default;

  virtual Status LLen(const Slice& key, uint64_t* len) noexcept = 0;
  virtual Status LIndex(const Slice& key, UserIndex index, std::string* value) noexcept = 0;
  virtual Status LPos(const Slice& key, const Slice& value, int64_t rank, int64_t count, int64_t maxlen, std::vector<uint64_t>& indices) noexcept = 0;
  virtual Status LRange(const Slice& key, UserIndex from, UserIndex to, std::vector<std::string>* values) noexcept = 0;
  virtual Status LSet(const Slice& key, UserIndex index, const Slice& value) noexcept = 0;
  virtual Status Push(const Slice& key, const Slice& value, bool createListIfNotFound, enum Side side) noexcept = 0;
  virtual Status Push(const Slice& key, const std::vector<Slice>& values, bool createListIfNotFound, enum Side side) noexcept = 0;
  virtual Status Pop(const Slice& key, std::string* value, enum Side side) noexcept = 0;
  virtual Status Pop(const Slice& key, uint64_t count, std::vector<std::string>* values, enum Side side) noexcept = 0;
  virtual Status LTrim(const Slice& key, UserIndex from, UserIndex to) noexcept = 0;
  virtual Status LInsert(const Slice& key, const BeforeOrAfter& beforeOrAfter, const Slice& pivotValue, const Slice& value) noexcept = 0;
  virtual Status LRem(const Slice& key, int64_t count, const Slice& value, uint64_t* removedCount) noexcept = 0;
  virtual Status LMove(const Slice& srcKey, const Slice& dstKey, enum Side srcSide, enum Side dstSide, std::string* value) noexcept = 0;
};

}


#endif //MERODIS_REDIS_LIST_H
