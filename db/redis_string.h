#ifndef MERODIS_REDIS_STRING_H
#define MERODIS_REDIS_STRING_H

#include <cstdint>
#include <string>

#include "redis.h"

namespace merodis {

class RedisString : public Redis {
public:
  RedisString() noexcept = default;
  ~RedisString() noexcept override = default;

  virtual Status Get(const Slice& key, std::string* value) noexcept = 0;
  virtual Status Set(const Slice& key, const Slice& value) noexcept = 0;
  virtual Status Incr(const Slice& key, int64_t* result) noexcept = 0;
  virtual Status IncrBy(const Slice& key, int64_t increment, int64_t* result) noexcept = 0;
  virtual Status Decr(const Slice& key, int64_t* result) noexcept = 0;
  virtual Status DecrBy(const Slice& key, int64_t decrement, int64_t* result) noexcept = 0;
};

}


#endif //MERODIS_REDIS_STRING_H
