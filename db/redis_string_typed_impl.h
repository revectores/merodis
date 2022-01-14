#ifndef MERODIS_REDIS_STRING_TYPED_IMPL_H
#define MERODIS_REDIS_STRING_TYPED_IMPL_H

#include "redis_string.h"


namespace merodis {

class RedisStringTypedImpl final : public RedisString {
public:
  RedisStringTypedImpl() noexcept;
  ~RedisStringTypedImpl() noexcept final;

  Status Open(const Options& options, const std::string& db_path) noexcept final;
  Status Get(const Slice& key, std::string* value) noexcept final;
  Status Set(const Slice& key, const Slice& value) noexcept final;
  Status Incr(const Slice& key, int64_t* result) noexcept final;
  Status IncrBy(const Slice& key, int64_t increment, int64_t* result) noexcept final;
  Status Decr(const Slice& key, int64_t* result) noexcept final;
  Status DecrBy(const Slice& key, int64_t decrement, int64_t* result) noexcept final;
};

}


#endif //MERODIS_REDIS_STRING_TYPED_IMPL_H
