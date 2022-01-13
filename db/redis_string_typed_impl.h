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
};

}


#endif //MERODIS_REDIS_STRING_TYPED_IMPL_H
