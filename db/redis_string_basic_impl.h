#ifndef MERODIS_REDIS_STRING_BASIC_IMPL_H
#define MERODIS_REDIS_STRING_BASIC_IMPL_H

#include "redis_string.h"


namespace merodis {

class RedisStringBasicImpl final : public RedisString {
public:
  RedisStringBasicImpl() noexcept;
  ~RedisStringBasicImpl() noexcept final;

  Status Open(const Options& options, const std::string& db_path) noexcept final;
  Status Get(const Slice& key, std::string* value) noexcept final;
  Status Set(const Slice& key, const Slice& value) noexcept final;
};

}


#endif //MERODIS_REDIS_STRING_BASIC_IMPL_H
