#ifndef MERODIS_REDIS_STRING_H
#define MERODIS_REDIS_STRING_H

#include "redis.h"


namespace merodis {

class RedisString final : public Redis {
public:
  RedisString() noexcept;
  ~RedisString() noexcept final;

  Status Open(const std::string& db_path) noexcept final;
  Status Get(const Slice& key, std::string* value) noexcept;
  Status Set(const Slice& key, const Slice& value) noexcept;
};

}


#endif //MERODIS_REDIS_STRING_H
