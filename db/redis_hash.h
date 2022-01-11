#ifndef MERODIS_REDIS_HASH_H
#define MERODIS_REDIS_HASH_H

#include "redis.h"

namespace merodis {

class RedisHash final : public Redis {
public:
  RedisHash() noexcept;
  ~RedisHash() noexcept final;

  Status Open(const Options& options, const std::string& db_path) noexcept final;

};

}

#endif //MERODIS_REDIS_HASH_H
