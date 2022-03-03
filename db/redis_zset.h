#ifndef MERODIS_REDIS_ZSET_H
#define MERODIS_REDIS_ZSET_H

#include "redis.h"
#include "util/coding.h"

namespace merodis {

class RedisZSet : public Redis {
public:
  RedisZSet() noexcept = default;
  ~RedisZSet() noexcept override = default;
};

}


#endif //MERODIS_REDIS_ZSET_H
