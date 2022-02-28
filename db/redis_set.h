#ifndef MERODIS_REDIS_SET_H
#define MERODIS_REDIS_SET_H

#include "redis.h"
#include "util/coding.h"

namespace merodis {

class RedisSet : public Redis {
public:
  RedisSet() noexcept = default;
  ~RedisSet() noexcept override = default;
};

}

#endif //MERODIS_REDIS_SET_H
