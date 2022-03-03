#ifndef MERODIS_REDIS_ZSET_BASIC_IMPL_H
#define MERODIS_REDIS_ZSET_BASIC_IMPL_H

#include "redis_zset.h"
#include "util/coding.h"

namespace merodis {

class RedisZSetBasicImpl final : public RedisZSet {
public:
  RedisZSetBasicImpl() noexcept;
  ~RedisZSetBasicImpl() noexcept final;
};

}

#endif //MERODIS_REDIS_ZSET_BASIC_IMPL_H
