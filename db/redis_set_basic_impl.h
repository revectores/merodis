#ifndef MERODIS_REDIS_SET_BASIC_IMPL_H
#define MERODIS_REDIS_SET_BASIC_IMPL_H

#include "redis_set.h"
#include "util/coding.h"

namespace merodis {

class RedisSetBasicImpl final : public RedisSet {
public:
  RedisSetBasicImpl() noexcept;
  ~RedisSetBasicImpl() noexcept final;
};

}

#endif //MERODIS_REDIS_SET_BASIC_IMPL_H
