#include "redis_set_basic_impl.h"

#include "leveldb/db.h"
#include "leveldb/write_batch.h"

namespace merodis {

RedisSetBasicImpl::RedisSetBasicImpl() noexcept = default;

RedisSetBasicImpl::~RedisSetBasicImpl() noexcept = default;

}