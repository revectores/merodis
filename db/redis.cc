#include "redis.h"


namespace merodis {

Redis::Redis() noexcept :
  db_(nullptr) {}

Redis::~Redis() noexcept {
  delete db_;
};

}