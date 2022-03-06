#include "redis.h"

#include "merodis/merodis.h"


namespace merodis {

Redis::Redis() noexcept :
  db_(nullptr) {}

Redis::~Redis() noexcept {
  delete db_;
};

Status Redis::Open(const Options& options, const std::string& db_path) noexcept {
  return DB::Open(options, db_path, &db_);
}

}