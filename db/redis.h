#ifndef MERODIS_REDIS_H
#define MERODIS_REDIS_H

#include "leveldb/db.h"

namespace merodis {
using Options = leveldb::Options;
using Status = leveldb::Status;
using Slice = leveldb::Slice;

class Redis {
public:
  Redis() noexcept;
  virtual ~Redis() noexcept;

  virtual Status Open(const Options& options, const std::string& db_path) noexcept = 0;

protected:
  leveldb::DB* db_;
};

}


#endif //MERODIS_REDIS_H
