#ifndef MERODIS_MERODIS_H
#define MERODIS_MERODIS_H

#include "leveldb/db.h"

namespace merodis {
using Options = leveldb::Options;
using Status = leveldb::Status;
using Slice = leveldb::Slice;

class RedisString;

class Merodis {
public:
  Merodis() noexcept;
  ~Merodis() noexcept;

  Status Open(const std::string& db_path) noexcept;
  Status Get(const Slice& key, std::string* value) noexcept;
  Status Set(const Slice& key, const Slice& value) noexcept;
private:
  RedisString* string_db_;
};

}

#endif //MERODIS_MERODIS_H
