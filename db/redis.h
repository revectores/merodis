#ifndef MERODIS_REDIS_H
#define MERODIS_REDIS_H

#include "merodis/merodis.h"

namespace merodis {

class Redis {
public:
  Redis() noexcept;
  virtual ~Redis() noexcept;

  virtual Status Open(const Options& options, const std::string& db_path) noexcept;

protected:
  DB* db_;
};

}


#endif //MERODIS_REDIS_H
