#ifndef MERODIS_COMMON_H
#define MERODIS_COMMON_H

#include "gtest/gtest.h"

#include "merodis/merodis.h"
#include "testutil.h"


class RedisTest : public testing::Test {
public:
  RedisTest() {
    merodis::Merodis::DestroyDB(db_path, merodis::Options());
    options.create_if_missing = true;
  }

  std::string db_path = "/tmp/test";
  merodis::Options options;
  merodis::Merodis db;
  merodis::Status s;
};



#endif //MERODIS_COMMON_H
