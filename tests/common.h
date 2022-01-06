#ifndef MERODIS_COMMON_H
#define MERODIS_COMMON_H

#include "gtest/gtest.h"
#include "merodis/merodis.h"


class RedisTest : public testing::Test {
public:
  RedisTest() {
    s = merodis::Merodis::DestroyDB("/tmp/test", merodis::Options());
    std::string path = "/tmp/test";
    merodis::Options options;
    options.create_if_missing = true;
    s = db.Open(options, path);
  }

  merodis::Merodis db;
  merodis::Status s;
};



#endif //MERODIS_COMMON_H
