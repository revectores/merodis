#include <vector>
#include "gtest/gtest.h"
#include "merodis/merodis.h"
#include "common.h"
#include "testutil.h"

namespace merodis {
namespace test {

class ZSetTest: public RedisTest {
public:
  ZSetTest(): key_("key") {}

private:
  Slice key_;
};

class ZSetBasicImplTest: public ZSetTest {
public:
  ZSetBasicImplTest() {
    options.zset_impl = kZSetBasicImpl;
    db.Open(options, db_path);
  }
};

}
}