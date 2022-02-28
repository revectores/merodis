#include "gtest/gtest.h"
#include "merodis/merodis.h"
#include "common.h"
#include "testutil.h"

namespace merodis {
namespace test {

class SetTest : public RedisTest {
public:
  SetTest() : key_("key") {}

private:
  Slice key_;
};

class SetBasicImplTest: public SetTest {
public:
  SetBasicImplTest() {
    options.set_impl = kSetBasicImpl;
    db.Open(options, db_path);
  }
};

}
}
