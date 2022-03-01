#include "gtest/gtest.h"
#include "merodis/merodis.h"
#include "common.h"
#include "testutil.h"

namespace merodis {
namespace test {

class SetTest : public RedisTest {
public:
  SetTest() : key_("key") {}
  uint64_t SCard(const Slice& key) {
    uint64_t len;
    EXPECT_MERODIS_OK(db.SCard(key, &len));
    return len;
  }
  bool SIsMember(const Slice& key, const Slice& setKey) {
    bool isMember;
    EXPECT_MERODIS_OK(db.SIsMember(key, setKey, &isMember));
    return isMember;
  }
  uint64_t SAdd(const Slice& key, const Slice& setKey) {
    uint64_t count;
    EXPECT_MERODIS_OK(db.SAdd(key, setKey, &count));
    return count;
  }

  uint64_t SCard() { return SCard(key_); }
  bool SIsMember(const Slice& setKey) { return SIsMember(key_, setKey); }
  uint64_t SAdd(const Slice& setKey) { return SAdd(key_, setKey); }

  virtual void TestSAdd();

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

void SetTest::TestSAdd() {
  ASSERT_EQ(SAdd("k0"), 1);
  ASSERT_EQ(SCard(), 1);
  ASSERT_EQ(SAdd("k1"), 1);
  ASSERT_EQ(SCard(), 2);
  ASSERT_EQ(SAdd("k0"), 0);
  ASSERT_EQ(SCard(), 2);
  ASSERT_EQ(SIsMember("k0"), true);
  ASSERT_EQ(SIsMember("k1"), true);
  ASSERT_EQ(SIsMember("k2"), false);
}

TEST_F(SetBasicImplTest, SAdd) {
  TestSAdd();
}

}
}
