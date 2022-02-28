#include "gtest/gtest.h"
#include "merodis/merodis.h"
#include "common.h"
#include "testutil.h"


namespace merodis {
namespace test {

class StringTest : public RedisTest {
public:
  StringTest() : key_("key") {}

  std::string Get(const Slice& key) {
    std::string value;
    EXPECT_MERODIS_OK(db.Get(key, &value));
    return value;
  }
  void Set(const Slice& key, const Slice& value) {
    EXPECT_MERODIS_OK(db.Set(key, value));
  }
  int64_t Incr(const Slice& key) {
    int64_t result;
    EXPECT_MERODIS_OK(db.Incr(key, &result));
    return result;
  }
  int64_t IncrBy(const Slice& key, int64_t increment) {
    int64_t result;
    EXPECT_MERODIS_OK(db.IncrBy(key, increment, &result));
    return result;
  }
  int64_t Decr(const Slice& key) {
    int64_t result;
    EXPECT_MERODIS_OK(db.Decr(key, &result));
    return result;
  }
  int64_t DecrBy(const Slice& key, int64_t increment) {
    int64_t result;
    EXPECT_MERODIS_OK(db.DecrBy(key, increment, &result));
    return result;
  }

  std::string Get() { return Get(key_); }
  void Set(const Slice& value) { Set(key_, value); }
  int64_t Incr() { return Incr(key_); }
  int64_t IncrBy(int64_t increment) { return IncrBy(key_, increment); }
  int64_t Decr() { return Decr(key_); }
  int64_t DecrBy(int64_t decrement) { return DecrBy(key_, decrement); }
  void SetKey(const Slice& key) { key_ = key; }

  virtual void TestGetSet();
  virtual void TestIncrDecr();
private:
  Slice key_;
};

class StringBasicImplTest : public StringTest {
public:
  StringBasicImplTest() {
    options.string_impl = kStringBasicImpl;
    db.Open(options, db_path);
  }
};

class StringTypedImplTest : public StringTest {
public:
  StringTypedImplTest() {
    options.string_impl = kStringTypedImpl;
    db.Open(options, db_path);
  }
};

void StringTest::TestGetSet() {
  Set("value");
  ASSERT_EQ(Get(), "value");
  Set("123");
  ASSERT_EQ(Get(), "123");
  Set("0");
  ASSERT_EQ(Get(), "0");
  Set("-123");
  ASSERT_EQ(Get(), "-123");
  Set("9223372036854775807");
  ASSERT_EQ(Get(), "9223372036854775807");
  Set("9223372036854775808");
  ASSERT_EQ(Get(), "9223372036854775808");
  Set("-9223372036854775808");
  ASSERT_EQ(Get(), "-9223372036854775808");
  Set("-9223372036854775809");
  ASSERT_EQ(Get(), "-9223372036854775809");
  Set("");
  ASSERT_EQ(Get(), "");
  Set("1xx");
  ASSERT_EQ(Get(), "1xx");
  Set("1.2");
  ASSERT_EQ(Get(), "1.2");
}

void StringTest::TestIncrDecr() {
  int64_t n;
  Status s;
  Set("1");
  ASSERT_EQ(Get(), "1");
  ASSERT_EQ(Incr(), 2);
  ASSERT_EQ(Get(), "2");
  ASSERT_EQ(IncrBy(2), 4);
  ASSERT_EQ(Get(), "4");
  ASSERT_EQ(DecrBy(2), 2);
  ASSERT_EQ(Get(), "2");
  ASSERT_EQ(Decr(), 1);
  ASSERT_EQ(Get(), "1");
  Incr("nokey");
  ASSERT_EQ(Get("nokey"), "1");

  Set("1xx");
  s = db.Incr("key", &n);
  ASSERT_MERODIS_IS_INVALID_ARGUMENT(s);
  ASSERT_EQ(s.ToString(), "Invalid argument: Value is not an integer");
  ASSERT_EQ(Get(), "1xx");

  Set("9223372036854775808");
  s = db.Incr("key", &n);
  ASSERT_MERODIS_IS_INVALID_ARGUMENT(s);
  ASSERT_EQ(s.ToString(), "Invalid argument: Value is out of range");
  ASSERT_EQ(Get(), "9223372036854775808");

  Set("9223372036854775807");
  s = db.Incr("key", &n);
  ASSERT_MERODIS_IS_INVALID_ARGUMENT(s);
  ASSERT_EQ(s.ToString(), "Invalid argument: Result overflow");
  ASSERT_EQ(n, 9223372036854775807);
  ASSERT_EQ(Get(), "9223372036854775807");

  Set("-9223372036854775808");
  s = db.Decr("key", &n);
  ASSERT_MERODIS_IS_INVALID_ARGUMENT(s);
  ASSERT_EQ(s.ToString(), "Invalid argument: Result underflow");
  ASSERT_EQ(n, -9223372036854775808U);
  ASSERT_EQ(Get(), "-9223372036854775808");
}

TEST_F(StringBasicImplTest, GetSet) {
  TestGetSet();
}

TEST_F(StringBasicImplTest, IncrDecr) {
  TestIncrDecr();
}

TEST_F(StringTypedImplTest, GetSet) {
  TestGetSet();
}

TEST_F(StringTypedImplTest, IncrDecr) {
  TestIncrDecr();
}

}
}