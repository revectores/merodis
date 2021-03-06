#include <cstdint>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "merodis/merodis.h"
#include "common.h"
#include "testutil.h"

namespace merodis {
namespace test {

class ListTest : public RedisTest {
public:
  ListTest() : key_("key") {}

  uint64_t LLen(const Slice& key) {
    uint64_t len;
    db.LLen(key, &len);
    return len;
  }
  std::string LIndex(const Slice& key, UserIndex index) {
    std::string value;
    EXPECT_MERODIS_OK(db.LIndex(key, index, &value));
    return value;
  }
  std::vector<uint64_t> LPos(const Slice& key, const Slice& value, int64_t rank, int64_t count, int64_t maxlen) {
    std::vector<uint64_t> indices;
    EXPECT_MERODIS_OK(db.LPos(key, value, rank, count, maxlen, &indices));
    return indices;
  }
  std::vector<std::string> LRange(const Slice& key, UserIndex from, UserIndex to) {
    std::vector<std::string> values;
    EXPECT_MERODIS_OK(db.LRange(key, from, to, &values));
    return values;
  }
  std::vector<std::string> List(const Slice& key) {
    return LRange(key, 0, -1);
  }
  void LSet(const Slice& key, UserIndex index, const Slice& value) {
    EXPECT_MERODIS_OK(db.LSet(key, index, value));
  }
  void LPush(const Slice& key, const Slice& value) {
    EXPECT_MERODIS_OK(db.LPush(key, value));
  }
  void LPush(const Slice& key, const std::vector<Slice>& values) {
    EXPECT_MERODIS_OK(db.LPush(key, values));
  }
  void RPush(const Slice& key, const Slice& value) {
    EXPECT_MERODIS_OK(db.RPush(key, value));
  }
  void RPush(const Slice& key, const std::vector<Slice>& values) {
    EXPECT_MERODIS_OK(db.RPush(key, values));
  }
  void LInsert(const Slice& key, const BeforeOrAfter& beforeOrAfter, const Slice& pivotValue, const Slice& value) {
    EXPECT_MERODIS_OK(db.LInsert(key, beforeOrAfter, pivotValue, value));
  }
  std::string LPop(const Slice& key) {
    std::string value;
    EXPECT_MERODIS_OK(db.LPop(key, &value));
    return value;
  }
  std::vector<std::string> LPop(const Slice& key, uint64_t count) {
    std::vector<std::string> values;
    EXPECT_MERODIS_OK(db.LPop(key, count, &values));
    return values;
  }
  std::string RPop(const Slice& key) {
    std::string value;
    EXPECT_MERODIS_OK(db.RPop(key, &value));
    return value;
  }
  std::vector<std::string> RPop(const Slice& key, uint64_t count) {
    std::vector<std::string> values;
    EXPECT_MERODIS_OK(db.RPop(key, count, &values));
    return values;
  }
  void LTrim(const Slice& key, UserIndex from, UserIndex to) {
    EXPECT_MERODIS_OK(db.LTrim(key, from, to));
  }
  uint64_t LRem(const Slice& key, int64_t count, const Slice& value) {
    uint64_t removedCount;
    EXPECT_MERODIS_OK(db.LRem(key, count, value, &removedCount));
    return removedCount;
  }
  std::string LMove(const Slice& srcKey, const Slice& dstKey, enum Side srcSide, enum Side dstSide) {
    std::string value;
    EXPECT_MERODIS_OK(db.LMove(srcKey, dstKey, srcSide, dstSide, &value));
    return value;
  }
  uint64_t LLen() { return LLen(key_); }
  std::string LIndex(UserIndex index) { return LIndex(key_, index); }
  std::vector<uint64_t> LPos(const Slice& value, int64_t rank, int64_t count, int64_t maxlen) {
    return LPos(key_, value, rank, count, maxlen);
  }
  std::vector<std::string> LRange(UserIndex from, UserIndex to) { return LRange(key_, from, to); }
  std::vector<std::string> List() { return List(key_); }
  void LSet(UserIndex index, const Slice& value) { LSet(key_, index, value); }
  void LPush(const Slice& value) { LPush(key_, value); }
  void LPush(const std::vector<Slice>& values) { LPush(key_, values); }
  void RPush(const Slice& value) { RPush(key_, value); }
  void RPush(const std::vector<Slice>& values) { RPush(key_, values); }
  std::string LPop() { return LPop(key_); }
  std::vector<std::string> LPop(uint64_t count) { return LPop(key_, count); }
  std::string RPop() { return RPop(key_); }
  std::vector<std::string> RPop(uint64_t count) { return RPop(key_, count); }
  void LTrim(UserIndex from, UserIndex to) { return LTrim(key_, from, to); }
  void LInsert(const BeforeOrAfter& beforeOrAfter, const Slice& pivotValue, const Slice& value) {
    LInsert(key_, beforeOrAfter, pivotValue, value);
  };
  uint64_t LRem(int64_t count, const Slice& value) { return LRem(key_, count, value); }
  void SetKey(const Slice& key) { key_ = key; }

  virtual void TestLIndex();
  virtual void TestLPos();
  virtual void TestLRange();
  virtual void TestLSet();
  virtual void TestPush();
  virtual void TestLPopSingle();
  virtual void TestRPopSingle();
  virtual void TestRPopMultiple();
  virtual void TestLTrim();
  virtual void TestLInsert();
  virtual void TestLRem();
  virtual void TestLMove();

private:
  Slice key_;
};

class ListArrayImplTest : public ListTest {
public:
  ListArrayImplTest() {
    options.list_impl = kListArrayImpl;
    db.Open(options, db_path);
  }
};

void ListTest::TestLIndex() {
  std::string value;
  ASSERT_EQ(LLen(), 0);

  // [] => ["1"]
  ASSERT_MERODIS_OK(db.LPush("key", "1"));
  ASSERT_EQ(LLen(), 1);

  // ["1"] => ["0", "1"]
  ASSERT_MERODIS_OK(db.LPush("key", "0"));
  ASSERT_EQ(LLen(), 2);
  ASSERT_EQ(LIndex(0), "0");
  ASSERT_EQ(LIndex(1), "1");
  ASSERT_EQ(LIndex(-1), "1");
  ASSERT_EQ(LIndex(-2), "0");
  ASSERT_MERODIS_IS_INVALID_ARGUMENT(db.LIndex("key", 2, &value));
  ASSERT_MERODIS_IS_INVALID_ARGUMENT(db.LIndex("key", -3, &value));
}

void ListTest::TestLPos() {
  RPush({"0", "1", "0", "0", "1", "0"});
  ASSERT_EQ(List(), LIST("0", "1", "0", "0", "1", "0"));

  ASSERT_EQ(LPos("0", 0, 0, 0), UINTS(0, 2, 3, 5));
  ASSERT_EQ(LPos("0", 0, 1, 0), UINTS(0));
  ASSERT_EQ(LPos("0", 0, 2, 0), UINTS(0, 2));
  ASSERT_EQ(LPos("0", 0, 2, 1), UINTS(0));
  ASSERT_EQ(LPos("0", 0, 2, 2), UINTS(0));

  ASSERT_EQ(LPos("0", 1, 0, 0), UINTS(2, 3, 5));
  ASSERT_EQ(LPos("0", 1, 1, 0), UINTS(2));
  ASSERT_EQ(LPos("0", 1, 2, 0), UINTS(2, 3));
  ASSERT_EQ(LPos("0", 1, 2, 2), UINTS());
  ASSERT_EQ(LPos("0", 1, 2, 3), UINTS(2));

  ASSERT_EQ(LPos("0", -1, 0, 0), UINTS(5, 3, 2, 0));
  ASSERT_EQ(LPos("0", -1, 1, 0), UINTS(5));
  ASSERT_EQ(LPos("0", -1, 2, 0), UINTS(5, 3));
  ASSERT_EQ(LPos("0", -1, 2, 1), UINTS(5));
  ASSERT_EQ(LPos("0", -1, 2, 2), UINTS(5));

  ASSERT_EQ(LPos("0", -2, 0, 0), UINTS(3, 2, 0));
  ASSERT_EQ(LPos("0", -2, 1, 0), UINTS(3));
  ASSERT_EQ(LPos("0", -2, 2, 0), UINTS(3, 2));
  ASSERT_EQ(LPos("0", -2, 2, 2), UINTS());
  ASSERT_EQ(LPos("0", -2, 2, 3), UINTS(3));
}

void ListTest::TestLRange() {
  RPush({"0", "1", "2"});
  ASSERT_EQ(LRange(0, 0), LIST("0"));
  ASSERT_EQ(LRange(-3, 1), LIST("0", "1"));
  ASSERT_EQ(LRange(-3, -1), LIST("0", "1", "2"));
  ASSERT_EQ(LRange(-100, 100), LIST("0", "1", "2"));
  ASSERT_EQ(LRange(5, 10), LIST());
  ASSERT_EQ(LRange(-3, -6), LIST());
}

void ListTest::TestLSet() {
  RPush({"0", "1", "2"});
  ASSERT_EQ(List(), LIST("0", "1", "2"));
  LSet(0, "zero");
  ASSERT_EQ(LIndex(0), "zero");
  LSet(1, "one");
  ASSERT_EQ(LIndex(1), "one");
  LSet(-1, "two");
  ASSERT_EQ(LIndex(-1), "two");
  LSet(-2, "another one");
  ASSERT_EQ(LIndex(-2), "another one");
  ASSERT_EQ(List(), LIST("zero", "another one", "two"));
  ASSERT_MERODIS_IS_INVALID_ARGUMENT(db.LSet("key", 3, "out of range"));
  ASSERT_MERODIS_IS_INVALID_ARGUMENT(db.LSet("key", -4, "out of range"));
}

void ListTest::TestPush() {
  LPush({"2", "1"});
  ASSERT_EQ(List(), LIST("1", "2"));
  LPush("0");
  ASSERT_EQ(List(), LIST("0", "1", "2"));

  RPush("3");
  ASSERT_EQ(List(), LIST("0", "1", "2", "3"));
  RPush({"4", "5"});
  ASSERT_EQ(List(), LIST("0", "1", "2", "3", "4", "5"));

  ASSERT_MERODIS_IS_NOT_FOUND(db.LPushX("no-such-key", "0"));
  ASSERT_MERODIS_IS_NOT_FOUND(db.LPushX("no-such-key", {"0", "1"}));
  ASSERT_MERODIS_IS_NOT_FOUND(db.RPushX("no-such-key", "0"));
  ASSERT_MERODIS_IS_NOT_FOUND(db.RPushX("no-such-key", {"0", "1"}));
}

void ListTest::TestLPopSingle() {
  RPush("key", {"0", "1"});
  ASSERT_EQ(List(), LIST("0", "1"));

  // ["0", "1"] => ["1"]
  ASSERT_EQ(LPop(), "0");
  ASSERT_EQ(List(), LIST("1"));

  // ["1"] => []
  ASSERT_EQ(LPop(), "1");
  ASSERT_EQ(List(), LIST());

  // [] => []
  ASSERT_EQ(LPop(), "");
  ASSERT_EQ(List(), LIST());
}

void ListTest::TestRPopSingle() {
  RPush("key", {"0", "1"});
  ASSERT_EQ(List(), LIST("0", "1"));

  // ["0", "1"] => ["0"]
  ASSERT_EQ(RPop(), "1");
  ASSERT_EQ(List(), LIST("0"));

  // ["0"] => []
  ASSERT_EQ(RPop(), "0");
  ASSERT_EQ(List(), LIST());

  // [] => []
  ASSERT_EQ(RPop(), "");
  ASSERT_EQ(List(), LIST());
}

void ListTest::TestRPopMultiple() {
  RPush("key", {"0", "1", "2"});
  ASSERT_EQ(List(), LIST("0", "1", "2"));

  // ["0", "1", "2"] => ["1", "2"]
  ASSERT_EQ(RPop(1), LIST("2"));
  ASSERT_EQ(List(), LIST("0", "1"));

  // ["1", "2"] => []
  ASSERT_EQ(RPop(2), LIST("0", "1"));
  ASSERT_EQ(List(), LIST());

  // [] => []
  ASSERT_EQ(RPop(3), LIST());
  ASSERT_EQ(List(), LIST());

  // Make sure RPUSH can be successfully done after RPOP.
  // [] => ["value-0", "value-1"]
  ASSERT_MERODIS_OK(db.RPush("key", "value-0"));
  ASSERT_MERODIS_OK(db.RPush("key", "value-1"));
  ASSERT_EQ(List(), LIST("value-0", "value-1"));
}

void ListTest::TestLTrim() {
  RPush({"0", "1", "2", "3", "4", "5", "6", "7"});
  ASSERT_EQ(List(), LIST("0", "1", "2", "3", "4", "5", "6", "7"));
  LTrim(1, 6);
  ASSERT_EQ(List(), LIST("1", "2", "3", "4", "5", "6"));
  LTrim(1, 5);
  ASSERT_EQ(List(), LIST("2", "3", "4", "5", "6"));
  LTrim(0, 3);
  ASSERT_EQ(List(), LIST("2", "3", "4", "5"));
  LTrim(0, 1024);
  ASSERT_EQ(List(), LIST("2", "3", "4", "5"));
  LTrim(2, 2);
  ASSERT_EQ(List(), LIST("4"));
  LTrim(0, 0);
  ASSERT_EQ(List(), LIST("4"));
  LTrim(1, 0);
  ASSERT_EQ(List(), LIST());

  RPush({"0", "1", "2", "3", "4", "5", "6", "7"});
  ASSERT_EQ(List(), LIST("0", "1", "2", "3", "4", "5", "6", "7"));
  LTrim(-7, -2);
  ASSERT_EQ(List(), LIST("1", "2", "3", "4", "5", "6"));
  LTrim(-5, -1);
  ASSERT_EQ(List(), LIST("2", "3", "4", "5", "6"));
  LTrim(-5, -2);
  ASSERT_EQ(List(), LIST("2", "3", "4", "5"));
  LTrim(-1025, -1);
  ASSERT_EQ(List(), LIST("2", "3", "4", "5"));
  LTrim(-2, -2);
  ASSERT_EQ(List(), LIST("4"));
  LTrim(-1, -1);
  ASSERT_EQ(List(), LIST("4"));
  LTrim(-3, -2);
  ASSERT_EQ(List(), LIST());

  RPush({"0", "1", "2", "3", "4", "5", "6", "7"});
  ASSERT_EQ(List(), LIST("0", "1", "2", "3", "4", "5", "6", "7"));
  LTrim(1, -2);
  ASSERT_EQ(List(), LIST("1", "2", "3", "4", "5", "6"));
  LTrim(-5, 5);
  ASSERT_EQ(List(), LIST("2", "3", "4", "5", "6"));
  LTrim(0, -2);
  ASSERT_EQ(List(), LIST("2", "3", "4", "5"));
  LTrim(-1025, 1024);
  ASSERT_EQ(List(), LIST("2", "3", "4", "5"));
  LTrim(2, -2);
  ASSERT_EQ(List(), LIST("4"));
  LTrim(-1, 0);
  ASSERT_EQ(List(), LIST("4"));
  LTrim(1, -1);
  ASSERT_EQ(List(), LIST());
}

void ListTest::TestLInsert() {
  RPush("key", {"0", "1", "2"});
  ASSERT_EQ(List(), LIST("0", "1", "2"));

  LInsert(kBefore, "1", "0.5");
  ASSERT_EQ(List(), LIST("0", "0.5", "1", "2"));
  LInsert(kBefore, "0", "-1");
  ASSERT_EQ(List(), LIST("-1", "0", "0.5", "1", "2"));
  LInsert(kAfter, "1", "1.5");
  ASSERT_EQ(List(), LIST("-1", "0", "0.5", "1", "1.5", "2"));
  LInsert(kAfter, "2", "3");
  ASSERT_EQ(List(), LIST("-1", "0", "0.5", "1", "1.5", "2", "3"));
  LPush("-2");
  ASSERT_EQ(List(), LIST("-2", "-1", "0", "0.5", "1", "1.5", "2", "3"));
}

void ListTest::TestLRem() {
  RPush({"1", "2", "1", "2", "1", "1", "1"});
  ASSERT_EQ(List(), LIST("1", "2", "1", "2", "1", "1", "1"));

  ASSERT_EQ(LRem(1, "1"), 1);
  ASSERT_EQ(List(), LIST("2", "1", "2", "1", "1", "1"));

  ASSERT_EQ(LRem(2, "1"), 2);
  ASSERT_EQ(List(), LIST("2", "2", "1", "1"));

  ASSERT_EQ(LRem(3, "1"), 2);
  ASSERT_EQ(List(), LIST("2", "2"));

  RPush({"1", "1", "1"});
  LPush({"1", "1", "1"});
  ASSERT_EQ(List(), LIST("1", "1", "1", "2", "2", "1", "1", "1"));

  ASSERT_EQ(LRem(-2, "1"), 2);
  ASSERT_EQ(List(), LIST("1", "1", "1", "2", "2", "1"));

  ASSERT_EQ(LRem(-2, "1"), 2);
  ASSERT_EQ(List(), LIST("1", "1", "2", "2"));

  ASSERT_EQ(LRem(0, "2"), 2);
  ASSERT_EQ(List(), LIST("1", "1"));

  ASSERT_EQ(LRem(0, "1"), 2);
  ASSERT_EQ(List(), LIST());
}

void ListTest::TestLMove() {
  RPush("k1", {"0", "1", "2"});
  RPush("k2", {"a", "b", "c"});

  ASSERT_EQ(LMove("k1", "k2", kLeft, kLeft), "0");
  ASSERT_EQ(List("k1"), LIST("1", "2"));
  ASSERT_EQ(List("k2"), LIST("0", "a", "b", "c"));

  ASSERT_EQ(LMove("k2", "k1", kRight, kRight), "c");
  ASSERT_EQ(List("k1"), LIST("1", "2", "c"));
  ASSERT_EQ(List("k2"), LIST("0", "a", "b"));

  ASSERT_EQ(LMove("k1", "k2", kLeft, kRight), "1");
  ASSERT_EQ(List("k1"), LIST("2", "c"));
  ASSERT_EQ(List("k2"), LIST("0", "a", "b", "1"));

  ASSERT_EQ(LMove("k2", "k1", kRight, kLeft), "1");
  ASSERT_EQ(List("k1"), LIST("1", "2", "c"));
  ASSERT_EQ(List("k2"), LIST("0", "a", "b"));

  ASSERT_EQ(LMove("k1", "k1", kLeft, kRight), "1");
  ASSERT_EQ(List("k1"), LIST("2", "c", "1"));

  ASSERT_EQ(LMove("k2", "k2", kRight, kLeft), "b");
  ASSERT_EQ(List("k2"), LIST("b", "0", "a"));

  ASSERT_EQ(LMove("k1", "k1", kLeft, kLeft), "");
  ASSERT_EQ(List("k1"), LIST("2", "c", "1"));

  ASSERT_EQ(LMove("k2", "k2", kRight, kRight), "");
  ASSERT_EQ(List("k2"), LIST("b", "0", "a"));
}

TEST_F(ListArrayImplTest, LIndex) {
  TestLIndex();
}

TEST_F(ListArrayImplTest, LPos) {
  TestLPos();
}

TEST_F(ListArrayImplTest, LRange) {
  TestLRange();
}

TEST_F(ListArrayImplTest, LSet) {
  TestLSet();
}

TEST_F(ListArrayImplTest, Push) {
  TestPush();
}

TEST_F(ListArrayImplTest, LPopSingle) {
  TestLPopSingle();
}

TEST_F(ListArrayImplTest, RPopSingle) {
  TestRPopSingle();
}

TEST_F(ListArrayImplTest, RPopMultiple) {
  TestRPopMultiple();
}

TEST_F(ListArrayImplTest, LTrim) {
  TestLTrim();
}

TEST_F(ListArrayImplTest, LInsert) {
  TestLInsert();
}

TEST_F(ListArrayImplTest, LRem) {
  TestLRem();
}

TEST_F(ListArrayImplTest, LMove) {
  TestLMove();
}

}
}