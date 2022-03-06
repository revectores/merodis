#include <cstdint>

#include "gtest/gtest.h"

#include "util/coding.h"


TEST(CodingTest, EncodeDecode) {
  char* raw = new char[sizeof(uint64_t)];
  EncodeFixed64(raw, 0x1122334455667788);
  ASSERT_EQ(DecodeFixed64(raw), 0x1122334455667788);
}

TEST(CodingTest, PackedEncodeDecode) {
  char* raw = new char[3 * sizeof(uint64_t)];
  EncodeFixed64(raw, 0x1122334455667788);
  EncodeFixed64(raw + sizeof(uint64_t), 0x1020304050607080);
  EncodeFixed64(raw + 2 * sizeof(uint64_t), 0x0102030405060708);
  ASSERT_EQ(DecodeFixed64(raw), 0x1122334455667788);
  ASSERT_EQ(DecodeFixed64(raw + sizeof(uint64_t)), 0x1020304050607080);
  ASSERT_EQ(DecodeFixed64(raw + 2 * sizeof(uint64_t)), 0x0102030405060708);
}
