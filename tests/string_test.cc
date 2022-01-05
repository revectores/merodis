#include <iostream>

#include "gtest/gtest.h"
#include "merodis/merodis.h"


class StringTest : public testing::Test {
public:
  StringTest() {
    std::string path = "/tmp/test";
    s = db.Open(path);
  }

  merodis::Merodis db;
  merodis::Status s;
};

TEST_F(StringTest, StringRW) {
  s = db.Set("key", "value");
  ASSERT_EQ(s.ok(), true);
  std::string value;
  s = db.Get("key", &value);
  ASSERT_EQ(s.ok(), true);
  EXPECT_EQ(value, "value");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
