#include "utility/logging.h"

#include <gtest/gtest.h>


namespace minigraph {

// The fixture for testing class LogTest
class SampleTest : public ::testing::Test {
 protected:
  // You can do set-up work for each test in constructor.
  SampleTest() = default;

  // You can do clean-up work that doesn't throw exceptions here.
  ~SampleTest() override = default;

  void SetUp() override {
    // Code here will be called immediately after the constructor (right
    // before each test).
  }

  void TearDown() override {
    // Code here will be called immediately after each test (right
    // before the destructor).
  }
};

// Demonstrate some basic assertions.
TEST_F(SampleTest, BasicAssertions) {
  //LOG_FATAL("This is a fatal-level log.");
  EXPECT_DEATH(LOG_FATAL("This is a fatal-level log."),
               "This is a fatal-level log.");
  // Expect two strings not to be equal.
  EXPECT_STRNE("hello", "world");
  // Expect equality.
  EXPECT_EQ(7 * 6, 42);
}

} // namespace minigraph