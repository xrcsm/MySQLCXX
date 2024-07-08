#include <gtest/gtest.h>

#include <mysqlcxx.h>

TEST(MySQLCXXTest, ConnectionTest) {
  EXPECT_TRUE(mysqlcxx::connect("localhost", MYSQLCXX_TEST_USER, MYSQLCXX_TEST_PASSWORD,
                                MYSQLCXX_TEST_DATABASE_NAME, 3306));
}

TEST(MySQLCXXTest, QueryTest) {
  auto rows       = mysqlcxx::query("SELECT * FROM ?;", {"users"});
  auto last_error = mysqlcxx::get_last_error();
  EXPECT_FALSE(last_error.has_value());
  EXPECT_FALSE(rows.empty());
}

TEST(MySQLCXXTest, FailedQueryTest) {
  mysqlcxx::query("SELECT * FROM;", {""});
  auto last_error = mysqlcxx::get_last_error();
  GTEST_LOG_(INFO) << last_error->message;
  EXPECT_TRUE(last_error.has_value());
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
