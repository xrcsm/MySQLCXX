find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

set(MYSQLCXX_TEST_NAME mysqlcxx_test)

add_executable(${MYSQLCXX_TEST_NAME}
	mysqlcxx_test.cpp
)

target_include_directories(${MYSQLCXX_TEST_NAME} PRIVATE
	${MYSQLCXX_TEST_INCLUDE_DIRS}
)

target_link_libraries(${MYSQLCXX_TEST_NAME} PRIVATE mysqlcxx ${GTEST_LIBRARIES})

set(MYSQLCXX_TEST_USER		 			"default_user")
set(MYSQLCXX_TEST_PASSWORD			"default_password")
set(MYSQLCXX_TEST_DATABASE_NAME "default_database_name")

target_compile_definitions(${MYSQLCXX_TEST_NAME} PRIVATE
	MYSQLCXX_TEST_USER="${MYSQLCXX_TEST_USER}"
	MYSQLCXX_TEST_PASSWORD="${MYSQLCXX_TEST_PASSWORD}"
	MYSQLCXX_TEST_DATABASE_NAME="${MYSQLCXX_TEST_DATABASE_NAME}"
)

add_test(NAME MySQLCXX_Test COMMAND ${MYSQLCXX_TEST_NAME})
