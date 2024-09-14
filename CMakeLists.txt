cmake_minimum_required (VERSION 3.19)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)


project(mysqlcxx
	VERSION 1.0
  LANGUAGES CXX
)

include(FetchContent)
FetchContent_Declare(
  fmt
  GIT_REPOSITORY https://github.com/fmtlib/fmt.git
  GIT_TAG        11.0.2
)
FetchContent_MakeAvailable(fmt)

add_library(${PROJECT_NAME}
	src/mysqlcxx.cpp
)

set_target_properties(${PROJECT_NAME} PROPERTIES
	CXX_STANDARD 20
	CXX_STANDARD_REQUIRED ON
)

target_include_directories(${PROJECT_NAME} PUBLIC
	include/
)

find_package(MySQL REQUIRED)

if (MYSQL_FOUND)
  target_link_libraries(${PROJECT_NAME} PRIVATE
		${MYSQL_LIBRARY}
    fmt::fmt
	)
endif()

option(CMAKE_BUILD_TESTS "Build tests?" ON)
if (CMAKE_BUILD_TESTS)
	set(MYSQLCXX_TEST_INCLUDE_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/include)
	enable_testing()
	add_subdirectory(tests)
endif()


# TODO: install project
