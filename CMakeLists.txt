cmake_minimum_required (VERSION 3.19)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

project(mysqlcxx
	VERSION 1.0
)

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
	target_link_libraries(${PROJECT_NAME}
		${MYSQL_LIBRARY}
	)
endif()

# TODO: tests and install
