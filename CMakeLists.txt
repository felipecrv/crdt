cmake_minimum_required (VERSION 3.6)
project (foc_libraries)

# C99
set(CMAKE_C_STANDARD 99)

# C++17
set(CMAKE_CXX_STANDARD 17)

include(prelude.cmake)

add_executable(crdt crdt.cpp)

# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-gnu-statement-expression")

# include_directories("${PROJECT_BINARY_DIR}")
# include_directories("${PROJECT_SOURCE_DIR}")

# add_subdirectory(test)
