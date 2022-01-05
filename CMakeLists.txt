cmake_minimum_required(VERSION 3.21)
project(merodis)

set(CMAKE_CXX_STANDARD 11)

add_subdirectory(third_party/leveldb)
include_directories(third_party/leveldb/include)
link_directories(third_party/leveldb)
add_executable(merodis db/main.cc)
target_link_libraries(merodis leveldb)
