cmake_minimum_required(VERSION 3.21)
project(merodis)

set(CMAKE_CXX_STANDARD 17)

SET(LEVELDB_BUILD_TESTS OFF CACHE BOOL "Build LevelDB's unit tests" FORCE)
SET(LEVELDB_BUILD_BENCHMARKS OFF CACHE BOOL "Build LevelDB's benchmarks" FORCE)
option(MERODIS_BUILD_TESTS "Build Merodis' unit tests" ON)
option(MERODIS_BUILD_BENCHMARKS "Build Merodis' benchmarks" ON)

add_subdirectory(third_party/leveldb)

include_directories(
  third_party/leveldb/include
  include
  .
)
link_directories(third_party/leveldb)

add_library(merodis)
target_sources(merodis PRIVATE
  db/redis.cc
  db/redis.h
  db/merodis.cc
  include/merodis/merodis.h
  db/redis_string.h
  db/redis_string_basic_impl.cc
  db/redis_string_basic_impl.h
  db/redis_string_typed_impl.cc
  db/redis_string_typed_impl.h
  db/redis_list.h
  db/redis_list_array_impl.cc
  db/redis_list_array_impl.h
  db/redis_hash.h
  db/redis_hash_basic_impl.cc
  db/redis_hash_basic_impl.h
  db/redis_set.h
  db/redis_set_basic_impl.cc
  db/redis_set_basic_impl.h
  db/redis_zset.h
  db/redis_zset_basic_impl.cc
  db/redis_zset_basic_impl.h
  db/layout.cc
  db/layout.h
  util/coding.h
  util/number.h
  util/random.h
  util/sequence.cc
  util/sequence.h
  util/variant_helper.h
)
target_link_libraries(merodis leveldb)


if(MERODIS_BUILD_TESTS)
  add_subdirectory(third_party/googletest)
  add_executable(test_merodis)
  target_sources(test_merodis PRIVATE
    tests/testutil.h
    tests/common.h
    tests/test.cc
    tests/util_test.cc
    tests/coding_test.cc
    tests/string_test.cc
    tests/list_test.cc
    tests/hash_test.cc
    tests/set_test.cc
    tests/zset_test.cc
  )
  target_link_directories(test_merodis PRIVATE third_party/googletest)
  target_link_libraries(test_merodis merodis gmock gtest)
endif(MERODIS_BUILD_TESTS)

if(MERODIS_BUILD_BENCHMARKS)
  add_subdirectory(third_party/benchmark)
  add_executable(merodis_benchmark)
  target_sources(merodis_benchmark PRIVATE
    benchmark/bench.cc
  )
  target_link_directories(merodis_benchmark PRIVATE third_party/benchmark)
  target_link_libraries(merodis_benchmark merodis benchmark)
endif(MERODIS_BUILD_BENCHMARKS)
