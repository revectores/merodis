#include "benchmark/benchmark.h"

//static void LRemRandom() {
//  const int VALUES_CNT = 10000;
//  std::vector<std::string> values;
//  values.reserve(VALUES_CNT);
//  for (int i = 0; i < VALUES_CNT; i++) {
//    values.push_back("value" + std::to_string(rand_uint64()));
//  }
//
//  for (int i = 0; i < VALUES_CNT; i++) {
//    db->LPush("key", values[i]);
//  }
//  uint64_t removedCount;
//  for (int i = 0; i < VALUES_CNT; i++) {
//    db->LRem("key", 1, values[i], &removedCount);
//    // assert(removedCount == 1);
//  }
//}
//
//
//
//static void EncodeDecode() {
//  char* dst = new char[64];
//  for (int i = 0; i < 100000; i++) {
//    EncodeFixed64(dst, rand_uint64());
//    DecodeFixed64(dst);
//  }
//}
//
//static void ReversedEncodeDecode() {
//  char* dst = new char[64];
//  for (int i = 0; i < 100000; i++) {
//    ReversedEncodeFixed64(dst, rand_uint64());
//    ReversedDecodeFixed64(dst);
//  }
//}

BENCHMARK_MAIN();
