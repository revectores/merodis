#include <iostream>
#include <cstdio>

#include "benchmark/benchmark.h"
#include "merodis/merodis.h"

merodis::Merodis* db = nullptr;

static void DoSetup(const benchmark::State& state) {
  delete db;
  merodis::Status s = merodis::Merodis::DestroyDB("/tmp/test", merodis::Options());
  std::string path = "/tmp/test";
  merodis::Options options;
  options.create_if_missing = true;
  db = new merodis::Merodis;
  s = db->Open(options, path);
}

static void BM_GET(benchmark::State& state) {
  db->Set("key", "value");
  std::string value;
  for (auto _ : state) {
    db->Get("key", &value);
  }
}

static void BM_SET(benchmark::State& state) {
  for (auto _ : state) {
    db->Set("key", "value");
  }
}

BENCHMARK(BM_GET)->Setup(DoSetup);
BENCHMARK(BM_SET)->Setup(DoSetup);
BENCHMARK_MAIN();
