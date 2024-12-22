#include "db_fixture.h"

#include "benchmark/benchmark.h"
#include "merodis/merodis.h"


void RedisFixture::SetUp(benchmark::State& state) {
  merodis::Status s = merodis::Merodis::DestroyDB("/tmp/test", merodis::Options());
  assert(s.ok());
  std::string path = "/tmp/test";
  merodis::Options options;
  options.create_if_missing = true;
  db = new merodis::Merodis;
  s = db->Open(options, path);
  assert(s.ok());
}

void RedisFixture::TearDown(benchmark::State& state) {
  merodis::Status s = merodis::Merodis::DestroyDB("/tmp/test", merodis::Options());
  delete db;
}
