#include <numeric>
#include <string>
#include <vector>
#include <map>

#include "benchmark/benchmark.h"

#include "db_fixture.h"
#include "merodis/merodis.h"

static const size_t SIZE = 1 << 16;

class HashFixture : public RedisFixture {
public:
  void SetNodes(size_t count, const std::string& value, const std::string& key) const;
};

void HashFixture::SetNodes(size_t count = SIZE, const std::string& value = "v", const std::string& key = "k") const {
  merodis::Status s;
  std::map<merodis::Slice, merodis::Slice> nodes;
  std::vector<std::string> keys(count);
  std::iota(keys.begin(), keys.end(), 0);
  for (const auto& k: keys) {
    nodes[k] = value;
  }

  uint64_t c;
  s = db->HSet(key, nodes, &c);
  assert(s.ok());
  assert(c == count);
}

BENCHMARK_DEFINE_F(HashFixture, HLen)(benchmark::State& state) {
  const size_t nodeCount = state.range();
  SetNodes(nodeCount);

  merodis::Status s;
  uint64_t l;
  for (auto _ : state) {
    s = db->HLen("k", &l);
    assert(s.ok());
    assert(l == state.range());
  }
}

BENCHMARK_DEFINE_F(HashFixture, HGet)(benchmark::State& state) {
  const int64_t keyNumber = state.range() - 1;
  std::string key = std::to_string(keyNumber);
  SetNodes();

  merodis::Status s;
  std::string v;
  for (auto _ : state) {
    s = db->HGet("k", key, &v);
    assert(s.ok());
    assert(v == "v");
  }
}

BENCHMARK_DEFINE_F(HashFixture, HMGet)(benchmark::State& state) {

}

BENCHMARK_DEFINE_F(HashFixture, HGetAll)(benchmark::State& state) {

}

BENCHMARK_DEFINE_F(HashFixture, HKeys)(benchmark::State& state) {

}

BENCHMARK_DEFINE_F(HashFixture, HVals)(benchmark::State& state) {

}

BENCHMARK_DEFINE_F(HashFixture, HExists)(benchmark::State& state) {

}

BENCHMARK_DEFINE_F(HashFixture, HSet1)(benchmark::State& state) {

}

BENCHMARK_DEFINE_F(HashFixture, HSetN)(benchmark::State& state) {

}

BENCHMARK_DEFINE_F(HashFixture, HDel)(benchmark::State& state) {

}

BENCHMARK_DEFINE_F(HashFixture, HDelN)(benchmark::State& state) {

}

BENCHMARK_REGISTER_F(HashFixture, HLen)->RangeMultiplier(16)->Range(1, 1 << 16);
// BENCHMARK_REGISTER_F(HashFixture, HGet);
