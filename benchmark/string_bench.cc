#include "benchmark/benchmark.h"

#include "db_fixture.h"
#include "merodis/merodis.h"
#include "util/random.h"

static const size_t SIZE = 1 << 16;

class StringFixture : public RedisFixture {
public:
  void PushAP(size_t count) const;
};

void StringFixture::PushAP(size_t count = SIZE) const {
  merodis::Status s;
  for (int c = 0; c < count; c++) {
    s = db->Set(std::to_string(c), std::to_string(c));
    assert(s.ok());
  }
  std::string v;
  s = db->Get(std::to_string(count - 1), &v);
  assert(s.ok());
  assert(v == std::to_string(count - 1));
}

BENCHMARK_DEFINE_F(StringFixture, FixedGetSmall)(benchmark::State& state) {
  std::string buf(state.range(), '0');
  std::string _s;
  db->Set(buf, buf);
  for (auto _ : state) {
    db->Get(buf, &_s);
  }
}

BENCHMARK_DEFINE_F(StringFixture, FixedSetSmall)(benchmark::State& state) {
  std::string buf(state.range(), '0');
  for (auto _ : state) {
    db->Set(buf, buf);
  }
}

BENCHMARK_DEFINE_F(StringFixture, FixedGetLarge)(benchmark::State& state) {
  std::string buf(state.range(), '0');
  std::string _s;
  db->Set(buf, buf);
  for (auto _ : state) {
    db->Get(buf, &_s);
  }
}

BENCHMARK_DEFINE_F(StringFixture, FixedSetLarge)(benchmark::State& state) {
  std::string buf(state.range(), '0');
  for (auto _ : state) {
    db->Set(buf, buf);
  }
}

//BENCHMARK_DEFINE_F(StringFixture, SequentialGet)(benchmark::State& state) {
//
//}
//
//BENCHMARK_DEFINE_F(StringFixture, SequentialSet)(benchmark::State& state) {
//
//}

BENCHMARK_DEFINE_F(StringFixture, RandomGet)(benchmark::State& state) {
  const size_t size = state.range();
  PushAP(size);
  std::vector<uint64_t> keys = rand_uint64(0, size - 1, 1 << 16);

  size_t c = 0;
  std::string v;
  for (auto _ : state) {
    assert(c < keys.size());
    std::string k = std::to_string(keys[c]);
    db->Get(k, &v);
    assert(k == v);
    c = (c + 1) & 0xffff;
  }
}

BENCHMARK_DEFINE_F(StringFixture, RandomSet)(benchmark::State& state) {
  const size_t size = state.range();
  PushAP(size);
  std::vector<uint64_t> keys = rand_uint64(0, size - 1, 1 << 16);
  std::vector<uint64_t> values = rand_uint64(0, size - 1, 1 << 16);

  size_t c = 0;
  std::string v;
  for (auto _ : state) {
    assert(c < keys.size());
    db->Set(std::to_string(keys[c]), std::to_string(values[c]));
    c = (c + 1) & 0xffff;
  }
}

BENCHMARK_DEFINE_F(StringFixture, Incr)(benchmark::State& state) {
  db->Set("k", "0");
  int64_t _r;
  for (auto _ : state) {
    db->Incr("k", &_r);
  }
}

//BENCHMARK_REGISTER_F(StringFixture, Get)->RangeMultiplier(2)->Range(1, 1 << 20);
//BENCHMARK_REGISTER_F(StringFixture, Set)->RangeMultiplier(2)->Range(1, 1 << 20);
BENCHMARK_REGISTER_F(StringFixture, FixedGetSmall)->DenseRange(0, 1 << 12, 1 << 6);
BENCHMARK_REGISTER_F(StringFixture, FixedSetSmall)->DenseRange(0, 1 << 12, 1 << 6);
BENCHMARK_REGISTER_F(StringFixture, FixedGetLarge)->DenseRange(1 << 16, 1 << 20, 1 << 16);
BENCHMARK_REGISTER_F(StringFixture, FixedSetLarge)->DenseRange(1 << 16, 1 << 20, 1 << 16);
BENCHMARK_REGISTER_F(StringFixture, RandomGet)->RangeMultiplier(2)->Range(1, 1 << 11)->DenseRange(1 << 12, 1 << 16, 1 << 12);
BENCHMARK_REGISTER_F(StringFixture, RandomSet)->RangeMultiplier(2)->Range(1, 1 << 11)->DenseRange(1 << 12, 1 << 16, 1 << 12);
BENCHMARK_REGISTER_F(StringFixture, Incr);
