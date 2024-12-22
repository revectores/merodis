#include "benchmark/benchmark.h"

#include "db_fixture.h"
#include "merodis/merodis.h"
#include "util/random.h"

static const size_t SIZE = 1 << 16;

class SetFixture : public RedisFixture {
public:
  void PushAP(size_t count) const;
};

void SetFixture::PushAP(size_t count) const {
  merodis::Status s;
  std::vector<std::string> values(count);
  std::generate(values.begin(), values.end(), [n = 0]() mutable { return std::to_string(n++); });
  uint64_t c;
  s = db->SAdd("s", SliceSet(values.begin(), values.end()), &c);
  assert(s.ok());
  assert(c == count);
}

BENCHMARK_DEFINE_F(SetFixture, SAdd)(benchmark::State& state) {
  // const size_t size = state.range();
  merodis::Status s;

  size_t c = 0;
  uint64_t count;
  for (auto _ : state) {
    s = db->SAdd("s", std::to_string(c++), &count);
    assert(s.ok());
    assert(count == 1);
  }
}

BENCHMARK_DEFINE_F(SetFixture, SAddExisted)(benchmark::State& state) {
  merodis::Status s;
  PushAP(1 << 18);

  int c = 0;
  uint64_t count;
  for (auto _ : state) {
    s = db->SAdd("s", std::to_string(c++), &count);
    assert(s.ok());
    assert(count == 0);
  }
}

BENCHMARK_DEFINE_F(SetFixture, SRem)(benchmark::State& state) {
  merodis::Status s;
  PushAP(1 << 18);

  int c = 0;
  uint64_t count;
  for (auto _ : state) {
    s = db->SRem("s", std::to_string(c++), &count);
    assert(s.ok());
    assert(count == 1);
  }
}

BENCHMARK_DEFINE_F(SetFixture, SRemNotExisted)(benchmark::State& state) {
  merodis::Status s;
  uint64_t _c;
  db->SAdd("s", "s", &_c);
  assert(_c == 1);

  int c = 0;
  uint64_t count;
  for (auto _ : state) {
    s = db->SRem("s", std::to_string(c++), &count);
    assert(s.ok());
    assert(count == 0);
  }
}

BENCHMARK_DEFINE_F(SetFixture, SIsMember)(benchmark::State& state) {
  merodis::Status s;
  PushAP(1 << 16);

  uint16_t c = 0;
  bool is_member;
  for (auto _ : state) {
    s = db->SIsMember("s", std::to_string(c), &is_member);
    assert(s.ok());
    assert(is_member);
  }
}

BENCHMARK_DEFINE_F(SetFixture, SIsMemberNotExisted)(benchmark::State& state) {
  merodis::Status s;
  PushAP(1 << 16);

  int c = 1 << 16;
  bool is_member;
  for (auto _ : state) {
    s = db->SIsMember("s", std::to_string(c++), &is_member);
    assert(s.ok());
    assert(!is_member);
  }
}

BENCHMARK_REGISTER_F(SetFixture, SAdd);
BENCHMARK_REGISTER_F(SetFixture, SAddExisted);
BENCHMARK_REGISTER_F(SetFixture, SRem);
BENCHMARK_REGISTER_F(SetFixture, SRemNotExisted);
BENCHMARK_REGISTER_F(SetFixture, SIsMember);
BENCHMARK_REGISTER_F(SetFixture, SIsMemberNotExisted);
