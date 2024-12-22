#include "benchmark/benchmark.h"

#include "db_fixture.h"
#include "merodis/merodis.h"
#include "util/random.h"

static const size_t SIZE = 1 << 16;

class ZSetFixture : public RedisFixture {
public:
  void PushAP(size_t count) const;
};

void ZSetFixture::PushAP(size_t count = SIZE + 1) const {
  merodis::Status s;
  std::vector<std::string> members(count);
  std::generate(members.begin(), members.end(), [n = 0]() mutable { return std::to_string(n++); });

  std::map<merodis::Slice, int64_t> scoredMembers;
  for (int64_t c = 0; c < count; c++) {
    scoredMembers[members[c]] = c;
  }
  uint64_t c;
  s = db->ZAdd("s", scoredMembers, &c);
  assert(s.ok());
  assert(c == count);
}

BENCHMARK_DEFINE_F(ZSetFixture, ZRank)(benchmark::State& state) {
  int64_t member_score = state.range(0);
  std::string member = std::to_string(member_score);
  PushAP();

  merodis::Status s;
  for (auto _ : state) {
    uint64_t rank;
    s = db->ZRank("s", member, &rank);
    assert(s.ok());
    assert(rank == member_score);
  }
}

BENCHMARK_REGISTER_F(ZSetFixture, ZRank)->DenseRange(0, 1 << 16, 1 << 12);
