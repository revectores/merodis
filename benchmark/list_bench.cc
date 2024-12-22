#include <algorithm>
#include <string>
#include <vector>

#include "benchmark/benchmark.h"

#include "db_fixture.h"
#include "merodis/merodis.h"

static const size_t SIZE = 1 << 16;


class ListFixture : public RedisFixture {
public:
  void PushNodes(size_t count, const std::string& value, const std::string& key) const;
  void PushAP(size_t count, int64_t first, int64_t step) const;
  void PushFragments(size_t count, size_t fragmentSize) const;
};

void ListFixture::PushNodes(size_t count = SIZE, const std::string& value = "v", const std::string& key = "k") const {
  merodis::Status s;
  std::vector<merodis::Slice> nodes(count, value);
  s = db->RPush(key, nodes);
  assert(s.ok());

  uint64_t l;
  s = db->LLen(key, &l);
  assert(s.ok());
  assert(l == count);
}

void ListFixture::PushAP(size_t count = SIZE, int64_t first = 0, int64_t step = 1) const {
  merodis::Status s;
  std::vector<std::string> values(count);
  std::generate(values.begin(), values.end(), [n = first - step, step]() mutable { return std::to_string(n += step); });
  s = db->RPush("k", Slices(values.begin(), values.end()));
  assert(s.ok());

  uint64_t l;
  s = db->LLen("k", &l);
  assert(s.ok());
  assert(l == count);

  std::string v;
  s = db->LIndex("k", 0, &v);
  assert(s.ok());
  assert(v == std::to_string(0));
  s = db->LIndex("k", (int64_t)count - 1, &v);
  assert(s.ok());
  assert(v == std::to_string(first + step * (count - 1)));
}

void ListFixture::PushFragments(size_t count = SIZE, size_t fragmentSize = SIZE >> 2) const {
  merodis::Status s;
  assert(count % fragmentSize == 0);
  size_t fragmentCount = count / fragmentSize;

  std::vector<merodis::Slice> nodes(count, "0");
  for (int c = 1; c <= fragmentCount; ++c) {
    nodes[c * fragmentSize - 1] = "1";
  }
  uint64_t count1 = std::count(nodes.begin(), nodes.end(), "1");
  assert(count1 == fragmentCount);
  s = db->RPush("k", nodes);
  assert(s.ok());
}

template<uint8_t k>
static void IndexGenerator(benchmark::internal::Benchmark* b) {
  const int64_t step = SIZE >> k;
  b->Args({(int64_t)0});
  for (int64_t index = step - 1; index < SIZE; index += step) {
    b->Args({index});
  }
}

template<uint8_t k>
static void IndexPairGenerator(benchmark::internal::Benchmark* b) {
  const int64_t step = SIZE >> k;
  for (int64_t p1 = 0; p1 < SIZE; p1 += step) {
    for (int64_t p2 = p1; p2 < SIZE; p2 += step) {
      b->Args({p1, p2});
    }
  }
}

template<uint8_t k>
static void RangeGenerator(benchmark::internal::Benchmark* b) {
  const int64_t step = SIZE >> k;
  for (int64_t from = 0; from <= SIZE; from += step) {
    for (int64_t to = from; to <= SIZE; to += step) {
      b->Args({from, to});
    }
  }
}

BENCHMARK_DEFINE_F(ListFixture, LLen)(benchmark::State& state) {
  const size_t nodeCount = state.range();
  PushNodes(nodeCount);

  merodis::Status s;
  uint64_t l;
  for (auto _ : state) {
    s = db->LLen("k", &l);
    assert(s.ok());
    assert(l == nodeCount);
  }
}

BENCHMARK_DEFINE_F(ListFixture, LIndex)(benchmark::State& state) {
  const int64_t index = state.range();
  PushNodes();

  merodis::Status s;
  std::string v;
  for (auto _ : state) {
    s = db->LIndex("k", index, &v);
    assert(s.ok());
    assert(v == "v");
  }
}

BENCHMARK_DEFINE_F(ListFixture, LPos)(benchmark::State& state) {
  const size_t targetIndex = state.range();
  const std::string target = std::to_string(targetIndex);
  PushAP();

  merodis::Status s;
  std::vector<uint64_t> indices;
  for (auto _ : state) {
    indices.clear();
    s = db->LPos("k", target, 0, 1, -1, &indices);
    assert(s.ok());
    assert(indices.size() == 1);
    assert(indices[0] == targetIndex);
  }
}

BENCHMARK_DEFINE_F(ListFixture, LRange)(benchmark::State& state) {
  const int64_t from = state.range(0);
  const int64_t to = state.range(1);
  size_t rangeSize = std::min(to, (int64_t)(SIZE - 1)) - std::max(from, (int64_t)0) + 1;
  PushNodes();

  merodis::Status s;
  std::vector<std::string> vs;
  for (auto _ : state) {
    vs.clear();
    s = db->LRange("k", from, to, &vs);
    assert(s.ok());
    assert(vs.size() == rangeSize);
  }
  assert(std::count(vs.begin(), vs.end(), "v") == rangeSize);
}

BENCHMARK_DEFINE_F(ListFixture, LSet)(benchmark::State& state) {
  const int64_t index = state.range();
  PushNodes();

  merodis::Status s;
  for (auto _ : state) {
    s = db->LSet("k", index, "v");
    assert(s.ok());
  }
  std::string _v;
  s = db->LIndex("k", index, &_v);
  assert(s.ok());
}

BENCHMARK_DEFINE_F(ListFixture, LPush)(benchmark::State& state) {
  size_t valueSize = state.range();
  std::string value = std::string(valueSize, 'v');

  merodis::Status s;
  for (auto _ : state) {
    s = db->LPush("k", value);
    assert(s.ok());
  }
}

BENCHMARK_DEFINE_F(ListFixture, LPop)(benchmark::State& state) {
  size_t valueSize = state.range();
  std::string value = std::string(valueSize, 'v');
  PushNodes(SIZE, value);

  merodis::Status s;
  std::string v;
  for (auto _ : state) {
    s = db->LPop("k", &v);
    assert(s.ok());
    assert(v == value);
  }
}

BENCHMARK_DEFINE_F(ListFixture, RPush)(benchmark::State& state) {
  size_t valueSize = state.range();
  std::string value = std::string(valueSize, 'v');

  merodis::Status s;
  for (auto _ : state) {
    s = db->RPush("k", value);
    assert(s.ok());
  }
}

BENCHMARK_DEFINE_F(ListFixture, RPop)(benchmark::State& state) {
  size_t valueSize = state.range();
  std::string value = std::string(valueSize, 'v');
  PushNodes(SIZE, value);

  merodis::Status s;
  std::string v;
  for (auto _ : state) {
    s = db->RPop("k", &v);
    assert(s.ok());
    assert(v == value);
  }
}

BENCHMARK_DEFINE_F(ListFixture, LTrim)(benchmark::State& state) {
  const int64_t from = state.range(0);
  const int64_t to = state.range(1);
  size_t rangeSize = std::min(to, (int64_t)(SIZE - 1)) - std::max(from, (int64_t)0) + 1;
  PushNodes();

  merodis::Status s;
  for (auto _ : state) {
    s = db->LTrim("k", from, to);
    assert(s.ok());
  }
  std::vector<std::string> vs;
  s = db->LRange("k", 0, -1, &vs);
  assert(s.ok());
  assert(std::count(vs.begin(), vs.end(), "v") == rangeSize);
}

BENCHMARK_DEFINE_F(ListFixture, LInsert)(benchmark::State& state) {
  const int64_t targetIndex = state.range();
  const std::string target = std::to_string(targetIndex);
  PushAP();

  merodis::Status s;
  for (auto _ : state) {
    s = db->LInsert("k", merodis::kBefore, target, "w");
    assert(s.ok());
  }
  std::vector<std::string> vs;
  s = db->LRange("k", targetIndex, targetIndex + (int64_t)state.iterations() - 1, &vs);
  assert(s.ok());
  assert(vs == std::vector<std::string>(state.iterations(), "w"));
}

BENCHMARK_DEFINE_F(ListFixture, LRemSingle)(benchmark::State& state) {
  size_t p = state.range(0);
  size_t size = SIZE;
  PushNodes(size);

  merodis::Status s;
  s = db->LSet("k", (int64_t)p, "t");
  assert(s.ok());

  uint64_t c;
  for (auto _ : state) {
    s = db->LRem("k", 0, "t", &c);
    assert(s.ok());
    assert(c == 1);
  }
}

BENCHMARK_DEFINE_F(ListFixture, LRemPair)(benchmark::State& state) {
  size_t p1 = state.range(0);
  size_t p2 = state.range(1);
  size_t size = SIZE;
  PushNodes(size);

  merodis::Status s;
  s = db->LSet("k", (int64_t)p1, "t");
  assert(s.ok());
  s = db->LSet("k", (int64_t)p2, "t");
  assert(s.ok());

  uint64_t c;
  for (auto _ : state) {
    s = db->LRem("k", 0, "t", &c);
    assert(s.ok());
    assert(c == 2 - (p1 == p2));
  }
}

BENCHMARK_DEFINE_F(ListFixture, LRemPoints)(benchmark::State& state) {
  size_t fragmentCount = state.range();
  size_t fragmentSize = SIZE / fragmentCount;
  size_t count1 = fragmentCount;
  PushFragments(SIZE, fragmentSize);

  merodis::Status s;
  uint64_t _c;
  for (auto _ : state) {
    s = db->LRem("k", 0, "1", &_c);
    assert(s.ok());
    assert(_c == count1);
  }
}

BENCHMARK_DEFINE_F(ListFixture, LRemFragments)(benchmark::State& state) {
  size_t fragmentCount = state.range();
  size_t fragmentSize = SIZE / fragmentCount;
  size_t count0 = SIZE - fragmentCount;
  PushFragments(SIZE, fragmentSize);

  merodis::Status s;
  uint64_t _c;
  for (auto _ : state) {
    s = db->LRem("k", 0, "0", &_c);
    assert(s.ok());
    assert(_c == count0);
  }
}

BENCHMARK_DEFINE_F(ListFixture, LPopRPush)(benchmark::State& state) {
  PushNodes();

  merodis::Status s;
  std::string v;
  for (auto _ : state) {
    s = db->LMove("k", "k", merodis::kLeft, merodis::kRight, &v);
    assert(s.ok());
    assert(v == "v");
  }
}

BENCHMARK_DEFINE_F(ListFixture, LMove)(benchmark::State& state) {
  PushNodes(SIZE, "v", "k1");
  PushNodes(SIZE, "v", "k2");

  merodis::Status s;
  std::string v;
  for (auto _ : state) {
    s = db->LMove("k1", "k2", merodis::kLeft, merodis::kRight, &v);
    assert(s.ok());
    assert(v == "v");
  }
}


BENCHMARK_REGISTER_F(ListFixture, LLen)->RangeMultiplier(16)->Range(1, SIZE);
BENCHMARK_REGISTER_F(ListFixture, LIndex)->Apply(IndexGenerator<6>);
BENCHMARK_REGISTER_F(ListFixture, LPos)->Apply(IndexGenerator<2>);
BENCHMARK_REGISTER_F(ListFixture, LRange)->Apply(RangeGenerator<2>);
BENCHMARK_REGISTER_F(ListFixture, LSet)->Apply(IndexGenerator<2>);
BENCHMARK_REGISTER_F(ListFixture, LPush)->RangeMultiplier(16)->Range(1, SIZE);
BENCHMARK_REGISTER_F(ListFixture, LPop)->RangeMultiplier(16)->Range(1, 1 << 12);
BENCHMARK_REGISTER_F(ListFixture, RPush)->RangeMultiplier(16)->Range(1, SIZE);
BENCHMARK_REGISTER_F(ListFixture, RPop)->RangeMultiplier(16)->Range(1, 1 << 12);
BENCHMARK_REGISTER_F(ListFixture, LTrim)->Iterations(1)->Repetitions(16)->Apply(RangeGenerator<4>)->ReportAggregatesOnly(true);
BENCHMARK_REGISTER_F(ListFixture, LInsert)->Iterations(16)->Apply(IndexGenerator<4>);
BENCHMARK_REGISTER_F(ListFixture, LRemSingle)->Iterations(1)->Repetitions(1)->Apply(IndexGenerator<4>)->ReportAggregatesOnly(true);
BENCHMARK_REGISTER_F(ListFixture, LRemPair)->Iterations(1)->Repetitions(1)->Apply(IndexPairGenerator<7>)->ReportAggregatesOnly(true);
BENCHMARK_REGISTER_F(ListFixture, LRemPoints)->Iterations(1)->Repetitions(16)->RangeMultiplier(2)->Range(1, SIZE)->ReportAggregatesOnly(true);
BENCHMARK_REGISTER_F(ListFixture, LRemFragments)->Iterations(1)->Repetitions(16)->RangeMultiplier(2)->Range(1, SIZE)->ReportAggregatesOnly(true);
BENCHMARK_REGISTER_F(ListFixture, LPopRPush);
BENCHMARK_REGISTER_F(ListFixture, LMove);
