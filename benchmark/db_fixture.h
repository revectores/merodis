#ifndef MERODIS_DB_FIXTURE_H
#define MERODIS_DB_FIXTURE_H

#include "benchmark/benchmark.h"
#include "merodis/merodis.h"

typedef std::vector<merodis::Slice> Slices;
typedef std::set<merodis::Slice> SliceSet;

class RedisFixture : public benchmark::Fixture {
public:
  void SetUp(benchmark::State& state) override;
  void TearDown(benchmark::State& state) override;
  merodis::Merodis* db;
};


#endif //MERODIS_DB_FIXTURE_H
