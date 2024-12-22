#ifndef MERODIS_RANDOM_H
#define MERODIS_RANDOM_H

#include <vector>
#include <random>

inline uint64_t rand_uint64() {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<std::mt19937::result_type> dist;
  return dist(gen);
}

inline uint64_t rand_uint64(uint64_t lower, uint64_t upper) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<std::mt19937::result_type> dist(lower, upper);
  return dist(gen);
}

inline std::vector<uint64_t> rand_uint64(uint64_t lower, uint64_t upper, uint64_t count) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<std::mt19937::result_type> dist(lower, upper);

  auto generator = [&dist, &gen](){ return dist(gen); };
  std::vector<uint64_t> rands(count);
  generate(begin(rands), end(rands), generator);
  return rands;
}

#endif //MERODIS_RANDOM_H
