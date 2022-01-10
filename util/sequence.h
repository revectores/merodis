#ifndef MERODIS_SEQUENCE_H
#define MERODIS_SEQUENCE_H

#include <cstdint>
#include <vector>

struct Block {
  uint64_t start;
  uint64_t end;
  int64_t step;  // This is not enough for keyspace-scale distance,
                 // for instance distance between block [0, 0] and block [2^64 - 1, 2^64 - 1].
                 // consider replacing it with uint64_t and a sign bool
                 // if you really would like to utilize the entire key space.
  uint64_t size() const noexcept {
    return end - start + 1;
  }
  bool operator==(const Block& other) const noexcept {
    return (start == other.start) && (end == other.end) && (step == other.step);
  }
};

std::vector<Block> GetBlocks(uint64_t& start, uint64_t& end, const std::vector<uint64_t>& removedIndices);

#endif //MERODIS_SEQUENCE_H
