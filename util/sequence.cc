#include "sequence.h"

#include <vector>
#include <iterator>
#include <algorithm>


std::vector<Block> GetBlocks(uint64_t& start,
                             uint64_t& end,
                             const std::vector<uint64_t>& removedIndices) {
  std::vector<Block> blocks;
  blocks.reserve(removedIndices.size());
  std::vector<uint64_t>::const_iterator iter = removedIndices.cbegin();
  for (; iter != removedIndices.cend() && *iter == start; ++iter) ++start;
  if (start > end) return {};
  uint64_t prevIndex = start - 1;
  for (; iter != removedIndices.cend(); ++iter) {
    blocks.emplace_back(Block{prevIndex + 1, *iter - 1});
    for (; iter + 1 != removedIndices.cend() && *iter == *(iter + 1) - 1; ++iter);
    prevIndex = *iter;
  }
  if (prevIndex != end) {
    blocks.emplace_back(Block{prevIndex + 1, end});
  } else {
    end = blocks.back().end;
  }

  int64_t (*sizeComparator)(Block, Block) = [](Block a, Block b) -> int64_t {
    return (int64_t)a.size() - (int64_t)b.size();
  };
  std::vector<Block>::iterator forward_it = std::max_element(blocks.begin(), blocks.end(), sizeComparator);
  std::vector<Block>::reverse_iterator backward_it = std::make_reverse_iterator(forward_it);
  forward_it->step = 0;
  ++forward_it;
  for (; forward_it != blocks.end(); ++forward_it) {
    forward_it->step = -(int64_t)((forward_it - 1)->step + forward_it->start - (forward_it - 1)->end - 1);
  }
  for (; backward_it != blocks.rend(); ++backward_it) {
    backward_it->step = (int64_t)((backward_it - 1)->step + (backward_it - 1)->start - backward_it->end - 1);
  }
  start += (backward_it - 1)->step;
  end += (forward_it - 1)->step;
  return blocks;
}
