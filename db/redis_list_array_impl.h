#ifndef MERODIS_REDIS_LIST_ARRAY_IMPL_H
#define MERODIS_REDIS_LIST_ARRAY_IMPL_H

#include <cstdint>
#include <string>
#include <vector>

#include "redis_list.h"

namespace merodis {

typedef uint64_t InternalIndex;

typedef Slice ListMetaKey;

struct ListMetaValue {
  constexpr static uint64_t InitIndex = 9223372036854775807ull;

  explicit ListMetaValue() noexcept;
  explicit ListMetaValue(uint64_t leftIndex, uint64_t rightIndex) noexcept;
  explicit ListMetaValue(const std::string& rawValue) noexcept;
  ~ListMetaValue() noexcept = default;

  uint64_t Length() const;
  Slice Encode() const;

  uint64_t leftIndex;
  uint64_t rightIndex;
};

struct ListNodeKey {
  explicit ListNodeKey(Slice key, InternalIndex index) noexcept;
  explicit ListNodeKey(const Slice& rawValue) noexcept;
  ~ListNodeKey() noexcept = default;

  std::string Encode() const;

  Slice key;
  uint64_t index;
};

typedef std::string ListNodeValue;


class RedisListArrayImpl final : public RedisList {
public:
  RedisListArrayImpl() noexcept;
  ~RedisListArrayImpl() noexcept final;

  Status LLen(const Slice& key, uint64_t* len) noexcept override;
  Status LIndex(const Slice& key, UserIndex index, std::string* value) noexcept final;
  Status LPos(const Slice& key, const Slice& value, int64_t rank, int64_t count, int64_t maxlen, std::vector<uint64_t>* indices) noexcept final;
  Status LRange(const Slice& key, UserIndex from, UserIndex to, std::vector<std::string>* values) noexcept final;
  Status LSet(const Slice& key, UserIndex index, const Slice& value) noexcept final;
  Status Push(const Slice& key, const Slice& value, bool createListIfNotFound, enum Side side) noexcept final;
  Status Push(const Slice& key, const std::vector<Slice>& values, bool createListIfNotFound, enum Side side) noexcept final;
  Status Pop(const Slice& key, std::string* value, enum Side side) noexcept final;
  Status Pop(const Slice& key, uint64_t count, std::vector<std::string>* values, enum Side side) noexcept final;
  Status LTrim(const Slice& key, UserIndex from, UserIndex to) noexcept final;
  Status LInsert(const Slice& key, const BeforeOrAfter& beforeOrAfter, const Slice& pivotValue, const Slice& value) noexcept final;
  Status LRem(const Slice& key, int64_t count, const Slice& value, uint64_t* removedCount) noexcept final;
  Status LMove(const Slice& srcKey, const Slice& dstKey, enum Side srcSide, enum Side dstSide, std::string* value) noexcept final;

private:
  static inline InternalIndex GetInternalIndex(UserIndex userIndex, ListMetaValue meta) noexcept;
  static inline bool IsValidInternalIndex(InternalIndex internalIndex, ListMetaValue meta) noexcept;
};

}


#endif //MERODIS_REDIS_LIST_ARRAY_IMPL_H
