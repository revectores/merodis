#ifndef MERODIS_REDIS_LIST_H
#define MERODIS_REDIS_LIST_H

#include <cstdint>
#include "redis.h"

namespace merodis {

typedef int64_t UserIndex;
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

  Slice Encode() const;

  Slice key;
  uint64_t index;
};

typedef std::string ListNodeValue;


class RedisList final : public Redis {
public:
  RedisList() noexcept;
  ~RedisList() noexcept final;

  Status Open(const Options& options, const std::string& db_path) noexcept final;
  Status LLen(const Slice& key, uint64_t* len) noexcept;
  Status LIndex(const Slice& key, int64_t index, std::string* value) noexcept;
  Status LPos(const Slice& key, const Slice& value, int64_t rank, int64_t count, int64_t maxlen, std::vector<uint64_t>& indices) noexcept;
  Status LRange(const Slice& key, int64_t from, int64_t to, std::vector<std::string>* values) noexcept;
  Status LSet(const Slice& key, int64_t index, const Slice& value) noexcept;
  Status Push(const Slice& key, const Slice& value, bool createListIfNotFound, enum Side side) noexcept;
  Status Push(const Slice& key, const std::vector<Slice>& values, bool createListIfNotFound, enum Side side) noexcept;
  Status Pop(const Slice& key, std::string* value, enum Side side) noexcept;
  Status Pop(const Slice& key, uint64_t count, std::vector<std::string>* values, enum Side side) noexcept;
  Status LTrim(const Slice& key, int64_t from, int64_t to) noexcept;
  Status LInsert(const Slice& key, const BeforeOrAfter& beforeOrAfter, const Slice& pivotValue, const Slice& value) noexcept;
  Status LRem(const Slice& key, int64_t count, const Slice& value, uint64_t* removedCount) noexcept;
  Status LMove(const Slice& srcKey, const Slice& dstKey, enum Side srcSide, enum Side dstSide, std::string* value) noexcept;

private:
  static inline InternalIndex GetInternalIndex(UserIndex userIndex, ListMetaValue meta) noexcept;
  static inline bool IsValidInternalIndex(InternalIndex internalIndex, ListMetaValue meta) noexcept;
};

}


#endif //MERODIS_REDIS_LIST_H
