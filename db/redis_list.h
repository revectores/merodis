#ifndef MERODIS_REDIS_LIST_H
#define MERODIS_REDIS_LIST_H

#include <cstdint>
#include "redis.h"

namespace merodis {

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
  explicit ListNodeKey(Slice key, uint64_t index) noexcept;
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
  Status LRange(const Slice& key, int64_t from, int64_t to, std::vector<std::string>* values) noexcept;
  Status Push(const Slice& key, const Slice& value, bool createListIfNotFound, enum Side side) noexcept;
  Status Push(const Slice& key, const std::vector<const Slice>& values, bool createListIfNotFound, enum Side side) noexcept;
  Status Pop(const Slice& key, std::string* value, enum Side side) noexcept;
  Status Pop(const Slice& key, uint64_t count, std::vector<std::string>* values, enum Side side) noexcept;
  Status LInsert(const Slice& key, const BeforeOrAfter& beforeOrAfter, const Slice& pivotValue, const Slice& value) noexcept;
  Status LMove(const Slice& srcKey, const Slice& dstKey, enum Side srcSide, enum Side dstSide, std::string* value) noexcept;

private:
  static inline uint64_t GetInternalIndex(int64_t userIndex, ListMetaValue meta) noexcept;
};

}


#endif //MERODIS_REDIS_LIST_H
