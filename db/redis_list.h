#ifndef MERODIS_REDIS_LIST_H
#define MERODIS_REDIS_LIST_H

#include <cstdint>
#include "redis.h"

namespace merodis {

typedef Slice ListMetaKey;

struct ListMetaValue {
  constexpr static uint64_t InitIndex = 9223372036854775807ull;

  explicit ListMetaValue() noexcept;
  explicit ListMetaValue(int64_t count, int64_t leftIndex, int64_t rightIndex) noexcept;
  explicit ListMetaValue(const std::string& rawValue) noexcept;
  ~ListMetaValue() noexcept = default;

  Slice Encode() const;

  uint64_t count;
  uint64_t leftIndex;
  uint64_t rightIndex;
};

struct ListNodeKey {
  explicit ListNodeKey(Slice key, uint64_t index) noexcept;
  explicit ListNodeKey(const std::string& rawValue) noexcept;
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
  Status LIndex(const Slice& key, uint64_t index, std::string* value) noexcept;
  Status LPush(const Slice& key, const Slice& value) noexcept;
};

}


#endif //MERODIS_REDIS_LIST_H
