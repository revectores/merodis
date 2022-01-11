#ifndef MERODIS_REDIS_HASH_H
#define MERODIS_REDIS_HASH_H

#include "redis.h"

namespace merodis {

struct HashMetaValue {
  explicit HashMetaValue() noexcept = default;

  uint64_t len;
};

struct HashNodeKey {
  explicit HashNodeKey() noexcept = default;
  explicit HashNodeKey(const Slice& key, const Slice& hashKey) noexcept {
    data = new char[key.size() + hashKey.size()];
    keySize = key.size();
    hashKeySize = hashKey.size();
    memcpy(data, key.data(), keySize);
    memcpy(data + keySize, hashKey.data(), hashKeySize);
  };
  explicit HashNodeKey(const Slice& rawHashNodeKey, size_t keySize_) noexcept {
    data = const_cast<char*>(rawHashNodeKey.data());
    keySize = keySize_;
    hashKeySize = rawHashNodeKey.size() - keySize_;
  }
  ~HashNodeKey() noexcept { delete[] data; }

  size_t size() { return keySize + hashKeySize; }
  Slice key() { return {data, keySize}; }
  Slice hashKey() { return {data + keySize, hashKeySize}; }
  Slice Encode() { return {data, size()}; }

  char* data;
  size_t keySize;
  size_t hashKeySize;
};



class RedisHash final : public Redis {
public:
  RedisHash() noexcept;
  ~RedisHash() noexcept final;

  Status Open(const Options& options, const std::string& db_path) noexcept final;

  Status HGet(const Slice& key, const Slice& hashKey, std::string* value);
  Status HSet(const Slice& key, const Slice& hashKey, const Slice& value);
};

}

#endif //MERODIS_REDIS_HASH_H