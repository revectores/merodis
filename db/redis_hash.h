#ifndef MERODIS_REDIS_HASH_H
#define MERODIS_REDIS_HASH_H

#include "redis.h"
#include "util/coding.h"

namespace merodis {

struct HashMetaValue {
  explicit HashMetaValue() noexcept : len(0) {};
  explicit HashMetaValue(const std::string& rawValue) noexcept {
    len = DecodeFixed64(rawValue.data());
  };
  ~HashMetaValue() noexcept = default;
  std::string Encode() noexcept {
    std::string rawValue(sizeof(int64_t), 0);
    EncodeFixed64(rawValue.data(), len);
    return rawValue;
  }

  uint64_t len;
};

struct HashNodeKey {
  explicit HashNodeKey() noexcept = default;
  explicit HashNodeKey(const Slice& key, const Slice& hashKey) noexcept {
    data = new char[key.size() + 1 + hashKey.size()];
    keySize = key.size();
    hashKeySize = hashKey.size();
    memcpy(data, key.data(), keySize);
    data[keySize] = '\0';
    memcpy(data + keySize + 1, hashKey.data(), hashKeySize);
  };
  explicit HashNodeKey(const Slice& rawHashNodeKey, size_t keySize_) noexcept {
    data = const_cast<char*>(rawHashNodeKey.data());
    keySize = keySize_;
    hashKeySize = rawHashNodeKey.size() - keySize_ - 1;
  }
  ~HashNodeKey() noexcept { delete[] data; }

  size_t size() { return keySize + 1 + hashKeySize; }
  Slice key() { return {data, keySize}; }
  Slice hashKey() { return {data + keySize + 1, hashKeySize}; }
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
  Status HExists(const Slice& key, const Slice& hashKey, bool* exists);
  Status HSet(const Slice& key, const Slice& hashKey, const Slice& value);
  Status HLen(const Slice& key, uint64_t* len);
};

}

#endif //MERODIS_REDIS_HASH_H
