#ifndef MERODIS_REDIS_HASH_BASIC_IMPL_H
#define MERODIS_REDIS_HASH_BASIC_IMPL_H

#include "redis_hash.h"
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
  explicit HashNodeKey(const Slice& key, const Slice& hashKey) noexcept :
    keySize_(key.size()),
    hashKeySize_(hashKey.size()),
    data_(keySize_ + 1 + hashKeySize_, 0) {
    memcpy(data_.data(), key.data(), keySize_);
    data_[keySize_] = '\0';
    memcpy(data_.data() + keySize_ + 1, hashKey.data(), hashKeySize_);
  };
  explicit HashNodeKey(const Slice& rawHashNodeKey, size_t keySize) noexcept :
    keySize_(keySize),
    hashKeySize_(rawHashNodeKey.size() - keySize_ - 1),
    data_(rawHashNodeKey.ToString()) {
  }
  ~HashNodeKey() noexcept = default;

  size_t size() const { return data_.size(); }
  Slice key() const { return {data_.data(), keySize_}; }
  Slice hashKey() const { return {data_.data() + keySize_ + 1, hashKeySize_}; }
  Slice Encode() const { return data_; }

private:
  size_t keySize_;
  size_t hashKeySize_;
  std::string data_;
};



class RedisHashBasicImpl final : public RedisHash {
public:
  RedisHashBasicImpl() noexcept;
  ~RedisHashBasicImpl() noexcept final;

  Status HLen(const Slice& key, uint64_t* len) final;
  Status HGet(const Slice& key, const Slice& hashKey, std::string* value) final;
  Status HMGet(const Slice& key, const std::vector<Slice>& hashKeys, std::vector<std::string>* values) final;
  Status HGetAll(const Slice& key, std::map<std::string, std::string>* kvs) final;
  Status HKeys(const Slice& key, std::vector<std::string>* keys) final;
  Status HVals(const Slice& key, std::vector<std::string>* values) final;
  Status HExists(const Slice& key, const Slice& hashKey, bool* exists) final;
  Status HSet(const Slice& key, const Slice& hashKey, const Slice& value, uint64_t* count) final;
  Status HSet(const Slice& key, const std::map<Slice, Slice>& kvs, uint64_t* count) final;
  Status HDel(const Slice& key, const Slice& hashKey, uint64_t* count) final;
  Status HDel(const Slice& key, const std::set<Slice>& hashKeys, uint64_t* count) final;

private:
  uint64_t CountKeysIntersection(const Slice& key, const HashNodeKey& hashKey);
  uint64_t CountKeysIntersection(const Slice& key, const std::set<Slice>& hashKeys);
  uint64_t CountKeysIntersection(const Slice& key, const std::map<Slice, Slice>& kvs);
};

}

#endif //MERODIS_REDIS_HASH_BASIC_IMPL_H
