#ifndef MERODIS_REDIS_SET_BASIC_IMPL_H
#define MERODIS_REDIS_SET_BASIC_IMPL_H

#include "redis_set.h"
#include "util/coding.h"

namespace merodis {

struct SetMetaValue {
  explicit SetMetaValue() noexcept : len(0) {};
  explicit SetMetaValue(const std::string& rawValue) noexcept {
    len = DecodeFixed64(rawValue.data());
  }
  ~SetMetaValue() noexcept = default;
  std::string Encode() noexcept {
    std::string rawValue(sizeof(int64_t), 0);
    EncodeFixed64(rawValue.data(), len);
    return rawValue;
  }

  uint64_t len;
};

struct SetNodeKey {
  explicit SetNodeKey() noexcept = default;
  explicit SetNodeKey(const Slice& key, const Slice& setKey) noexcept :
    keySize_(key.size()),
    setKeySize_(setKey.size()),
    data_(keySize_ + 1 + setKeySize_, 0) {
    memcpy(data_.data(), key.data(), keySize_);
    data_[keySize_] = '\0';
    memcpy(data_.data() + keySize_ + 1, setKey.data(), setKeySize_);
  }
  explicit SetNodeKey(const Slice& rawSetNodeKey, size_t keySize) noexcept :
    keySize_(keySize),
    setKeySize_(rawSetNodeKey.size() - keySize_ - 1),
    data_(rawSetNodeKey.ToString()) {
  }
  ~SetNodeKey() noexcept = default;

  size_t size() const { return data_.size(); }
  Slice key() const { return {data_.data(), keySize_}; }
  Slice setKey() const { return {data_.data() + keySize_ + 1, setKeySize_}; }
  Slice Encode() const { return data_; }

private:
  size_t keySize_{};
  size_t setKeySize_{};
  std::string data_;
};


class RedisSetBasicImpl final : public RedisSet {
public:
  RedisSetBasicImpl() noexcept;
  ~RedisSetBasicImpl() noexcept final;

  Status SCard(const Slice& key, uint64_t* len) final;
  Status SIsMember(const Slice& key, const Slice& setKey, bool* isMember) final;
  Status SMIsMember(const Slice& key, const std::set<Slice>& keys, std::vector<bool>* isMembers) final;
  Status SMembers(const Slice& key, std::vector<std::string>* keys) final;
  Status SRandMember(const Slice& key, std::string* member) final;
  Status SRandMember(const Slice& key, int64_t count, std::vector<std::string>* members) final;
  Status SAdd(const Slice& key, const Slice& setKey, uint64_t* count) final;
  Status SAdd(const Slice& key, const std::set<Slice>& keys, uint64_t* count) final;
  Status SRem(const Slice& key, const Slice& member, uint64_t* count) final;
  Status SRem(const Slice& key, const std::set<Slice>& members, uint64_t* count) final;
  Status SPop(const Slice& key, std::string* member) final;
  Status SPop(const Slice& key, uint64_t count, std::vector<std::string>* members) final;
  Status SMove(const Slice& srcKey, const Slice& dstKey, const Slice& member, uint64_t* count) final;

private:
  uint64_t CountKeyIntersection(const Slice& key, const SetNodeKey& nodeKey);
};

}

#endif //MERODIS_REDIS_SET_BASIC_IMPL_H
