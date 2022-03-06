#ifndef MERODIS_REDIS_SET_BASIC_IMPL_H
#define MERODIS_REDIS_SET_BASIC_IMPL_H

#include <cstdint>
#include <string>
#include <vector>
#include <map>
#include <set>

#include "redis_set.h"
#include "util/coding.h"

namespace merodis {

class SetIteratorComparator {
public:
  explicit SetIteratorComparator(std::map<Iterator*, Slice>& iter2key): iter2key(iter2key) {}

  bool operator()(Iterator* iter1, Iterator* iter2) {
    Slice key1 = iter2key[iter1];
    Slice key2 = iter2key[iter2];
    Slice member1(iter1->key().data() + key1.size() + 1, iter1->key().size() - key1.size() - 1);
    Slice member2(iter2->key().data() + key2.size() + 1, iter2->key().size() - key2.size() - 1);
    return member2 < member1;
  };

private:
  std::map<Iterator*, Slice> iter2key;
};

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

  Status Del(const Slice& key);

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
  Status SUnion(const std::vector<Slice>& keys, std::vector<std::string>* members) final;
  Status SInter(const std::vector<Slice>& keys, std::vector<std::string>* members) final;
  Status SDiff(const std::vector<Slice>& keys, std::vector<std::string>* members) final;
  Status SUnionStore(const std::vector<Slice>& keys, const Slice& dstKey, uint64_t* count) final;
  Status SInterStore(const std::vector<Slice>& keys, const Slice& dstKey, uint64_t* count) final;
  Status SDiffStore(const std::vector<Slice>& keys, const Slice& dstKey, uint64_t* count) final;

private:
  Slice GetMember(Iterator* iter, uint64_t keySize);
  uint64_t CountKeyIntersection(const Slice& key, const SetNodeKey& nodeKey);
  static bool IsMemberKey(const Slice& iterKey, uint64_t keySize);
};

}

#endif //MERODIS_REDIS_SET_BASIC_IMPL_H
