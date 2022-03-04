#ifndef MERODIS_REDIS_ZSET_BASIC_IMPL_H
#define MERODIS_REDIS_ZSET_BASIC_IMPL_H

#include <limits>
#include "redis_zset.h"
#include "iterator_decorator.h"
#include "util/coding.h"

namespace merodis {

static uint64_t ScoreOffset = -std::numeric_limits<int64_t>::min();

struct ZSetMetaValue {
  explicit ZSetMetaValue() noexcept: len(0) {};
  explicit ZSetMetaValue(const std::string& rawValue) noexcept {
    len = DecodeFixed64(rawValue.data());
  }
  ~ZSetMetaValue() noexcept = default;
  std::string Encode() noexcept {
    std::string rawValue(sizeof(int64_t), 0);
    EncodeFixed64(rawValue.data(), len);
    return rawValue;
  }

  uint64_t len;
};


struct ZSetMemberKey {
  explicit ZSetMemberKey() noexcept = default;
  explicit ZSetMemberKey(const Slice& key, const Slice& member) noexcept:
    keySize_(key.size()),
    memberSize_(member.size()),
    data_(keySize_ + 1 + memberSize_, 0) {
    memcpy(data_.data(), key.data(), keySize_);
    data_[keySize_] = static_cast<char>(0xff);
    memcpy(data_.data() + keySize_ + 1, member.data(), memberSize_);
  }
  explicit ZSetMemberKey(const Slice& rawZSetMemberKey, size_t keySize) noexcept:
    keySize_(keySize),
    memberSize_(rawZSetMemberKey.size() - keySize_ - 1),
    data_(rawZSetMemberKey.ToString()) {
  }
  ~ZSetMemberKey() noexcept = default;

  size_t size() const { return data_.size(); }
  Slice key() const { return {data_.data(), keySize_}; }
  Slice member() const { return {data_.data() + keySize_ + 1, memberSize_}; }
  Slice Encode() const { return data_; }

private:
  size_t keySize_;
  size_t memberSize_;
  std::string data_;
};


struct ZSetMemberValue {
  explicit ZSetMemberValue() noexcept: score_(ScoreOffset) {};
  explicit ZSetMemberValue(int64_t score) noexcept: score_(score + ScoreOffset) {};
  explicit ZSetMemberValue(const std::string& rawValue) noexcept {
    score_ = DecodeFixed64(rawValue.data());
  }
  ~ZSetMemberValue() noexcept = default;
  int64_t score() const noexcept { return static_cast<int64_t>(score_ - ScoreOffset); }
  void score(int64_t score) { score_ = score + ScoreOffset; }
  std::string Encode() const noexcept {
    std::string rawValue(sizeof(int64_t), 0);
    EncodeFixed64(rawValue.data(), score_);
    return rawValue;
  }

private:
  uint64_t score_;
};


struct ZSetScoredMemberKey {
  explicit ZSetScoredMemberKey() noexcept = default;
  explicit ZSetScoredMemberKey(const Slice& key, const Slice& member, int64_t score) noexcept:
    keySize_(key.size()),
    memberSize_(member.size()),
    data_(keySize_ + 1 + sizeof(int64_t) + memberSize_, 0) {
    memcpy(data_.data(), key.data(), keySize_);
    data_[keySize_] = '\0';
    EncodeFixed64(data_.data() + keySize_ + 1, score + ScoreOffset);
    memcpy(data_.data() + keySize_ + sizeof(int64_t) + 1, member.data(), memberSize_);
  }
  explicit ZSetScoredMemberKey(const Slice& key, const std::pair<Slice, int64_t>& scoredMember) noexcept:
    ZSetScoredMemberKey(key, scoredMember.first, scoredMember.second) {}
  explicit ZSetScoredMemberKey(const Slice& rawZSetScoredMemberKey, size_t keySize) noexcept:
    keySize_(keySize),
    memberSize_(rawZSetScoredMemberKey.size() - keySize_ - sizeof(int64_t) - 1),
    data_(rawZSetScoredMemberKey.ToString()) {
  }
  ~ZSetScoredMemberKey() noexcept = default;

  size_t size() const { return data_.size(); }
  Slice key() const { return {data_.data(), keySize_}; }
  Slice member() const { return {data_.data() + data_.size() - memberSize_, memberSize_}; }
  int64_t score() const { return static_cast<int64_t>(DecodeFixed64(data_.data() + keySize_ + 1) - ScoreOffset); }
  void score(int64_t score) { EncodeFixed64(data_.data() + keySize_ + 1, score + ScoreOffset); };
  Slice Encode() { return data_; }

private:
  size_t keySize_;
  size_t memberSize_;
  std::string data_;
};

class ScoredMemberIterator: public IteratorDecorator {
public:
  explicit ScoredMemberIterator(DB* db, const Slice& setKey):
    IteratorDecorator(db->NewIterator(ReadOptions())),
    setKey_(setKey) {
    iter_->Seek(setKey);
    valid_ = iter_->Valid() && iter_->key() == setKey;
  }
  explicit ScoredMemberIterator(Iterator* iter, const Slice& setKey):
    IteratorDecorator(iter),
    setKey_(setKey),
    valid_(iter_->Valid() && IsScoredMemberKey()) {}
  ScoredMemberIterator(const ScoredMemberIterator&) = delete;
  ScoredMemberIterator& operator=(const ScoredMemberIterator&) = delete;
  ~ScoredMemberIterator() override = default;

  bool Valid() const override { return valid_; }
  void Next() override {
    assert(valid_);
    iter_->Next();
    valid_ = iter_->Valid() && IsScoredMemberKey();
  }
  int64_t score() const {
    return static_cast<int64_t>(DecodeFixed64(key().data() + setKey_.size() + 1) - ScoreOffset);
  };
  Slice member() const {
    size_t memberSize = key().size() - setKey_.size() - 1 - sizeof(uint64_t);
    return {key().data() + key().size() - memberSize, memberSize};
  }

private:
  Slice setKey_;
  bool valid_;
  bool IsScoredMemberKey() const {
    return key().size() > setKey_.size() && key()[setKey_.size()] == 0;
  }
};

class MemberIterator: public IteratorDecorator {
public:
  explicit MemberIterator(DB* db, const Slice& setKey):
    IteratorDecorator(db->NewIterator(ReadOptions())),
    setKey_(setKey) {
    std::string memberKeyPrefix(setKey.size() + 1, 0);
    memcpy(memberKeyPrefix.data(), setKey_.data(), setKey_.size());
    memberKeyPrefix[setKey.size()] = (char)0xff;
    iter_->Seek(memberKeyPrefix);
    valid_ = iter_->Valid() && iter_->key().starts_with(memberKeyPrefix);
  }
  explicit MemberIterator(Iterator* iter, const Slice& setKey):
    IteratorDecorator(iter),
    setKey_(setKey),
    valid_(iter_->Valid() && IsMemberKey()) {};
  MemberIterator(const MemberIterator&) = delete;
  MemberIterator& operator=(const MemberIterator&) = delete;
  ~MemberIterator() override = default;

  bool Valid() const override { return valid_; }
  void Next() override {
    assert(valid_);
    iter_->Next();
    valid_ = iter_->Valid() && IsMemberKey();
  }
  Slice member() const {
    return {key().data() + setKey_.size() + 1, key().size() - setKey_.size() - 1};
  }

private:
  Slice setKey_;
  bool valid_;
  bool IsMemberKey() const {
    return key().size() > setKey_.size() && key()[setKey_.size()] == (char)0xff;
  }
};


class RedisZSetBasicImpl final : public RedisZSet {
public:
  RedisZSetBasicImpl() noexcept;
  ~RedisZSetBasicImpl() noexcept final;

  Status ZCard(const Slice& key, uint64_t* len) final;
  Status ZScore(const Slice& key, const Slice& member, int64_t* score) final;
  Status ZMScore(const Slice& key, const std::vector<Slice>& members, std::vector<int64_t>* scores) final;
  Status ZRank(const Slice& key, const Slice& member, uint64_t* rank) final;
  Status ZRevRank(const Slice& key, const Slice& member, uint64_t* rank) final;
  Status ZCount(const Slice& key, int64_t minScore, int64_t maxScore, uint64_t* count) final;
  Status ZLexCount(const Slice& key, const Slice& minLex, const Slice& maxLex, uint64_t* count) final;
  Status ZRange(const Slice& key, int64_t minRank, int64_t maxRank, Members* members) final;
  Status ZRangeByScore(const Slice& key, int64_t minScore, int64_t maxScore, Members* members) final;
  Status ZRangeByLex(const Slice& key, const Slice& minLex, const Slice& maxLex, Members* members) final;
  Status ZRangeWithScores(const Slice& key, int64_t minRank, int64_t maxRank, ScoredMembers* scoredMembers) final;
  Status ZRangeByScoreWithScores(const Slice& key, int64_t minScore, int64_t maxScore, ScoredMembers* scoredMembers) final;
  Status ZRangeByLexWithScores(const Slice& key, const Slice& minLex, const Slice& maxLex, ScoredMembers* scoredMembers) final;
  Status ZRevRange(const Slice& key, int64_t minRank, int64_t maxRank, Members* members) final;
  Status ZRevRangeByScore(const Slice& key, int64_t minScore, int64_t maxScore, Members* members) final;
  Status ZRevRangeByLex(const Slice& key, const Slice& minLex, const Slice& maxLex, Members* members) final;
  Status ZRevRangeWithScores(const Slice& key, int64_t minRank, int64_t maxRank, ScoredMembers* scoredMembers) final;
  Status ZRevRangeByScoreWithScores(const Slice& key, int64_t minScore, int64_t maxScore, ScoredMembers* scoredMembers) final;
  Status ZRevRangeByLexWithScores(const Slice& key, const Slice& minLex, const Slice& maxLex, ScoredMembers* scoredMembers) final;

  Status ZAdd(const Slice& key, const std::pair<Slice, int64_t>& scoredMember, uint64_t* count) final;
  Status ZAdd(const Slice& key, const std::map<Slice, int64_t>& scoredMembers, uint64_t* count) final;
  Status ZRem(const Slice& key, const Slice& member, uint64_t* count) final;
  Status ZRem(const Slice& key, const std::set<Slice>& members, uint64_t* count) final;
  Status ZPopMax(const Slice& key, ScoredMember* scoredMember) final;
  Status ZPopMin(const Slice& key, ScoredMember* scoredMember) final;
  Status ZRemRangeByRank(const Slice& key, int64_t minRank, int64_t maxRank, uint64_t* count) final;
  Status ZRemRangeByScore(const Slice& key, int64_t minScore, int64_t maxScore, uint64_t* count) final;
  Status ZRemRangeByLex(const Slice& key, const Slice& minLex, const Slice& maxLex, uint64_t* count) final;

  Status ZUnion(const std::vector<Slice>& keys, Members* members) final;
  Status ZInter(const std::vector<Slice>& keys, Members* members) final;
  Status ZDiff(const std::vector<Slice>& keys, Members* members) final;
  Status ZUnionWithScores(const std::vector<Slice>& keys, ScoredMembers* scoredMembers) final;
  Status ZInterWithScores(const std::vector<Slice>& keys, ScoredMembers* scoredMembers) final;
  Status ZDiffWithScores(const std::vector<Slice>& keys, ScoredMembers* scoredMembers) final;
  Status ZUnionStore(const std::vector<Slice>& keys, const Slice& dstKey, uint64_t* count) final;
  Status ZInterStore(const std::vector<Slice>& keys, const Slice& dstKey, uint64_t* count) final;
  Status ZDiffStore(const std::vector<Slice>& keys, const Slice& dstKey, uint64_t* count) final;

private:
  Status ZRankInternal(const Slice& key, const Slice& member, uint64_t* rank, bool rev);
};

}

#endif //MERODIS_REDIS_ZSET_BASIC_IMPL_H
