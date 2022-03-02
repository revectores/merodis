#ifndef MERODIS_REDIS_SET_H
#define MERODIS_REDIS_SET_H

#include "redis.h"
#include "util/coding.h"

namespace merodis {

class RedisSet : public Redis {
public:
  RedisSet() noexcept = default;
  ~RedisSet() noexcept override = default;

  virtual Status SCard(const Slice& key, uint64_t* len) = 0;
  virtual Status SIsMember(const Slice& key, const Slice& setKey, bool* isMember) = 0;
  virtual Status SMIsMember(const Slice& key, const std::set<Slice>& keys, std::vector<bool>* isMembers) = 0;
  virtual Status SMembers(const Slice& key, std::vector<std::string>* keys) = 0;
  virtual Status SRandMember(const Slice& key, std::string* member) = 0;
  virtual Status SRandMember(const Slice& key, int64_t count, std::vector<std::string>* members) = 0;
  virtual Status SAdd(const Slice& key, const Slice& setKey, uint64_t* count) = 0;
  virtual Status SAdd(const Slice& key, const std::set<Slice>& setKey, uint64_t* count) = 0;
};

}

#endif //MERODIS_REDIS_SET_H
