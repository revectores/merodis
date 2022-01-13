#ifndef MERODIS_REDIS_HASH_H
#define MERODIS_REDIS_HASH_H

#include "redis.h"
#include "util/coding.h"

namespace merodis {

class RedisHash : public Redis {
public:
  RedisHash() noexcept = default;
  ~RedisHash() noexcept override = default;

  virtual Status HLen(const Slice& key, uint64_t* len) = 0;
  virtual Status HGet(const Slice& key, const Slice& hashKey, std::string* value) = 0;
  virtual Status HMGet(const Slice& key, const std::vector<Slice>& hashKeys, std::vector<std::string>* values) = 0;
  virtual Status HGetAll(const Slice& key, std::map<std::string, std::string>* kvs) = 0;
  virtual Status HKeys(const Slice& key, std::vector<std::string>* keys) = 0;
  virtual Status HVals(const Slice& key, std::vector<std::string>* values) = 0;
  virtual Status HExists(const Slice& key, const Slice& hashKey, bool* exists) = 0;
  virtual Status HSet(const Slice& key, const Slice& hashKey, const Slice& value, uint64_t* count) = 0;
  virtual Status HSet(const Slice& key, const std::map<Slice, Slice>& kvs, uint64_t* count) = 0;
  virtual Status HDel(const Slice& key, const Slice& hashKey, uint64_t* count) = 0;
  virtual Status HDel(const Slice& key, const std::set<Slice>& hashKeys, uint64_t* count) = 0;
};

}


#endif //MERODIS_REDIS_HASH_H
