#ifndef MERODIS_MERODIS_H
#define MERODIS_MERODIS_H

#include <string>
#include <vector>
#include <map>
#include <set>

#include "leveldb/db.h"

namespace leveldb {

inline bool operator<(const Slice& a, const Slice& b) {
  const size_t min_len = (a.size() < b.size()) ? a.size() : b.size();
  int r = memcmp(a.data(), b.data(), min_len);
  return r == 0 ? a.size() < b.size() : r < 0;
}

}



namespace merodis {

typedef int64_t UserIndex;

enum BeforeOrAfter {
  kBefore,
  kAfter,
};
enum Side {
  kLeft,
  kRight,
};
enum StringImpl {
  kStringBasicImpl,
  kStringTypedImpl,
};
enum ListImpl {
  kListArrayImpl,
};
enum HashImpl {
  kHashBasicImpl,
};
enum SetImpl {
  kSetBasicImpl,
};

struct Options : public leveldb::Options {
  enum StringImpl string_impl = kStringTypedImpl;
  enum ListImpl list_impl = kListArrayImpl;
  enum HashImpl hash_impl = kHashBasicImpl;
  enum SetImpl set_impl = kSetBasicImpl;
};

using ReadOptions = leveldb::ReadOptions;
using WriteOptions = leveldb::WriteOptions;
using WriteBatch = leveldb::WriteBatch;
using Status = leveldb::Status;
using Slice = leveldb::Slice;
using Iterator = leveldb::Iterator;

class RedisString;
class RedisList;
class RedisHash;
class RedisSet;

class Merodis {
public:
  Merodis() noexcept;
  ~Merodis() noexcept;

  Status Open(const Options& options, const std::string& db_path) noexcept;
  static Status DestroyDB(const std::string& db_path, Options options) noexcept;

  // String Operators
  Status Get(const Slice& key, std::string* value) noexcept;
  Status Set(const Slice& key, const Slice& value) noexcept;
  Status Incr(const Slice& key, int64_t* result) noexcept;
  Status IncrBy(const Slice& key, int64_t increment, int64_t* result) noexcept;
  Status Decr(const Slice& key, int64_t* result) noexcept;
  Status DecrBy(const Slice& key, int64_t decrement, int64_t* result) noexcept;

  // List Operators
  Status LLen(const Slice& key, uint64_t* len) noexcept;
  Status LIndex(const Slice& key, UserIndex index, std::string* value) noexcept;
  Status LPos(const Slice& key, const Slice& value, int64_t rank, int64_t count, int64_t maxlen, std::vector<uint64_t>& indices) noexcept;
  Status LRange(const Slice& key, UserIndex from, UserIndex to, std::vector<std::string>* values) noexcept;
  Status LSet(const Slice& key, UserIndex index, const Slice& value) noexcept;
  Status LPush(const Slice& key, const Slice& value) noexcept;
  Status LPush(const Slice& key, const std::vector<Slice>& values) noexcept;
  Status LPushX(const Slice& key, const Slice& value) noexcept;
  Status LPushX(const Slice& key, const std::vector<Slice>& values) noexcept;
  Status LPop(const Slice& key, std::string* value) noexcept;
  Status LPop(const Slice& key, uint64_t count, std::vector<std::string>* values) noexcept;
  Status RPush(const Slice& key, const Slice& value) noexcept;
  Status RPush(const Slice& key, const std::vector<Slice>& values) noexcept;
  Status RPushX(const Slice& key, const Slice& value) noexcept;
  Status RPushX(const Slice& key, const std::vector<Slice>& values) noexcept;
  Status RPop(const Slice& key, std::string* value) noexcept;
  Status RPop(const Slice& key, uint64_t count, std::vector<std::string>* values) noexcept;
  Status LTrim(const Slice& key, UserIndex from, UserIndex to) noexcept;
  Status LInsert(const Slice& key, const BeforeOrAfter& beforeOrAfter, const Slice& pivotValue, const Slice& value) noexcept;
  Status LRem(const Slice& key, int64_t count, const Slice& value, uint64_t* removedCount) noexcept;
  Status LMove(const Slice& srcKey, const Slice& dstKey, enum Side srcSide, enum Side dstSide, std::string* value) noexcept;

  // Hash Operators
  Status HLen(const Slice& key, uint64_t* len);
  Status HGet(const Slice& key, const Slice& hashKey, std::string* value);
  Status HMGet(const Slice& key, const std::vector<Slice>& hashKeys, std::vector<std::string>* values);
  Status HGetAll(const Slice& key, std::map<std::string, std::string>* kvs);
  Status HKeys(const Slice& key, std::vector<std::string>* keys);
  Status HVals(const Slice& key, std::vector<std::string>* values);
  Status HExists(const Slice& key, const Slice& hashKey, bool* exists);
  Status HSet(const Slice& key, const Slice& hashKey, const Slice& value, uint64_t* count);
  Status HSet(const Slice& key, const std::map<Slice, Slice>& kvs, uint64_t* count);
  Status HDel(const Slice& key, const Slice& hashKey, uint64_t* count);
  Status HDel(const Slice& key, const std::set<Slice>& hashKeys, uint64_t* count);

  // Set Operators
  Status SCard(const Slice& key, uint64_t* len);
  Status SIsMember(const Slice& key, const Slice& setKey, bool* isMember);
  Status SMIsMember(const Slice& key, const std::set<Slice>& keys, std::vector<bool>* isMembers);
  Status SMembers(const Slice& key, std::vector<std::string>* keys);
  Status SAdd(const Slice& key, const Slice& setKey, uint64_t* count);
  Status SAdd(const Slice& key, const std::set<Slice>& keys, uint64_t* count);

private:
  RedisString* string_db_;
  RedisList* list_db_;
  RedisHash* hash_db_;
  RedisSet* set_db_;
};

}

#endif //MERODIS_MERODIS_H
