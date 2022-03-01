#include "redis_set_basic_impl.h"

#include "leveldb/db.h"
#include "leveldb/write_batch.h"

namespace merodis {

RedisSetBasicImpl::RedisSetBasicImpl() noexcept = default;

RedisSetBasicImpl::~RedisSetBasicImpl() noexcept = default;

Status RedisSetBasicImpl::SCard(const Slice& key,
                                uint64_t* len) {
  std::string rawSetMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawSetMetaValue);
  if (s.ok()) {
    SetMetaValue metaValue(rawSetMetaValue);
    *len = metaValue.len;
  } else if (s.IsNotFound()) {
    *len = 0;
    s = Status::OK();
  }
  return s;
}

Status RedisSetBasicImpl::SIsMember(const Slice& key,
                                    const Slice& setKey,
                                    bool* isMember) {
  std::string _;
  SetNodeKey nodeKey(key, setKey);
  *isMember = db_->Get(ReadOptions(), nodeKey.Encode(), &_).ok();
  return Status::OK();
}

Status RedisSetBasicImpl::SAdd(const Slice& key,
                               const Slice& setKey,
                               uint64_t* count) {
  std::string rawSetMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawSetMetaValue);
  SetMetaValue metaValue;
  if (s.ok()) metaValue = SetMetaValue(rawSetMetaValue);

  WriteBatch updates;
  SetNodeKey nodeKey(key, setKey);

  *count = 1 - CountKeyIntersection(key, nodeKey);
  if (*count == 0) return Status::OK();
  metaValue.len += 1;
  updates.Put(key, metaValue.Encode());
  updates.Put(nodeKey.Encode(), "");
  return db_->Write(WriteOptions(), &updates);
}

Status RedisSetBasicImpl::SAdd(const Slice& key,
                               const std::vector<Slice>& setKey,
                               uint64_t* count) {
  return Status::NotSupported("");
}

uint64_t RedisSetBasicImpl::CountKeyIntersection(const Slice& key, const SetNodeKey& nodeKey) {
  std::string _;
  Status s = db_->Get(ReadOptions(), nodeKey.Encode(), &_);
  return s.ok();
}

}