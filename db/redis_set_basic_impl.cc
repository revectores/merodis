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

Status RedisSetBasicImpl::SMIsMember(const Slice& key,
                                     const std::set<Slice>& keys,
                                     std::vector<bool>* isMembers) {
  Iterator* iter = db_->NewIterator(ReadOptions());
  iter->Seek(key);
  iter->Next();
  std::set<Slice>::const_iterator queryIter = keys.cbegin();
  while (iter->Valid() && queryIter != keys.cend()) {
    int cmp = queryIter->compare({iter->key().data() + key.size() + 1,
                                  iter->key().size() - key.size() - 1});
    if (cmp == 0) {
      ++queryIter;
      iter->Next();
      isMembers->push_back(true);
    } else if (cmp > 0) {
      iter->Next();
    } else {
      ++queryIter;
      isMembers->push_back(false);
    }
  }
  if (queryIter != keys.cend()) {
    std::vector<bool> falseVector(std::distance(queryIter, keys.cend()), false);
    isMembers->insert(isMembers->end(), falseVector.begin(), falseVector.end());
  }
  delete iter;
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
  std::string rawSetMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawSetMetaValue);
  SetMetaValue metaValue;
  if (s.ok()) metaValue = SetMetaValue(rawSetMetaValue);
  WriteBatch updates;

  return Status::OK();
}

uint64_t RedisSetBasicImpl::CountKeyIntersection(const Slice& key, const SetNodeKey& nodeKey) {
  std::string _;
  Status s = db_->Get(ReadOptions(), nodeKey.Encode(), &_);
  return s.ok();
}


}