#include "redis_set_basic_impl.h"

#include <cstdint>
#include <string>
#include <vector>
#include <queue>
#include <map>
#include <set>
#include <algorithm>

#include "util/random.h"

namespace merodis {

RedisSetBasicImpl::RedisSetBasicImpl() noexcept = default;

//RedisSetBasicImpl::RedisSetBasicImpl(bool MemoryMeta) noexcept: MemoryMeta(MemoryMeta), len(0) {}
//
RedisSetBasicImpl::~RedisSetBasicImpl() noexcept = default;
//
//Status RedisSetBasicImpl::Open(const Options& options, const std::string& db_path) noexcept {
//  Status s = Redis::Open(options, db_path);
//  if(!s.ok()) return s;
//  ReloadLens();
//}
//
Status RedisSetBasicImpl::Del(const Slice& key) {
  Iterator* iter = db_->NewIterator(ReadOptions());
  iter->Seek(key);
  if (!iter->Valid() || iter->key() != key) {
    delete iter;
    return Status::OK();
  }

  WriteBatch updates;
  updates.Delete(iter->key());
  for (iter->Next(); iter->Valid() && IsMemberKey(iter->key(), key.size()); iter->Next()) {
    updates.Delete(iter->key());
  }
  delete iter;
  return db_->Write(WriteOptions(), &updates);
}

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

Status RedisSetBasicImpl::SMembers(const Slice& key,
                                   std::vector<std::string>* keys) {
  Iterator* iter = db_->NewIterator(ReadOptions());
  iter->Seek(key);
  if (!iter->Valid() || iter->key() != key) {
    delete iter;
    return Status::OK();
  };
  iter->Next();
  for (; iter->Valid(); iter->Next()) {
    if (iter->key().size() <= key.size() || iter->key()[key.size()] != 0) break;
    SetNodeKey nodeKey(iter->key(), key.size());
    keys->push_back(nodeKey.setKey().ToString());
  }
  delete iter;
  return Status::OK();
}

Status RedisSetBasicImpl::SRandMember(const Slice& key,
                                      std::string* member) {
  std::string rawSetMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawSetMetaValue);
  if (!s.ok()) return s;
  SetMetaValue metaValue = SetMetaValue(rawSetMetaValue);

  if (metaValue.len == 0) return Status::NotFound("empty set");
  uint64_t index = rand_uint64(0, metaValue.len - 1);

  Iterator* iter = db_->NewIterator(ReadOptions());
  iter->Seek(key);
  iter->Next();
  while (index--) {
    assert(iter->Valid());
    iter->Next();
  }
  SetNodeKey nodeKey(iter->key(), key.size());
  *member = nodeKey.setKey().ToString();
  delete iter;
  return Status::OK();
}

Status RedisSetBasicImpl::SRandMember(const Slice& key,
                                      int64_t count,
                                      std::vector<std::string>* members) {
  std::string rawSetMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawSetMetaValue);
  if (!s.ok()) return s;
  SetMetaValue metaValue = SetMetaValue(rawSetMetaValue);

  if (count == 0) return Status::OK();
  std::vector<std::uint64_t> indices;
  std::vector<std::uint64_t> offsets(abs(count));
  if (count > 0) {
    indices = std::vector<std::uint64_t>(metaValue.len);
    if (count > metaValue.len) offsets.resize(metaValue.len);
    std::iota(indices.begin(), indices.end(), 0);
    std::shuffle(indices.begin(), indices.end(), std::mt19937{std::random_device{}()});
    std::sort(indices.begin(), indices.begin() + (ptrdiff_t)offsets.size());
  } else {
    indices = rand_uint64(0, metaValue.len - 1, abs(count));
    std::sort(indices.begin(), indices.end());
  }
  offsets[0] = indices[0];
  for (int c = 1; c < offsets.size(); c++) {
    offsets[c] = indices[c] - indices[c - 1];
  }

  Iterator* iter = db_->NewIterator(ReadOptions());
  iter->Seek(key);
  iter->Next();
  for (auto offset: offsets) {
    while (offset) {
      assert(iter->Valid());
      iter->Next();
      offset--;
    }
    SetNodeKey nodeKey(iter->key(), key.size());
    members->push_back(nodeKey.setKey().ToString());
  }
  delete iter;
  std::shuffle(members->begin(), members->end(), std::mt19937{std::random_device{}()});

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
                               const std::set<Slice>& keys,
                               uint64_t* count) {
  std::string rawSetMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawSetMetaValue);
  SetMetaValue metaValue;
  if (s.ok()) metaValue = SetMetaValue(rawSetMetaValue);
  WriteBatch updates;
  *count = 0;

  std::set<Slice>::const_iterator updatesIter = keys.cbegin();
  if (metaValue.len) {
    Iterator* iter = db_->NewIterator(ReadOptions());
    iter->Seek(key);
    iter->Next();
    while (iter->Valid() && updatesIter != keys.cend()) {
      int cmp = updatesIter->compare({iter->key().data() + key.size() + 1,
                                      iter->key().size() - key.size() - 1});
      if (cmp == 0) {
        ++updatesIter;
        iter->Next();
      } else if (cmp > 0) {
        iter->Next();
      } else {
        ++updatesIter;
        SetNodeKey nodeKey(key, *updatesIter);
        updates.Put(nodeKey.Encode(), "");
        *count += 1;
      }
    }
    delete iter;
  }
  while (updatesIter != keys.cend()) {
    SetNodeKey nodeKey(key, *updatesIter);
    updates.Put(nodeKey.Encode(), "");
    updatesIter++;
    *count += 1;
  }

  if (*count) {
    metaValue.len += *count;
    updates.Put(key, metaValue.Encode());
  }
  return db_->Write(WriteOptions(), &updates);
}

Status RedisSetBasicImpl::SRem(const Slice& key,
                               const Slice& member,
                               uint64_t* count) {
  std::string rawSetMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawSetMetaValue);
  if (!s.ok()) return s;
  SetMetaValue metaValue = SetMetaValue(rawSetMetaValue);

  SetNodeKey nodeKey(key, member);
  std::string _;
  s = db_->Get(ReadOptions(), nodeKey.Encode(), &_);
  if (!s.ok() && !s.IsNotFound()) return s;
  if (s.IsNotFound()) {
    *count = 0;
    return Status::OK();
  }

  WriteBatch updates;
  *count = 1;
  metaValue.len -= 1;
  updates.Put(key, metaValue.Encode());
  updates.Delete(nodeKey.Encode());
  return db_->Write(WriteOptions(), &updates);
}

Status RedisSetBasicImpl::SRem(const Slice& key,
                               const std::set<Slice>& members,
                               uint64_t* count) {
  std::string rawSetMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawSetMetaValue);
  if (!s.ok()) return s;
  SetMetaValue metaValue = SetMetaValue(rawSetMetaValue);

  WriteBatch updates;
  *count = 0;

  std::set<Slice>::const_iterator updatesIter = members.cbegin();
  Iterator *iter = db_->NewIterator(ReadOptions());
  iter->Seek(key);
  iter->Next();
  while (iter->Valid() && updatesIter != members.cend()) {
    int cmp = updatesIter->compare({iter->key().data() + key.size() + 1,
                                    iter->key().size() - key.size() - 1});
    if (cmp == 0) {
      SetNodeKey nodeKey(key, *updatesIter);
      updates.Delete(nodeKey.Encode());
      *count += 1;
      ++updatesIter;
      iter->Next();
    } else if (cmp > 0) {
      iter->Next();
    } else {
      ++updatesIter;
    }
  }
  delete iter;
  if (*count) {
    metaValue.len -= *count;
    updates.Put(key, metaValue.Encode());
  }
  return db_->Write(WriteOptions(), &updates);
}

Status RedisSetBasicImpl::SPop(const Slice& key,
                               std::string* member) {
  std::string rawSetMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawSetMetaValue);
  if (!s.ok()) return s;
  SetMetaValue metaValue = SetMetaValue(rawSetMetaValue);

  s = SRandMember(key, member);
  if (!s.ok()) return s;

  WriteBatch updates;
  metaValue.len -= 1;
  updates.Put(key, metaValue.Encode());
  SetNodeKey nodeKey(key, *member);
  updates.Delete(nodeKey.Encode());
  return db_->Write(WriteOptions(), &updates);
}

Status RedisSetBasicImpl::SPop(const Slice& key,
                               uint64_t count,
                               std::vector<std::string>* members) {
  std::string rawSetMetaValue;
  Status s = db_->Get(ReadOptions(), key, &rawSetMetaValue);
  if (!s.ok()) return s;
  SetMetaValue metaValue = SetMetaValue(rawSetMetaValue);

  s = SRandMember(key, (int64_t)count, members);
  if (!s.ok()) return s;

  WriteBatch updates;
  assert(metaValue.len >= members->size());
  metaValue.len -= members->size();
  updates.Put(key, metaValue.Encode());
  for (const auto& member: *members) {
    SetNodeKey nodeKey(key, member);
    updates.Delete(nodeKey.Encode());
  }
  return db_->Write(WriteOptions(), &updates);
}

Status RedisSetBasicImpl::SMove(const Slice& srcKey,
                                const Slice& dstKey,
                                const Slice& member,
                                uint64_t* count) {
  std::string rawSetMetaValue;
  Status s = db_->Get(ReadOptions(), srcKey, &rawSetMetaValue);
  if (!s.ok()) return s;
  SetMetaValue metaValue = SetMetaValue(rawSetMetaValue);
  SetNodeKey nodeKey(srcKey, member);
  std::string _;
  s = db_->Get(ReadOptions(), nodeKey.Encode(), &_);
  if (!s.ok() && !s.IsNotFound()) return s;
  if (s.IsNotFound()) {
    *count = 0;
    return Status::OK();
  }
  WriteBatch updates;
  *count = 1;
  metaValue.len -= 1;
  updates.Put(srcKey, metaValue.Encode());
  updates.Delete(nodeKey.Encode());

  s = db_->Get(ReadOptions(), dstKey, &rawSetMetaValue);
  if (s.ok()) metaValue = SetMetaValue(rawSetMetaValue);
  nodeKey = SetNodeKey(dstKey, member);
  if (!CountKeyIntersection(dstKey, nodeKey)) {
    metaValue.len += 1;
    updates.Put(dstKey, metaValue.Encode());
    updates.Put(nodeKey.Encode(), "");
  }
  return db_->Write(WriteOptions(), &updates);
}

Status RedisSetBasicImpl::SUnion(const std::vector<Slice>& keys, std::vector<std::string>* members) {
  std::map<Iterator*, Slice> iter2key;
  for (const auto& key: keys) {
    Iterator* iter = db_->NewIterator(ReadOptions());
    iter->Seek(key);
    if (iter->Valid() && iter->key() == key) {
      iter->Next();
      iter2key[iter] = key;
    } else {
      delete iter;
    }
  }
  SetIteratorComparator cmp(iter2key);
  std::priority_queue<Iterator*, std::vector<Iterator*>, decltype(cmp)> iters(cmp);
  for (const auto& [key, _]: iter2key) {
    iters.push(key);
  }

  while (!iters.empty()) {
    Iterator* top = iters.top();
    iters.pop();
    Slice topKey = iter2key[top];
    SetNodeKey nodeKey(top->key(), topKey.size());
    std::string newMember = nodeKey.setKey().ToString();
    if (members->empty() || members->back() != newMember) {
      members->push_back(newMember);
    }
    top->Next();
    if (top->Valid() && IsMemberKey(top->key(), topKey.size())) {
      iters.push(top);
    }
  }
  for (const auto& [iter, _]: iter2key) delete iter;
  return Status::OK();
}

Status RedisSetBasicImpl::SInter(const std::vector<Slice>& keys, std::vector<std::string>* members) {
  std::map<Iterator*, Slice> iter2key;
  for (const auto& key: keys) {
    Iterator* iter = db_->NewIterator(ReadOptions());
    iter->Seek(key);
    if (!iter->Valid() || iter->key() != key) {
      for (const auto& [k, _]: iter2key) {
        delete k;
      }
      delete iter;
      return Status::OK();
    }
    iter->Next();
    iter2key[iter] = key;
  }
  SetIteratorComparator cmp(iter2key);
  std::priority_queue<Iterator*, std::vector<Iterator*>, decltype(cmp)> iters(cmp);
  for (const auto& [key, _]: iter2key) {
    iters.push(key);
  }

  std::string currentMinMember;
  uint64_t minMemberCounter = 0;

  while (!iters.empty()) {
    Iterator* top = iters.top();
    iters.pop();
    Slice topKey = iter2key[top];
    SetNodeKey nodeKey(top->key(), topKey.size());
    std::string minMember = nodeKey.setKey().ToString();
    if (minMember == currentMinMember) {
      minMemberCounter += 1;
      if (minMemberCounter == keys.size()) {
        members->push_back(minMember);
      }
    } else {
      currentMinMember = minMember;
      minMemberCounter = 1;
    }

    top->Next();
    if (top->Valid() && IsMemberKey(top->key(), topKey.size())) {
      iters.push(top);
    }
  }
  for (const auto& [iter, _]: iter2key) delete iter;
  return Status::OK();
}

Status RedisSetBasicImpl::SDiff(const std::vector<Slice>& keys, std::vector<std::string>* members) {
  Iterator* baseIter = db_->NewIterator(ReadOptions());
  Slice baseKey = keys.front();
  baseIter->Seek(baseKey);
  if (!baseIter->Valid() || baseIter->key() != baseKey) {
    delete baseIter;
    return Status::OK();
  }
  baseIter->Next();

  std::map<Iterator*, Slice> iter2key;
  for (auto it = std::next(keys.begin()); it != keys.end(); it++) {
    Slice key = *it;
    Iterator* iter = db_->NewIterator(ReadOptions());
    iter->Seek(key);
    if (iter->Valid() && iter->key() == key) {
      iter->Next();
      iter2key[iter] = key;
    } else {
      delete iter;
    }
  }

  while (baseIter->Valid() && IsMemberKey(baseIter->key(), baseKey.size())) {
    bool save = true;
    Slice baseMember = GetMember(baseIter, baseKey.size());
    for (const auto& [iter, key]: iter2key) {
      SetNodeKey nodeKey(key, baseMember);
      iter->Seek(nodeKey.Encode());
      if (GetMember(iter, key.size()) == baseMember) {
        save = false;
        break;
      }
    }
    if (save) members->push_back(baseMember.ToString());
    baseIter->Next();
  }
  delete baseIter;
  for (const auto& [iter, _]: iter2key) {
    delete iter;
  }
  return Status::OK();
}

Status RedisSetBasicImpl::SUnionStore(const std::vector<Slice>& keys, const Slice& dstKey, uint64_t* count) {
  Status s;
  std::vector<std::string> members;
  s = SUnion(keys, &members);
  if (!s.ok()) return s;
  s = Del(dstKey);
  if (!s.ok()) return s;
  std::set<Slice> memberSet(members.begin(), members.end());
  return SAdd(dstKey, memberSet, count);
}

Status RedisSetBasicImpl::SInterStore(const std::vector<Slice>& keys, const Slice& dstKey, uint64_t* count) {
  Status s;
  std::vector<std::string> members;
  s = SInter(keys, &members);
  if (!s.ok()) return s;
  s = Del(dstKey);
  if (!s.ok()) return s;
  std::set<Slice> memberSet(members.begin(), members.end());
  return SAdd(dstKey, memberSet, count);
}

Status RedisSetBasicImpl::SDiffStore(const std::vector<Slice>& keys, const Slice& dstKey, uint64_t* count) {
  Status s;
  std::vector<std::string> members;
  s = SDiff(keys, &members);
  if (!s.ok()) return s;
  s = Del(dstKey);
  if (!s.ok()) return s;
  std::set<Slice> memberSet(members.begin(), members.end());
  return SAdd(dstKey, memberSet, count);
}


Slice RedisSetBasicImpl::GetMember(Iterator* iter, uint64_t keySize) {
  return Slice{iter->key().data() + keySize + 1, iter->key().size() - keySize - 1};
}

uint64_t RedisSetBasicImpl::CountKeyIntersection(const Slice& key, const SetNodeKey& nodeKey) {
  std::string _;
  Status s = db_->Get(ReadOptions(), nodeKey.Encode(), &_);
  return s.ok();
}

bool RedisSetBasicImpl::IsMemberKey(const Slice& iterKey, uint64_t keySize) {
  return iterKey.size() > keySize && iterKey[keySize] == 0;
}

//void RedisSetBasicImpl::ReloadLens() {
//  Iterator* iter = db_->NewIterator(ReadOptions());
//  lens.clear();
//  for (; iter->Valid(); iter->Next()) {
//    const char *c = iter->key().data();
//    for (; *c; c++);
//    size_t keySize = c - iter->key().data();
//    Slice key(iter->key().data(), keySize);
//    if (keySize < iter->key().size()) lens[key.ToString()] += 1;
//  }
//}
//
}