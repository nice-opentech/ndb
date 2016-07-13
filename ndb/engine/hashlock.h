#ifndef NDB_ENGINE_HASHLOCK_H_
#define NDB_ENGINE_HASHLOCK_H_

#include "ndb/common/encode.h"
#include "ndb/engine/common.h"

namespace ndb {

class Lock {
 public:
  void lock() {
    for (auto lock : locks_) {
      lock->lock();
    }
  }

  void unlock() {
    for (auto lock : locks_) {
      lock->unlock();
    }
  }

  void addlock(std::mutex* l) {
    locks_.insert(l);
  }

 private:
  std::set<std::mutex*> locks_;
};

class AutoLock {
 public:
  AutoLock(Lock&& l) : lock_(std::move(l)) {
    lock_.lock();
  }

  ~AutoLock() {
    lock_.unlock();
  }

 private:
  Lock lock_;
};

class HashLock {
 public:
  HashLock(size_t size = 1000000) : locks_(size) {}

  Lock GetLock(const Slice& s) {
    Lock lock;
    auto hash = BKDRHash(s.data(), s.size());
    lock.addlock(&locks_[hash % locks_.size()]);
    return lock;
  }

  Lock GetLock(const std::vector<Slice>& ss) {
    Lock lock;
    for (auto s : ss) {
      auto hash = BKDRHash(s.data(), s.size());
      lock.addlock(&locks_[hash % locks_.size()]);
    }
    return lock;
  }

 private:
  std::vector<std::mutex> locks_;
};

}  // namespace ndb

#endif /* NDB_ENGINE_HASHLOCK_H_ */
