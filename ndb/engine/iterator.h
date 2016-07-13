#ifndef NDB_ENGINE_ITERATOR_H_
#define NDB_ENGINE_ITERATOR_H_

#include "ndb/engine/common.h"

namespace ndb {

class WALIterator {
 public:
  WALIterator(rocksdb::DB* db) : db_(db) {}

  void Seek(uint64_t sequence) {
    if (it_ != NULL) {
      it_->Next();
      if (!it_->Valid()) {
        if (it_->status().ok()) {
          // No more data.
        } else {
          // Switch to another wal.
          it_.reset();
        }
      }
    }
    if (it_ == NULL) {
      status_ = db_->GetUpdatesSince(sequence, &it_);
    }
  }

  void Next() {
    it_->Next();
  }

  bool Valid() const {
    if (!result().ok()) {
      return false;
    }
    return it_->Valid();
  }

  Result result() const {
    auto s = status_;
    if (s.ok()) {
      s = it_->status();
    }
    return StatusToResult(s);
  }

  rocksdb::BatchResult batch() {
    return it_->GetBatch();
  }

 private:
  Status status_;
  rocksdb::DB* db_ {NULL};
  std::unique_ptr<rocksdb::TransactionLogIterator> it_;
};

class RangeIterator {
 public:
  // Callee take ownership of iterator.
  RangeIterator(rocksdb::Iterator* it) : it_(it) {}

  virtual ~RangeIterator() { delete it_; }

  virtual void Seek() = 0;

  virtual void Next() = 0;

  bool Valid() const { return valid_ && it_->Valid(); }

  Slice id() const { return it_->key(); }

  Slice value() const { return it_->value(); }

  Result result() const { return StatusToResult(it_->status()); }

 protected:
  bool valid_ = {true};
  rocksdb::Iterator* it_ {NULL};
};

class ForwardIterator : public RangeIterator {
 public:
  ForwardIterator(rocksdb::Iterator* it,
                  const Slice begin,
                  const Slice end,
                  size_t offset,
                  size_t limit)
      : RangeIterator(it),
        begin_(begin),
        end_(end),
        offset_(offset),
        limit_(limit) {}

  void Seek() {
    // Seek
    if (begin_.size() == 0) {
      it_->SeekToFirst();
    } else {
      it_->Seek(begin_);
    }
    // Skip offset
    for (; it_->Valid() && offset_ != 0; it_->Next(), offset_--);
    count_ = 1;
    CheckRange();
  }

  void Next() {
    // Check limit
    count_++;
    if (limit_ > 0 && count_ > limit_) {
      valid_ = false;
      return;
    }
    it_->Next();
    CheckRange();
  }

 private:
  void CheckRange() {
    if (it_->Valid()) {
      if (end_.size() != 0 && it_->key().compare(end_) > 0) {
        valid_ = false;
      }
    }
  }

 private:
  Slice begin_;
  Slice end_;
  size_t offset_ {0};
  size_t limit_ {0};
  size_t count_ {0};
};

class BackwardIterator : public RangeIterator {
 public:
  BackwardIterator(rocksdb::Iterator* it,
                   const Slice begin,
                   const Slice end,
                   size_t offset,
                   size_t limit)
      : RangeIterator(it),
        begin_(begin),
        end_(end),
        offset_(offset),
        limit_(limit) {}

  void Seek() {
    // Seek
    if (end_.size() == 0) {
      it_->SeekToLast();
    } else {
      it_->Seek(end_);
      if (it_->Valid()) {
        if (it_->key().compare(end_) > 0) {
          it_->Prev();
        }
      } else {
        it_->SeekToLast();
      }
    }
    // Skip offset
    for (; it_->Valid() && offset_ != 0; it_->Prev(), offset_--);
    count_ = 1;
    CheckRange();
  }

  void Next() {
    // Check limit
    count_++;
    if (limit_ > 0 && count_ > limit_) {
      valid_ = false;
      return;
    }
    it_->Prev();
    CheckRange();
  }

 private:
  void CheckRange() {
    if (it_->Valid()) {
      if (begin_.size() != 0 && it_->key().compare(begin_) < 0) {
        valid_ = false;
      }
    }
  }

 private:
  Slice begin_;
  Slice end_;
  size_t offset_ {0};
  size_t limit_ {0};
  size_t count_ {0};
};

}  // namespace ndb

#endif /* NDB_ENGINE_ITERATOR_H_ */
