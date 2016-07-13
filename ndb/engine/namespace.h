#ifndef NDB_ENGINE_NAMESPACE_H_
#define NDB_ENGINE_NAMESPACE_H_

#include "ndb/engine/common.h"
#include "ndb/engine/iterator.h"
#include "ndb/engine/value.h"

namespace ndb {

class Namespace {
 public:
  // Callee take ownership of handle.
  Namespace(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* handle)
      : db_(db), handle_(handle) {}

  ~Namespace() { delete handle_; }

  const std::string GetName() const { return handle_->GetName(); }

  const Configs& GetConfigs() const { return configs_; }

  Result PutConfigs(const Configs& configs);

  Result LoadConfigs();

  Result Put(const Slice& id, const Value& value);

  Result Delete(const Slice& id);

  Result Get(const Slice& id, Value* value);

  std::vector<Result> MultiGet(const std::vector<Slice>& ids,
                               std::vector<Value>* values);
  std::vector<Result> MultiGet(const std::vector<std::string>& ids,
                               std::vector<Value>* values) {
    return MultiGet(std::vector<Slice>{ids.begin(), ids.end()}, values);
  }

  std::unique_ptr<RangeIterator> RangeGet(const Slice& begin = Slice(),
                                          const Slice& end = Slice(),
                                          size_t offset = 0,
                                          size_t limit = 0,
                                          bool reverse = false);

  Result CompactRange(const Slice& begin = Slice(), const Slice& end = Slice());

  Stats GetStats() const;

 private:
  friend class Engine;
  friend class Batch;
  friend class NSBatch;

  Configs configs_;
  rocksdb::ReadOptions ropts_;
  rocksdb::WriteOptions wopts_;
  rocksdb::DB* db_ {NULL};
  rocksdb::ColumnFamilyHandle* handle_ {NULL};
};

typedef std::shared_ptr<Namespace> NSRef;

class NSBatch {
 public:
  NSBatch(NSRef ns) : ns_(ns) {}

  void Put(const Slice& id, const Value& value) {
    batch_.Put(ns_->handle_, id, value.Encode());
  }

  void Put(const Slice& id, const Slice& value = Slice()) {
    batch_.Put(ns_->handle_, id, value);
  }

  void Delete(const Slice& id) {
    batch_.Delete(ns_->handle_, id);
  }

  Result Commit() {
    auto s = ns_->db_->Write(ns_->wopts_, &batch_);
    return StatusToResult(s);
  }

  size_t GetDataSize() const { return batch_.GetDataSize(); }

 private:
  NSRef ns_;
  rocksdb::WriteBatch batch_;
};

}  // namespace ndb

#endif /* NDB_ENGINE_DB_H_ */
