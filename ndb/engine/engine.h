#ifndef NDB_ENGINE_ENGINE_H_
#define NDB_ENGINE_ENGINE_H_

#include "ndb/engine/backup.h"
#include "ndb/engine/encode.h"
#include "ndb/engine/namespace.h"

namespace ndb {

class Engine {
 public:
  struct Options {
    std::string dbname {"nicedb"};
    int max_open_files {-1};
    int WAL_ttl_seconds {3600};
    size_t WAL_size_limit {1 << 30};
    size_t memtable_size {1 << 30};
    size_t block_cache_size {1 << 30};
    size_t compaction_cache_size {1 << 20};
    int background_threads {4};
  };

  Engine(const Options& options);

  ~Engine();

  Result Open();

  std::vector<std::string> ListNamespaces();

  NSRef GetNamespace(const std::string& nsname);

  Result NewNamespace(const std::string& nsname);

  Result DropNamespace(const std::string& nsname);

  // Get multiple values from different namespaces.
  std::vector<Result> MultiGet(const std::vector<NSRef>& namespaces,
                               const std::vector<Slice>& ids,
                               std::vector<Value>* values);
  std::vector<Result> MultiGet(const std::vector<NSRef>& namespaces,
                               const std::vector<std::string>& ids,
                               std::vector<Value>* values) {
    std::vector<Slice> tmpids {ids.begin(), ids.end()};
    return MultiGet(namespaces, tmpids, values);
  }

  Stats GetStats() const;

  Backup* GetBackup() { return backup_; }

  rocksdb::DB* GetRocksDB() { return db_; }

  std::unique_ptr<WALIterator> NewWALIterator() {
    return std::unique_ptr<WALIterator>(new WALIterator(db_));
  }

 private:
  friend class Batch;

  Options options_;
  std::mutex lock_;
  std::string dbname_;
  rocksdb::DBOptions dbopts_;
  rocksdb::ReadOptions ropts_;
  rocksdb::WriteOptions wopts_;
  rocksdb::ColumnFamilyOptions cfopts_;
  rocksdb::BlockBasedTableOptions tbopts_;
  rocksdb::DB* db_ {NULL};
  Backup* backup_ {NULL};
  std::map<std::string, NSRef> namespaces_;
};

class Batch {
 public:
  Batch(Engine* engine) : engine_(engine) {}

  void Put(NSRef ns, const Slice& id, const Value& value) {
    batch_.Put(ns->handle_, id, value.Encode());
  }

  void Put(NSRef ns, const Slice& id, const Slice& value = Slice()) {
    batch_.Put(ns->handle_, id, value);
  }

  void Delete(NSRef ns, const Slice& id) {
    batch_.Delete(ns->handle_, id);
  }

  Result Commit() {
    auto s = engine_->db_->Write(engine_->wopts_, &batch_);
    return StatusToResult(s);
  }

  size_t GetDataSize() const { return batch_.GetDataSize(); }

 private:
  Engine* engine_ {NULL};
  rocksdb::WriteBatch batch_;
};

}  // namespace ndb

#endif /* NDB_ENGINE_ENGINE_H_ */
