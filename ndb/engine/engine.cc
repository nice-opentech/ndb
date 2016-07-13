#include "ndb/engine/engine.h"
#include "ndb/engine/compaction.h"

namespace ndb {

using namespace rocksdb;

Engine::Engine(const Options& options) : options_(options) {
  dbopts_.create_if_missing = true;
  dbopts_.max_open_files = options.max_open_files;
  dbopts_.max_file_opening_threads = 4;
  dbopts_.keep_log_file_num = 4;
  dbopts_.WAL_ttl_seconds = options.WAL_ttl_seconds;
  dbopts_.WAL_size_limit_MB = options.WAL_size_limit / 1024 / 1024;
  dbopts_.max_total_wal_size = options.memtable_size;
  dbopts_.db_write_buffer_size = options.memtable_size;
  dbopts_.statistics = rocksdb::CreateDBStatistics();
  dbopts_.IncreaseParallelism(options.background_threads);

  tbopts_.filter_policy.reset(NewBloomFilterPolicy(10));
  tbopts_.block_cache = NewLRUCache(options.block_cache_size);
  // TODO: Consider the following options.
  // tbopts_.format_version = 3;

  cfopts_.OptimizeLevelStyleCompaction(options.memtable_size);
  cfopts_.table_factory.reset(NewBlockBasedTableFactory(tbopts_));
  // TODO: Consider the following options.
  // cfopts.inplace_update_support
  // cfopts.inplace_update_num_locks
}

Engine::~Engine() {
  for (auto& ns : namespaces_) { ns.second.reset(); }
  delete backup_;
  delete db_;
}

Result Engine::Open() {
  std::vector<std::string> cfnames;
  auto s = rocksdb::Env::Default()->FileExists(options_.dbname + "/" + "CURRENT");
  if (s.ok()) {
    s = DB::ListColumnFamilies(dbopts_, options_.dbname, &cfnames);
    if (!s.ok()) return StatusToResult(s);
  } else {
    cfnames.push_back("default");
  }

  std::vector<ColumnFamilyDescriptor> cfds;
  for (auto name : cfnames) {
    cfopts_.compaction_filter_factory.reset(
        new CompactionFilterFactory(this, name, options_.compaction_cache_size));
    cfds.emplace_back(name, cfopts_);
  }

  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  s = DB::Open(dbopts_, options_.dbname, cfds, &handles, &db_);
  if (!s.ok()) return StatusToResult(s);

  backup_ = new Backup(db_);

  // Init namespaces.
  std::unique_lock<std::mutex> lock(lock_);
  for (auto handle: handles) {
    NSRef ns(new Namespace(db_, handle));
    NDB_TRY(ns->LoadConfigs());
    namespaces_[handle->GetName()] = ns;
  }

  return Result::OK();
}

std::vector<std::string> Engine::ListNamespaces() {
  std::unique_lock<std::mutex> lock(lock_);
  std::vector<std::string> names;
  for (const auto& it : namespaces_) {
    names.push_back(it.first);
  }
  return names;
}

NSRef Engine::GetNamespace(const std::string& nsname) {
  std::unique_lock<std::mutex> lock(lock_);
  auto it = namespaces_.find(nsname);
  if (it == namespaces_.end()) {
    return NULL;
  }
  return it->second;
}

Result Engine::NewNamespace(const std::string& nsname) {
  std::unique_lock<std::mutex> lock(lock_);
  auto it = namespaces_.find(nsname);
  if (it != namespaces_.end()) {
    return Result::Error("namespace:%s has exist", nsname.c_str());
  }
  ColumnFamilyHandle* handle = NULL;
  cfopts_.compaction_filter_factory.reset(
      new CompactionFilterFactory(this, nsname, options_.compaction_cache_size));
  auto s = db_->CreateColumnFamily(cfopts_, nsname, &handle);
  if (s.ok()) {
    namespaces_[handle->GetName()].reset(new Namespace(db_, handle));
  }
  return StatusToResult(s);
}

Result Engine::DropNamespace(const std::string& nsname) {
  std::unique_lock<std::mutex> lock(lock_);
  auto it = namespaces_.find(nsname);
  if (it == namespaces_.end()) {
    return Result::OK();
  }
  auto s = db_->DropColumnFamily(it->second->handle_);
  if (s.ok()) {
    namespaces_.erase(it);
  }
  return StatusToResult(s);
}

std::vector<Result> Engine::MultiGet(const std::vector<NSRef>& namespaces,
                                     const std::vector<Slice>& ids,
                                     std::vector<Value>* values) {
  auto size = ids.size();
  std::vector<std::string> vs;
  std::vector<Value> tmpvals(size);
  std::vector<Result> results(size);
  std::vector<ColumnFamilyHandle*> handles;
  for (auto ns : namespaces) handles.push_back(ns->handle_);

  auto ss = db_->MultiGet(ropts_, handles, ids, &vs);
  for (size_t i = 0; i < size; i++) {
    auto& v = tmpvals[i];
    auto& r = results[i];
    r = StatusToResult(ss[i]);
    if (r.ok()) {
      r = v.Decode(vs[i]);
    }
  }

  if (values != NULL) {
    values->swap(tmpvals);
  }
  return results;
}

Stats Engine::GetStats() const {
  Stats stats;
  stats.insert("dbname", db_->GetName());
  stats.insert("sequence", db_->GetLatestSequenceNumber());

  uint64_t value = 0, value1 = 0, value2 = 0;
  db_->GetAggregatedIntProperty("rocksdb.estimate-num-keys", &value);
  stats.insert("estimate_num_keys", value);
  db_->GetAggregatedIntProperty("rocksdb.estimate-live-data-size", &value);
  stats.insert("estimate_data_size", value);
  db_->GetAggregatedIntProperty("rocksdb.size-all-mem-tables", &value);
  stats.insert("memtables_size", value);
  db_->GetAggregatedIntProperty("rocksdb.num-entries-imm-mem-tables", &value1);
  db_->GetAggregatedIntProperty("rocksdb.num-entries-active-mem-table", &value2);
  stats.insert("memtables_num_entries", value1 + value2);
  db_->GetAggregatedIntProperty("rocksdb.num-deletes-imm-mem-tables", &value1);
  db_->GetAggregatedIntProperty("rocksdb.num-deletes-active-mem-table", &value2);
  stats.insert("memtables_num_deletes", value1 + value2);
  db_->GetAggregatedIntProperty("rocksdb.num-running-flushes", &value);
  stats.insert("num_running_flushes", value);
  db_->GetAggregatedIntProperty("rocksdb.num-running-compactions", &value);
  stats.insert("num_running_compactions", value);

  return stats;
}

}  // namespace ndb
