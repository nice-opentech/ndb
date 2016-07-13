#include "ndb/engine/namespace.h"

namespace ndb {

using namespace rocksdb;

Result Namespace::PutConfigs(const Configs& configs) {
  std::string v;
  if (!configs.SerializeToString(&v)) {
    return ProtobufError();
  }
  auto s = db_->Put(wopts_, handle_, "namespace.__configs__", v);
  if (s.ok()) {
    configs_ = configs;
  }
  return StatusToResult(s);
}

Result Namespace::LoadConfigs() {
  std::string v;
  auto s = db_->Get(ropts_, handle_, "namespace.__configs__", &v);
  if (s.IsNotFound()) {
    return Result::OK();
  }
  if (s.ok()) {
    if (!configs_.ParseFromString(v)) {
      return ProtobufError();
    }
  }
  return StatusToResult(s);
}

Result Namespace::Put(const Slice& id, const Value& value) {
  auto s = db_->Put(wopts_, handle_, id, value.Encode());
  return StatusToResult(s);
}

Result Namespace::Delete(const Slice& id) {
  auto s = db_->Delete(wopts_, handle_, id);
  return StatusToResult(s);
}

Result Namespace::Get(const Slice& id, Value* value) {
  std::string v;
  auto s = db_->Get(ropts_, handle_, id, &v);
  if (s.ok()) {
    if (value != NULL) {
      return value->Decode(v);
    } else {
      // We need to decode the value to know if it is expired or deleted.
      Value tmp;
      return tmp.Decode(v);
    }
  }
  return StatusToResult(s);
}

std::vector<Result> Namespace::MultiGet(const std::vector<Slice>& ids,
                                        std::vector<Value>* values) {
  auto size = ids.size();
  std::vector<std::string> vs;
  std::vector<Value> tmpvals(size);
  std::vector<Result> results(size);
  std::vector<ColumnFamilyHandle*> handles(size, handle_);

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

std::unique_ptr<RangeIterator> Namespace::RangeGet(const Slice& begin,
                                                   const Slice& end,
                                                   size_t offset,
                                                   size_t count,
                                                   bool reverse) {
  std::unique_ptr<RangeIterator> it;
  auto dbit = db_->NewIterator(ropts_, handle_);
  if (!reverse) {
    it.reset(new ForwardIterator(dbit, begin, end, offset, count));
  } else {
    it.reset(new BackwardIterator(dbit, begin, end, offset, count));
  }
  return it;
}

Result Namespace::CompactRange(const Slice& begin, const Slice& end) {
  auto btmp = begin.size() == 0 ? NULL : &begin;
  auto etmp = end.size() == 0 ? NULL : &end;
  auto s = db_->CompactRange(CompactRangeOptions(), handle_, btmp, etmp);
  return StatusToResult(s);
}

Stats Namespace::GetStats() const {
  Stats stats;

  ColumnFamilyMetaData meta;
  db_->GetColumnFamilyMetaData(handle_, &meta);
  stats.insert("name", meta.name);
  stats.insert("size", meta.size);
  stats.insert("file_count", meta.file_count);
  stats.insert("num_levels", db_->NumberLevels(handle_));

  uint64_t value = 0, value1 = 0, value2 = 0;
  db_->GetIntProperty(handle_, "rocksdb.estimate-num-keys", &value);
  stats.insert("estimate_num_keys", value);
  db_->GetIntProperty(handle_, "rocksdb.estimate-live-data-size", &value);
  stats.insert("estimate_data_size", value);
  db_->GetIntProperty(handle_, "rocksdb.size-all-mem-tables", &value);
  stats.insert("memtables_size", value);
  db_->GetIntProperty(handle_, "rocksdb.num-entries-imm-mem-tables", &value1);
  db_->GetIntProperty(handle_, "rocksdb.num-entries-active-mem-table", &value2);
  stats.insert("memtables_num_entries", value1 + value2);
  db_->GetIntProperty(handle_, "rocksdb.num-deletes-imm-mem-tables", &value1);
  db_->GetIntProperty(handle_, "rocksdb.num-deletes-active-mem-table", &value2);
  stats.insert("memtables_num_deletes", value1 + value2);
  db_->GetIntProperty(handle_, "rocksdb.num-running-flushes", &value);
  stats.insert("num_running_flushes", value);
  db_->GetIntProperty(handle_, "rocksdb.num-running-compactions", &value);
  stats.insert("num_running_compactions", value);

  return stats;
}

}  // namespace ndb
