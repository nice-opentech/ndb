#ifndef NDB_ENGINE_COMPACTION_H_
#define NDB_ENGINE_COMPACTION_H_

#include "ndb/engine/engine.h"

namespace ndb {

struct CompactionMeta {
  bool     deleted;
  uint64_t version;
  CompactionMeta(bool d = false, uint64_t v = 0)
      : deleted(d), version(v) {
  }
  CompactionMeta(const CompactionMeta& meta)
      : deleted(meta.deleted), version(meta.version) {
  }
};

class CompactionMetaCache {
 public:
  CompactionMetaCache(size_t size) {
    cache_ = rocksdb::NewLRUCache(size);
  }

  bool Get(const Slice& id, CompactionMeta* meta);

  void Put(const Slice& id, const CompactionMeta& meta);

  void Delete(const Slice& id);

 private:
  std::shared_ptr<rocksdb::Cache> cache_;
};

class CompactionFilter : public rocksdb::CompactionFilter {
 public:
  CompactionFilter(NSRef ns, std::shared_ptr<CompactionMetaCache> cache)
      : ns_(ns), cache_(cache) {}

  bool Filter(int level,
              const Slice& id,
              const Slice& existing_value,
              std::string* new_value,
              bool* value_changed) const override;

  const char* Name() const override { return "NDBCompactionFilter"; }

 private:
  Result GetMeta(const Slice& kmeta, CompactionMeta* vmeta) const;

  bool FilterMeta(const Slice& kmeta, const Value& vmeta) const;
  bool FilterMember(const Slice& id) const;

 private:
  NSRef ns_;
  std::shared_ptr<CompactionMetaCache> cache_;
};

class CompactionFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  CompactionFilterFactory(Engine* engine, const std::string& nsname, size_t cache_size)
      : engine_(engine), nsname_(nsname) {
    cache_.reset(new CompactionMetaCache(cache_size));
  }

  std::unique_ptr<rocksdb::CompactionFilter>
  CreateCompactionFilter(const rocksdb::CompactionFilter::Context& context) override {
    std::unique_ptr<rocksdb::CompactionFilter> filter;
    auto ns = engine_->GetNamespace(nsname_);
    if (ns == NULL) {
      // Namespace has not been initialized.
      filter.reset(NULL);
    } else {
      filter.reset(new CompactionFilter(ns, cache_));
    }
    return filter;
  }

  const char* Name() const override { return "NDBCompactionFilterFactory"; }

 private:
  Engine* engine_ {NULL};
  std::string nsname_;
  std::shared_ptr<CompactionMetaCache> cache_;
};

}  // namespace ndb

#endif /* NDB_ENGINE_COMPACTION_H_ */
