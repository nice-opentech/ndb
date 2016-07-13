#include "ndb/engine/compaction.h"

namespace ndb {

static void DeleteMeta(const Slice& id, void* meta) {
  delete reinterpret_cast<CompactionMeta*>(meta);
}

bool CompactionMetaCache::Get(const Slice& id, CompactionMeta* meta) {
  auto handle = cache_->Lookup(id);
  if (handle == NULL) return false;
  *meta = *reinterpret_cast<CompactionMeta*>(cache_->Value(handle));
  cache_->Release(handle);
  return true;
}

void CompactionMetaCache::Put(const Slice& id, const CompactionMeta& meta) {
  cache_->Release(cache_->Insert(id, new CompactionMeta(meta), 1, DeleteMeta));
}

void CompactionMetaCache::Delete(const Slice& id) {
  cache_->Erase(id);
}


bool CompactionFilter::Filter(int level,
                              const Slice& id,
                              const Slice& existing_value,
                              std::string* new_value,
                              bool* value_changed) const {
  // Parse value.
  Value value;
  if (!value.ParseFromArray(existing_value.data(), existing_value.size())) {
    return true;
  }

  Slice tmp = id;
  std::string kmeta;
  if (!DecodeMeta(&tmp, &kmeta)) {
    // Simple types.
    return value.IsDeleted();
  }

  // Collection types.
  if (value.has_meta()) {
    return FilterMeta(kmeta, value);
  }
  return FilterMember(id);
}

Result CompactionFilter::GetMeta(const Slice& kmeta, CompactionMeta* meta) const {
  if (cache_->Get(kmeta, meta)) return Result::OK();
  Value vmeta;
  NDB_TRY(ns_->Get(kmeta, &vmeta));
  if (!vmeta.has_meta()) return Result::NotFound();
  meta->deleted = vmeta.IsDeleted();
  meta->version = vmeta.meta().version();
  cache_->Put(kmeta, *meta);
  return Result::OK();
}

bool CompactionFilter::FilterMeta(const Slice& kmeta, const Value& vmeta) const {
  auto begin = EncodePrefix(kmeta, vmeta.meta().version(), 0, 0);
  auto it = ns_->RangeGet(begin);
  it->Seek();
  if (it->Valid()) {
    if (it->id().size() > kmeta.size() &&
        memcmp(it->id().data(), kmeta.data(), kmeta.size()) == 0) {
      cache_->Put(kmeta, {vmeta.IsDeleted(), vmeta.meta().version()});
      return false;
    }
  }
  cache_->Delete(kmeta);
  return true;
}

bool CompactionFilter::FilterMember(const Slice& id) const {
  Slice tmp = id;
  std::string kmeta;
  uint64_t version = 0;
  uint8_t type = 0, subtype = 0;
  if (!DecodePrefix(&tmp, &kmeta, &version, &type, &subtype)) {
    return false;
  }

  CompactionMeta meta;
  auto r = GetMeta(kmeta, &meta);
  if (r.IsNotFound()) return true;
  if (r.ok()) {
    if (version < meta.version || (version == meta.version && meta.deleted)) {
      return true;
    }
  }
  return false;
}

}  // namespace ndb
