#ifndef NDB_ENGINE_COMMON_H_
#define NDB_ENGINE_COMMON_H_

#include "ndb/common/stats.h"
#include "ndb/common/result.h"
#include "ndb/engine/engine.pb.h"

#include <rocksdb/db.h>
#include <rocksdb/cache.h>
#include <rocksdb/table.h>
#include <rocksdb/statistics.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/compaction_filter.h>
#include <rocksdb/utilities/backupable_db.h>

namespace ndb {

using rocksdb::Slice;
using rocksdb::Status;

inline Result ProtobufError() {
  return Result::Error("Protobuf error.");
}

inline Result StatusToResult(const Status& s) {
  if (s.ok()) {
    return Result::OK();
  }
  if (s.IsNotFound()) {
    return Result::NotFound();
  }
  return Result::Error(s.ToString().c_str());
}

}  // namespace ndb

#endif /* NDB_ENGINE_COMMON_H_ */
