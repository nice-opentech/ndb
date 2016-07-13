#include "ndb/engine/common.h"

namespace ndb {

using pb::Meta;
using pb::Pruning;
using pb::Configs;

class Value : public pb::Value {
 public:
  static Value FromInt64(int64_t i);
  static Value FromBytes(const Slice& b);

  std::string Encode() const;
  Result Decode(const Slice& s);

  bool IsDeleted() const;
  void SetLength(uint64_t length);
  void SetConfigs(const Configs& configs);

  // Remove [start, stop] if exceed maxlen.
  bool ExceedMaxlen(uint64_t* start, uint64_t* stop);
};

const char* TypeName(const Value& value);
const char* PruningName(Pruning pruning);

}  // namespace ndb
