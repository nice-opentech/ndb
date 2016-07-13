#include "ndb/engine/value.h"

namespace ndb {

Value Value::FromInt64(int64_t i) {
  Value value;
  value.set_int64(i);
  return value;
}

Value Value::FromBytes(const Slice& b) {
  Value value;
  value.set_bytes(b.data(), b.size());
  return value;
}

std::string Value::Encode() const {
  std::string s;
  NDB_ASSERT(SerializeToString(&s));
  return s;
}

Result Value::Decode(const Slice& s) {
  if (!ParseFromArray(s.data(), s.size())) {
    return ProtobufError();
  }
  if (IsDeleted()) {
    if (has_meta()) {
      auto version = meta().version();
      Clear();
      mutable_meta()->set_version(version + 1);
    } else {
      Clear();
    }
    return Result::NotFound();
  }
  return Result::OK();
}

bool Value::IsDeleted() const {
  // Deleted or expired.
  if (has_meta() && meta().deleted()) {
    return true;
  }
  if (has_expire() && expire() < getmstime()) {
    return true;
  }
  return false;
}

void Value::SetLength(uint64_t length) {
  mutable_meta()->set_length(length);
  if (length == 0) {
    mutable_meta()->set_deleted(true);
  }
}

void Value::SetConfigs(const Configs& configs) {
  if (!has_expire() && configs.has_expire()) {
    set_expire(getmstime() + configs.expire());
  }
  if (has_meta()) {
    auto meta = mutable_meta();
    if (configs.has_maxlen()) {
      meta->set_maxlen(configs.maxlen());
    }
    if (configs.has_pruning()) {
      meta->set_pruning(configs.pruning());
    }
  }
}

bool Value::ExceedMaxlen(uint64_t* start, uint64_t* stop) {
  if (!has_meta()) return false;
  if (!meta().has_maxlen()) return false;
  if (meta().length() < meta().maxlen() * 1.1) return false;
  if (meta().pruning() == Pruning::MIN) {
    *start = 0, *stop = meta().length() - meta().maxlen() - 1;
  } else {
    *start = meta().maxlen(), *stop = meta().length() - 1;
  }
  return true;
}

const char* TypeName(const Value& value) {
  if (value.IsDeleted()) {
    return "none";
  } else if (value.has_int64()) {
    return "int";
  } else if (value.has_bytes()) {
    return "string";
  } else if (value.has_meta()) {
    switch (value.meta().type()) {
      case Meta::SET: return "set";
      case Meta::OSET: return "oset";
      case Meta::ZSET: return "zset";
      case Meta::LIST: return "list";
      case Meta::HASH: return "hash";
    }
  }
  return "unknown";
}

const char* PruningName(Pruning pruning) {
  return pruning == Pruning::MIN ? "min" : "max";
}

}  // namespace ndb
