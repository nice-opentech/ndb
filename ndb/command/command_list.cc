#include "ndb/ndb.h"

namespace ndb {

namespace list {

#define NDB_TRY_GETMETA_BYKEY(k, ns, kmeta, vmeta)                  \
  NDB_TRY_GETMETA_TYPE_BYKEY(k, ns, kmeta, vmeta, Meta::LIST);      \
  if (length == 0) {                                                \
    vmeta.mutable_meta()->set_minindex(INT64_MAX);                  \
    vmeta.mutable_meta()->set_maxindex(INT64_MAX);                  \
  }                                                                 \
  auto minindex = vmeta.meta().minindex();                          \
  auto maxindex = vmeta.meta().maxindex()

std::string EncodePrefix(const Slice& kmeta, uint64_t version) {
  return ndb::EncodePrefix(kmeta, version, Meta::LIST, 1);
}

std::string EncodeMember(const Slice& kmeta, uint64_t version, uint64_t index) {
  std::string dst = EncodePrefix(kmeta, version);
  EncodeUint64(&dst, index);
  return dst;
}

bool DecodeNextIndex(const Slice& id, Value& vmeta, bool reverse) {
  auto field = id;
  uint64_t index = 0;
  if (!RemovePrefix(&field) || !DecodeUint64(&field, &index)) {
    return false;
  }
  if (!reverse) {
    vmeta.mutable_meta()->set_minindex(index);
  } else {
    vmeta.mutable_meta()->set_maxindex(index);
  }
  return true;
}

}  // namespace list

Response GenericLPUSH(const Request& request, bool exists, bool reverse) {
  NDB_LOCK_KEY(request.args(1));
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);
  // Return 0 if not exists.
  if (length == 0 && exists) return Response::Int(0);

  NSBatch batch(ns);
  int64_t count = request.argc() - 2;
  if (!reverse) {
    if (length > 0) minindex--;
    for (size_t i = 2; i < request.argc(); i++) {
      auto member = list::EncodeMember(kmeta, version, minindex--);
      batch.Put(member, Value::FromBytes(request.args(i)));
    }
    vmeta.mutable_meta()->set_minindex(minindex + 1);
  } else {
    if (length > 0) maxindex++;
    for (size_t i = 2; i < request.argc(); i++) {
      auto member = list::EncodeMember(kmeta, version, maxindex++);
      batch.Put(member, Value::FromBytes(request.args(i)));
    }
    vmeta.mutable_meta()->set_maxindex(maxindex - 1);
  }

  NDB_COMMAND_UPDATE_LENGTH(vmeta, count);
  vmeta.SetConfigs(configs);
  batch.Put(kmeta, vmeta);
  NDB_TRY(batch.Commit());
  return Response::Int(vmeta.meta().length());
}

// LPUSH key value [value ...]
Response CommandLPUSH(const Request& request) {
  return GenericLPUSH(request, false, false);
}

// LPUSHX key value [value ...]
Response CommandLPUSHX(const Request& request) {
  return GenericLPUSH(request, true, false);
}

// RPUSH key value [value ...]
Response CommandRPUSH(const Request& request) {
  return GenericLPUSH(request, false, true);
}

// RPUSHX key value [value ...]
Response CommandRPUSHX(const Request& request) {
  return GenericLPUSH(request, true, true);
}

Response GenericLPOP(const Request& request, bool reverse) {
  NDB_LOCK_KEY(request.args(1));
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);
  // Return nil if not exists.
  if (length == 0) return Response::Null();

  auto begin = list::EncodeMember(kmeta, version, minindex);
  auto end = list::EncodeMember(kmeta, version, maxindex);
  auto it = ns->RangeGet(begin, end, 0, UINT64_MAX, reverse);

  it->Seek();
  if (!it->Valid()) {
    return Response::Null();
  }

  // Delete the first member.
  NSBatch batch(ns);
  batch.Delete(it->id());
  Value value;
  NDB_TRY(value.Decode(it->value()));

  // Decode the second member's index
  it->Next();
  if (it->Valid()) {
    if (!list::DecodeNextIndex(it->id(), vmeta, reverse)) {
      return NDB_COMMAND_ERROR("corruption");
    }
  }

  NDB_COMMAND_UPDATE_LENGTH(vmeta, -1);
  vmeta.SetConfigs(configs);
  batch.Put(kmeta, vmeta);
  NDB_TRY(batch.Commit());
  return ConvertValueToBulk(value);
}

// LPOP key
Response CommandLPOP(const Request& request) {
  return GenericLPOP(request, false);
}

// RPOP key
Response CommandRPOP(const Request& request) {
  return GenericLPOP(request, true);
}

// LREM key count value
Response CommandLREM(const Request& request) {
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);
  // Return 0 if not exists.
  if (length == 0) return Response::Int(0);

  int64_t count;
  if (!ParseInt64(request.args(2), &count)) {
    return Response::InvalidArgument();
  }
  bool reverse = false;
  if (count < 0) {
    reverse = true;
    count = -count;
  }

  auto begin = list::EncodeMember(kmeta, version, minindex);
  auto end = list::EncodeMember(kmeta, version, maxindex);
  auto it = ns->RangeGet(begin, end, 0, UINT64_MAX, reverse);

  NSBatch batch(ns);
  bool is_first = false;
  int64_t deleted_count = 0;
  for (it->Seek(); it->Valid(); it->Next()) {
    Value value;
    NDB_TRY(value.Decode(it->value()));
    if (value.has_bytes() && value.bytes() == request.args(3)) {
      batch.Delete(it->id());
      deleted_count++;
    } else {
      // Decode the first unmatched index
      if (!is_first) {
        is_first = true;
        if (!list::DecodeNextIndex(it->id(), vmeta, reverse)) {
          return NDB_COMMAND_ERROR("corruption");
        }
      }
    }
    if (count != 0 && deleted_count >= count) {
      break;
    }
  }
  if (it->Valid()) {
    if (!is_first) {
        if (!list::DecodeNextIndex(it->id(), vmeta, reverse)) {
          return NDB_COMMAND_ERROR("corruption");
        }
    }
  }

  NDB_COMMAND_UPDATE_LENGTH(vmeta, -deleted_count);
  vmeta.SetConfigs(configs);
  batch.Put(kmeta, vmeta);
  NDB_TRY(batch.Commit());
  return Response::Int(deleted_count);
}

Response InternalLTRIM(NSRef ns, NSBatch& batch, Value& vmeta,
                       const Slice& begin, const Slice& end, int64_t count, bool reverse) {
  if (count <= 0) return Response::OK();
  auto it = ns->RangeGet(begin, end, 0, UINT64_MAX, reverse);
  for (it->Seek(); it->Valid() && count > 0; it->Next(), count--) {
    batch.Delete(it->id());
  }
  if (it->Valid()) {
    if (!list::DecodeNextIndex(it->id(), vmeta, reverse)) {
      return Result::Error("decode next index failed.");
    }
  }
  return Response::OK();
}

// LTRIM key start stop
Response CommandLTRIM(const Request& request) {
  int64_t start, stop;
  if (!ParseInt64(request.args(2), &start) ||
      !ParseInt64(request.args(3), &stop)) {
    return Response::InvalidArgument();
  }

  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);
  if (length == 0) return Response::OK();

  NSBatch batch(ns);

  if (CheckLimit(length, &start, &stop)) {
    auto begin = list::EncodeMember(kmeta, version, minindex);
    auto end = list::EncodeMember(kmeta, version, maxindex);
    // Trim left
    auto res = InternalLTRIM(ns, batch, vmeta, begin, end, start, false);
    if (!res.IsOK()) return res;
    // Trim right
    res = InternalLTRIM(ns, batch, vmeta, begin, end, length - stop - 1, true);
    if (!res.IsOK()) return res;
    vmeta.SetLength(stop - start + 1);
  } else {
    vmeta.SetLength(0);
  }

  vmeta.SetConfigs(configs);
  batch.Put(kmeta, vmeta);
  NDB_TRY(batch.Commit());
  return Response::OK();
}

// LSET key index value
Response CommandLSET(const Request& request) {
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);

  int64_t index = 0;
  if (!ParseInt64(request.args(2), &index)) {
    return Response::InvalidArgument();
  }
  if (index < 0) index += length;
  if (index < 0 || index >= (int64_t) length) return Response::OutofRange();

  bool reverse = false;
  if (index > (int64_t) length / 2) {
    reverse = true;
    index = length - index - 1;
  }

  auto begin = list::EncodeMember(kmeta, version, minindex);
  auto end = list::EncodeMember(kmeta, version, maxindex);
  auto it = ns->RangeGet(begin, end, index, 1, reverse);

  it->Seek();
  if (!it->Valid()) {
    return Response::OutofRange();
  }

  NDB_TRY(ns->Put(it->id(), Value::FromBytes(request.args(3))));
  return Response::OK();
}

// LINDEX key index
Response CommandLINDEX(const Request& request) {
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);

  int64_t index = 0;
  if (!ParseInt64(request.args(2), &index)) {
    return Response::InvalidArgument();
  }
  if (index < 0) index += length;
  if (index < 0 || index >= (int64_t) length) return Response::Null();

  bool reverse = false;
  if (index > (int64_t) length / 2) {
    reverse = true;
    index = length - index - 1;
  }

  auto begin = list::EncodeMember(kmeta, version, minindex);
  auto end = list::EncodeMember(kmeta, version, maxindex);
  auto it = ns->RangeGet(begin, end, index, 1, reverse);

  it->Seek();
  if (!it->Valid()) {
    return Response::Null();
  }

  Value value;
  NDB_TRY(value.Decode(it->value()));
  return ConvertValueToBulk(value);
}

// LRANGE key start stop
Response CommandLRANGE(const Request& request) {
  int64_t start, stop;
  if (!ParseInt64(request.args(2), &start) ||
      !ParseInt64(request.args(3), &stop)) {
    return Response::InvalidArgument();
  }

  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);
  if (length == 0) return Response::Size(0);

  if (!CheckLimit(length, &start, &stop)) {
    return Response::Size(0);
  }

  int64_t offset = start, count = stop - start + 1;
  auto begin = list::EncodeMember(kmeta, version, minindex);
  auto end = list::EncodeMember(kmeta, version, maxindex);
  auto it = ns->RangeGet(begin, end, offset, count);

  auto res = Response::Size(count);
  for (it->Seek(); it->Valid(); it->Next(), count--) {
    Value value;
    NDB_TRY(value.Decode(it->value()));
    res.Append(ConvertValueToBulk(value));
  }
  if (count != 0) {
    NDB_LOG_ERROR("*COMMAND* LRANGE namespace %s id %s count %lld",
                  ns->GetName().c_str(), kmeta.data(), (long long) count);
    return Result::Error("corruption");
  }
  return res;
}

// LLEN key
Response CommandLLEN(const Request& request) {
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);
  return Response::Int(length);
}

}  // namespace ndb
