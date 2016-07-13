#include "ndb/ndb.h"

namespace ndb {

namespace hash {

#define NDB_TRY_GETMETA_BYKEY(k, ns, kmeta, vmeta)                  \
  NDB_TRY_GETMETA_TYPE_BYKEY(k, ns, kmeta, vmeta, Meta::HASH)

std::string EncodePrefix(const Slice& kmeta, uint64_t version) {
  return ndb::EncodePrefix(kmeta, version, Meta::HASH, 1);
}

std::string EncodeMember(const Slice& kmeta, uint64_t version, const Slice& field) {
  std::string dst = EncodePrefix(kmeta, version);
  dst.append(field.data(), field.size());
  return dst;
}

}  // namespace hash

// HGET key field
Response CommandHGET(const Request& request) {
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);
  if (length == 0) return Response::Null();

  Value value;
  auto member = hash::EncodeMember(kmeta, version, request.args(2));
  NDB_TRY(ns->Get(member, &value));
  return ConvertValueToBulk(value);
}

// HMGET key field [field ...]
Response CommandHMGET(const Request& request) {
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);

  std::vector<std::string> members;
  for (size_t i = 2; i < request.argc(); i++) {
    members.push_back(hash::EncodeMember(kmeta, version, request.args(i)));
  }

  std::vector<Value> values;
  auto results = ns->MultiGet(members, &values);
  auto res = Response::Size(results.size());
  for (size_t i = 0; i < results.size(); i++) {
    const auto& v = values[i];
    const auto& r = results[i];
    if (r.ok()) {
      res.Append(ConvertValueToBulk(v));
    } else {
      res.Append(r);
    }
  }
  return res;
}

Response GenericHSET(const Request& request, bool not_exists) {
  NDB_LOCK_KEY(request.args(1));
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);

  int64_t count = 0;
  auto member = hash::EncodeMember(kmeta, version, request.args(2));
  auto r = ns->Get(member, NULL);
  if (r.IsNotFound()) {
    count++;
  } else if (!r.ok()) {
    return r;
  } else if (not_exists) {
    return Response::Int(0);
  }

  NDB_COMMAND_UPDATE_LENGTH(vmeta, +count);
  vmeta.SetConfigs(configs);

  NSBatch batch(ns);
  batch.Put(member, Value::FromBytes(request.args(3)));
  batch.Put(kmeta, vmeta);
  NDB_TRY(batch.Commit());
  return Response::Int(count);
}

// HSET key field value
Response CommandHSET(const Request& request) {
  return GenericHSET(request, false);
}

// HSETNX key field value
Response CommandHSETNX(const Request& request) {
  return GenericHSET(request, true);
}

// HMSET key field value [field value ...]
Response CommandHMSET(const Request& request) {
  if (request.argc() % 2 != 0) {
    return Response::InvalidArgument();
  }

  // Remove duplication.
  std::map<std::string, Slice> kv;
  for (size_t i = 2; i < request.argc(); i += 2) {
    kv[request.args(i)] = request.args(i+1);
  }

  NDB_LOCK_KEY(request.args(1));
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);

  std::vector<std::string> members;
  std::vector<Slice> values;
  for (const auto& it : kv) {
    members.push_back(hash::EncodeMember(kmeta, version, it.first));
    values.push_back(it.second);
  }

  NSBatch batch(ns);
  uint64_t count = 0;
  auto results = ns->MultiGet(members, NULL);
  for (size_t i = 0; i < results.size(); i++) {
    const auto& r = results[i];
    if (r.IsNotFound()) {
      count++;
    } else if (!r.ok()) {
      return r;
    }
    batch.Put(members[i], Value::FromBytes(values[i]));
  }

  NDB_COMMAND_UPDATE_LENGTH(vmeta, +count);
  vmeta.SetConfigs(configs);
  batch.Put(kmeta, vmeta);
  NDB_TRY(batch.Commit());
  return Response::OK();
}

// HDEL key field [field ...]
Response CommandHDEL(const Request& request) {
  // Remove duplication.
  std::set<std::string> fields(request.args().begin() + 2, request.args().end());

  NDB_LOCK_KEY(request.args(1));
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);
  if (length == 0) return Response::Int(0);

  std::vector<std::string> members;
  for (const auto& field : fields) {
    members.push_back(hash::EncodeMember(kmeta, version, field));
  }

  NSBatch batch(ns);
  uint64_t count = 0;
  auto results = ns->MultiGet(members, NULL);
  for (size_t i = 0; i < results.size(); i++) {
    const auto& m = members[i];
    const auto& r = results[i];
    if (r.ok()) {
      batch.Delete(m);
      count++;
    } else if (!r.IsNotFound()) {
      return r;
    }
  }

  NDB_COMMAND_UPDATE_LENGTH(vmeta, -count);
  vmeta.SetConfigs(configs);
  batch.Put(kmeta, vmeta);
  NDB_TRY(batch.Commit());
  return Response::Int(count);
}

// HEXISTS key field
Response CommandHEXISTS(const Request& request) {
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);
  if (length == 0) return Response::Int(0);

  auto member = hash::EncodeMember(kmeta, version, request.args(2));
  auto r = ns->Get(member, NULL);
  if (r.ok()) return Response::Int(1);
  if (r.IsNotFound()) return Response::Int(0);
  return r;
}

// HSTRLEN key field
Response CommandHSTRLEN(const Request& request) {
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);
  if (length == 0) return Response::Int(0);

  Value value;
  auto member = hash::EncodeMember(kmeta, version, request.args(2));
  auto r = ns->Get(member, &value);
  if (r.ok()) {
    auto size = 0;
    if (value.has_int64()) {
      size = std::to_string(value.int64()).size();
    }
    if (value.has_bytes()) {
      size = value.bytes().size();
    }
    return Response::Int(size);
  }
  if (r.IsNotFound()) {
    return Response::Int(0);
  }
  return r;
}

// HINCRBY key field increment
Response CommandHINCRBY(const Request& request) {
  int64_t increment = 0;
  if (!ParseInt64(request.args(3), &increment)) {
    return Response::InvalidArgument();
  }

  NDB_LOCK_KEY(request.args(1));
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);

  Value value;
  auto member = hash::EncodeMember(kmeta, version, request.args(2));
  auto r = ns->Get(member, &value);
  if (r.IsNotFound()) {
    value.set_int64(0);
    NDB_COMMAND_UPDATE_LENGTH(vmeta, +1);
  } else if (!r.ok()) {
    return r;
  }

  int64_t origin;
  if (!ConvertValueToInt64(value, &origin)) {
    return Response::WrongType();
  }
  if (!CheckIncrementBound(origin, increment)) {
    return Response::OutofRange();
  }

  NSBatch batch(ns);
  value.set_int64(origin + increment);
  batch.Put(member, value);
  vmeta.SetConfigs(configs);
  batch.Put(kmeta, vmeta);
  NDB_TRY(batch.Commit());
  return Response::Int(value.int64());
}

// HLEN key
Response CommandHLEN(const Request& request) {
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);
  return Response::Int(length);
}

Response GenericHGETALL(const Request& request, bool with_fields, bool with_values) {
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);
  if (length == 0) return Response::Size(0);

  auto begin = hash::EncodePrefix(kmeta, version);
  auto end = FindNextSuccessor(begin);
  auto res = Response::Size((with_fields + with_values) * length);

  auto it = ns->RangeGet(begin, end, 0, length);
  for (it->Seek(); it->Valid(); it->Next(), length--) {
    if (with_fields) {
      auto field = it->id();
      if (!RemovePrefix(&field)) {
        return NDB_COMMAND_ERROR("corruption");
      }
      res.AppendBulk(field.data(), field.size());
    }
    if (with_values) {
      Value value;
      NDB_TRY(value.Decode(it->value()));
      res.Append(ConvertValueToBulk(value));
    }
  }
  NDB_TRY(it->result());
  if (length != 0) {
    NDB_LOG_ERROR("*COMMAND* HGETALL namespace %s id %s length %lld",
                  ns->GetName().c_str(), kmeta.data(), (long long) length);
    return Result::Error("corruption");
  }
  return res;
}

// HKEYS key
Response CommandHKEYS(const Request& request) {
  return GenericHGETALL(request, true, false);
}

// HVALS key
Response CommandHVALS(const Request& request) {
  return GenericHGETALL(request, false, true);
}

// HGETALL key
Response CommandHGETALL(const Request& request) {
  return GenericHGETALL(request, true, true);
}

}  // namespace ndb
