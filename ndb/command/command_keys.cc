#include "ndb/ndb.h"

namespace ndb {

Response InternalTYPE(NSRef ns, const Slice& id) {
  Value value;
  NDB_TRY(ns->Get(id, &value));
  return Response::Simple(TypeName(value));
}

// TYPE key
Response CommandTYPE(const Request& request) {
  NDB_TRY_GETNS_BYKEY(request.args(1), ns, id);
  auto res = InternalTYPE(ns, id);
  if (res.IsNull()) {
    res = InternalTYPE(ns, EncodeMeta(id));
  }
  if (res.IsNull()) {
    res = Response::Simple("none");
  }
  return res;
}

// META key
Response CommandMETA(const Request& request) {
  NDB_TRY_GETNS_BYKEY(request.args(1), ns, id);

  Value value;
  NDB_TRY(ns->Get(EncodeMeta(id), &value));
  if (!value.has_meta()) return Response::Null();

  auto res = Response::Size(8*2);
  res.AppendBulk("type");
  res.AppendBulk(TypeName(value));
  res.AppendBulk("deleted");
  res.AppendBulk(value.meta().deleted() ? "true" : "false");
  res.AppendBulk("version");
  res.AppendBulk(value.meta().version());
  res.AppendBulk("maxlen");
  res.AppendBulk(value.meta().maxlen());
  res.AppendBulk("length");
  res.AppendBulk(value.meta().length());
  res.AppendBulk("pruning");
  res.AppendBulk(PruningName(value.meta().pruning()));
  res.AppendBulk("minindex");
  res.AppendBulk(value.meta().minindex() - INT64_MAX);
  res.AppendBulk("maxindex");
  res.AppendBulk(value.meta().maxindex() - INT64_MAX);
  return res;
}

// KEYS pattern count
Response CommandKEYS(const Request& request) {
  NDB_TRY_GETNS_BYKEY(request.args(1), ns, id);

  uint64_t count = 128;
  if (request.argc() == 3) {
    if (!ParseUint64(request.args(2), &count)) {
      return Response::InvalidArgument();
    }
  }

  std::vector<std::string> ids;
  auto it = ns->RangeGet(id, Slice(), 0, count);
  for (it->Seek(); it->Valid(); it->Next()) {
    ids.emplace_back(it->id().data(), it->id().size());
  }
  return Response::Bulks(ids);
}

// EXISTS key [key ...]
Response CommandEXISTS(const Request& request) {
  std::vector<Slice> keys;
  for (size_t i = 1; i < request.argc(); i++) {
    keys.push_back(request.args(i));
    keys.push_back(EncodeMeta(request.args(i)));
  }

  uint64_t count = 0;
  auto results = GenericMGET(ndb->engine, keys, NULL);
  for (auto r : results) {
    if (r.IsNotFound()) {
      continue;
    } else if (!r.ok()) {
      return r;
    }
    count++;
  }

  return Response::Int(count);
}

// DEL key [key ...]
Response CommandDEL(const Request& request) {
  std::vector<Slice> keys;
  for (size_t i = 1; i < request.argc(); i++) {
    keys.push_back(request.args(i));
    keys.push_back(EncodeMeta(request.args(i)));
  }

  NDB_LOCK_KEY(keys);

  Batch batch(ndb->engine);
  uint64_t count = 0;
  std::vector<Value> values;
  auto results = GenericMGET(ndb->engine, keys, &values);
  for (size_t i = 0; i < results.size(); i++) {
    auto& k = keys[i];
    auto& v = values[i];
    auto& r = results[i];

    if (r.IsNotFound()) {
      continue;
    } else if (!r.ok()) {
      return r;
    }

    NDB_TRY_GETNS_BYKEY(k, ns, id);
    if (v.has_meta()) {
      v.mutable_meta()->set_deleted(true);
      batch.Put(ns, id, v);
    } else {
      batch.Delete(ns, id);
    }

    count++;
  }

  NDB_TRY(batch.Commit());
  return Response::Int(count);
}

Response InternalEXPIRE(NSRef ns, const Slice& id, milliseconds expire) {
  Value value;
  NDB_TRY(ns->Get(id, &value));
  value.set_expire(expire.count());
  NDB_TRY(ns->Put(id, value));
  return Response::Int(1);
}

Response GenericEXPIRE(const Request& request, uint64_t basetime, bool inseconds) {
  NDB_LOCK_KEY(request.args(1));
  NDB_TRY_GETNS_BYKEY(request.args(1), ns, id);

  uint64_t expire = 0;
  if (!ParseUint64(request.args(2), &expire)) {
    return Response::InvalidArgument();
  }
  if (inseconds) {
    expire = basetime + expire * 1000;
  } else {
    expire = basetime + expire;
  }

  auto res = InternalEXPIRE(ns, id, milliseconds(expire));
  if (res.IsNull()) {
    res = InternalEXPIRE(ns, EncodeMeta(id), milliseconds(expire));
  }
  if (res.IsNull()) {
    return Response::Int(0);
  }
  return res;
}

// EXPIRE key seconds
Response CommandEXPIRE(const Request& request) {
  return GenericEXPIRE(request, getmstime(), true);
}

// EXPIREAT key timestamp
Response CommandEXPIREAT(const Request& request) {
  return GenericEXPIRE(request, 0, true);
}

// PEXPIRE key milliseconds
Response CommandPEXPIRE(const Request& request) {
  return GenericEXPIRE(request, getmstime(), false);
}

// PEXPIREAT key timestamp
Response CommandPEXPIREAT(const Request& request) {
  return GenericEXPIRE(request, 0, false);
}

Response InternalTTL(NSRef ns, const Slice& id, bool inseconds) {
  Value value;
  NDB_TRY(ns->Get(id, &value));
  if (!value.has_expire()) {
    return Response::Int(-1);
  }
  auto expire = value.expire() - getmstime();
  if (inseconds) {
    expire /= 1000;
  }
  return Response::Int(expire);
}

Response GenericTTL(const Request& request, bool inseconds) {
  NDB_TRY_GETNS_BYKEY(request.args(1), ns, id);
  auto res = InternalTTL(ns, id, inseconds);
  if (res.IsNull()) {
    res = InternalTTL(ns, EncodeMeta(id), inseconds);
  }
  if (res.IsNull()) {
    return Response::Int(-2);
  }
  return res;
}

// TTL key
Response CommandTTL(const Request& request) {
  return GenericTTL(request, true);
}

// PTTL key
Response CommandPTTL(const Request& request) {
  return GenericTTL(request, false);
}

}  // namespace ndb
