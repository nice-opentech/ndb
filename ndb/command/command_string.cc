#include "ndb/ndb.h"

namespace ndb {

// GET key
Response CommandGET(const Request& request) {
  NDB_TRY_GETNS_BYKEY(request.args(1), ns, id);
  Value value;
  NDB_TRY(ns->Get(id, &value));
  return ConvertValueToBulk(value);
}

// MGET key [key ...]
Response CommandMGET(const Request& request) {
  std::vector<Slice> keys(request.args().begin() + 1, request.args().end());
  std::vector<Value> values;
  auto results = GenericMGET(ndb->engine, keys, &values);
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

Response GenericSET(const Request& request, const Slice& bytes, int options, milliseconds expire) {
  NDB_LOCK_KEY(request.args(1));
  NDB_TRY_GETNS_BYKEY(request.args(1), ns, id);

  // Return Null if exists.
  if (options & NDB_OPTION_NX) {
    auto r = ns->Get(id, NULL);
    if (r.ok()) {
      return Response::Null();
    } else if (!r.IsNotFound()) {
      return r;
    }
  }

  // Return Null if not exists.
  if (options & NDB_OPTION_XX) {
    auto r = ns->Get(id, NULL);
    if (r.IsNotFound()) {
      return Response::Null();
    } else if (!r.ok()) {
      return r;
    }
  }

  if (options & NDB_OPTION_EX) {
    configs.set_expire(expire.count());
  }

  auto value = Value::FromBytes(bytes);
  value.SetConfigs(configs);
  return ns->Put(id, value);
}

// SET key value [EX seconds] [PX milliseconds] [NX] [XX]
Response CommandSET(const Request& request) {
  int options = 0;
  uint64_t expire = 0;
  for (size_t i = 3; i < request.argc(); i++) {
    auto opt = request.args(i).c_str();
    if (strcasecmp(opt, "EX") == 0 || strcasecmp(opt, "PX") == 0) {
      if (++i == request.argc() || !ParseUint64(request.args(i), &expire)) {
        return Response::InvalidArgument();
      }
      if (strcasecmp(opt, "EX") == 0) {
        expire *= 1000;
      }
      options |= NDB_OPTION_EX;
    } else if (strcasecmp(opt, "NX") == 0) {
      options |= NDB_OPTION_NX;
    } else if (strcasecmp(opt, "XX") == 0) {
      options |= NDB_OPTION_XX;
    } else {
      return Response::InvalidOption();
    }
  }
  if ((options & NDB_OPTION_NX) && (options & NDB_OPTION_XX)) {
    return Response::InvalidOption();
  }
  return GenericSET(request, request.args(2), options, milliseconds(expire));
}

// SETNX key value
Response CommandSETNX(const Request& request) {
  auto res = GenericSET(request, request.args(2), NDB_OPTION_NX, milliseconds(0));
  if (res.IsOK()) {
    return Response::Int(1);
  }
  if (res.IsNull()) {
    return Response::Int(0);
  }
  return res;
}

Response GenericSETEX(const Request& request, bool inseconds) {
  uint64_t expire = 0;
  if (!ParseUint64(request.args(2), &expire)) {
    return Response::InvalidArgument();
  }
  if (inseconds) {
    expire *= 1000;
  }
  return GenericSET(request, request.args(3), NDB_OPTION_EX, milliseconds(expire));
}

// SETEX key seconds value
Response CommandSETEX(const Request& request) {
  return GenericSETEX(request, true);
}

// PSETEX key milliseconds value
Response CommandPSETEX(const Request& request) {
  return GenericSETEX(request, false);
}

Response GenericMSET(const Request& request, bool not_exists) {
  if (request.argc() % 2 != 1) {
    return Response::InvalidArgument();
  }

  std::vector<Slice> keys;
  for (size_t i = 1; i < request.argc(); i += 2) {
    keys.push_back(request.args(i));
  }

  NDB_LOCK_KEY(keys);

  // Return 0 if any key exists.
  if (not_exists) {
    auto results = GenericMGET(ndb->engine, keys, NULL);
    for (const auto& r : results) {
      if (r.ok()) {
        return Response::Int(0);
      }
    }
  }

  Batch batch(ndb->engine);
  for (size_t i = 1; i < request.argc(); i += 2) {
    NDB_TRY_GETNS_BYKEY(request.args(i), ns, id);
    auto value = Value::FromBytes(request.args(i+1));
    value.SetConfigs(configs);
    batch.Put(ns, id, value);
  }

  NDB_TRY(batch.Commit());
  if (not_exists) {
    return Response::Int(1);
  }
  return Response::OK();
}

// MSET key value [key value ...]
Response CommandMSET(const Request& request) {
  return GenericMSET(request, false);
}

// MSETNX key value [key value ...]
Response CommandMSETNX(const Request& request) {
  return GenericMSET(request, true);
}

}  // namespace ndb
