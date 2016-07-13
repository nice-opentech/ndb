#include "ndb/ndb.h"

namespace ndb {

Response GenericINCRBY(const Request& request, int64_t increment) {
  NDB_LOCK_KEY(request.args(1));
  NDB_TRY_GETNS_BYKEY(request.args(1), ns, id);

  Value value;
  auto r = ns->Get(id, &value);
  if (r.IsNotFound()) {
    value.set_int64(0);
  } else if (!r.ok()) {
    return r;
  }

  int64_t origin = 0;
  if (!ConvertValueToInt64(value, &origin)) {
    return Response::WrongType();
  }
  if (!CheckIncrementBound(origin, increment)) {
    return Response::OutofRange();
  }

  value.set_int64(origin + increment);
  value.SetConfigs(configs);
  NDB_TRY(ns->Put(id, value));
  return Response::Int(value.int64());
}

// INCR key
Response CommandINCR(const Request& request) {
  return GenericINCRBY(request, 1);
}

// INCRBY key increment
Response CommandINCRBY(const Request& request) {
  int64_t increment = 0;
  if (!ParseInt64(request.args(2), &increment)) {
    return Response::InvalidArgument();
  }
  return GenericINCRBY(request, increment);
}

// DECR key
Response CommandDECR(const Request& request) {
  return GenericINCRBY(request, -1);
}

// DECRBY key increment
Response CommandDECRBY(const Request& request) {
  int64_t increment = 0;
  if (!ParseInt64(request.args(2), &increment)) {
    return Response::InvalidArgument();
  }
  return GenericINCRBY(request, -increment);
}

}  // namespace ndb
