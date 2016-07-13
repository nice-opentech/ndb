#include "ndb/ndb.h"

namespace ndb {

// NSNEW namespace
Response CommandNSNEW(const Request& request) {
  return ndb->engine->NewNamespace(request.args(1));
}

// NSDEL namespace
Response CommandNSDEL(const Request& request) {
  return ndb->engine->DropNamespace(request.args(1));
}

// NSGET namespace name
Response CommandNSGET(const Request& request) {
  auto ns = NDB_TRY_GETNS(request.args(1));
  auto configs = ns->GetConfigs();

  auto name = request.args(2).c_str();
  if (strcasecmp(name, "expire") == 0) {
    return Response::Bulk(configs.expire());
  }
  if (strcasecmp(name, "maxlen") == 0) {
    return Response::Bulk(configs.maxlen());
  }
  if (strcasecmp(name, "pruning") == 0) {
    return Response::Bulk(PruningName(configs.pruning()));
  }
  return Response::InvalidArgument();
}

// NSSET namespace name value
Response CommandNSSET(const Request& request) {
  auto ns = NDB_TRY_GETNS(request.args(1));
  auto configs = ns->GetConfigs();

  auto name = request.args(2).c_str();
  if (strcasecmp(name, "pruning") == 0) {
    Pruning pruning;
    if (!ParsePruning(request.args(3), &pruning)) {
      return Response::InvalidArgument();
    }
    configs.set_pruning(pruning);
  } else {
    uint64_t value = 0;
    if (!ParseUint64(request.args(3), &value)) {
      return Response::InvalidArgument();
    }
    if (strcasecmp(name, "expire") == 0) {
      configs.set_expire(value);
    } else if (strcasecmp(name, "maxlen") == 0) {
      configs.set_maxlen(value);
    } else {
      return Response::InvalidArgument();
    }
  }

  return ns->PutConfigs(configs);
}

// NSLIST
Response CommandNSLIST(const Request& request) {
  return Response::Bulks(ndb->engine->ListNamespaces());
}

}  // namespace ndb
