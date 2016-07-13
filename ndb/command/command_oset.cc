#include "ndb/ndb.h"

namespace ndb {

namespace oset {

#define NDB_TRY_GETMETA_BYKEY(k, ns, kmeta, vmeta)                  \
  NDB_TRY_GETMETA_TYPE_BYKEY(k, ns, kmeta, vmeta, Meta::OSET)

std::string EncodePrefix(const Slice& kmeta, uint64_t version) {
  return ndb::EncodePrefix(kmeta, version, Meta::OSET, 1);
}

std::string EncodeMember(const Slice& kmeta, uint64_t version, int64_t field) {
  std::string dst = EncodePrefix(kmeta, version);
  EncodeInt64(&dst, field);
  return dst;
}

bool ParseFields(const Request& request, size_t idx, std::set<int64_t>* fields) {
  for (size_t i = idx; i < request.argc(); i++) {
    int64_t field = 0;
    if (!ParseInt64(request.args(i), &field)) {
      return false;
    }
    // Reserve INT64_MIN for -inf, INT64_MAX for +inf.
    if (field == INT64_MIN || field == INT64_MAX) {
      return false;
    }
    fields->insert(field);
  }
  return fields->size() > 0;
}

}  // namespace oset

// NOTE: Require external lock.
Response GenericOREMRANGEBYRANK(const Request& request, int64_t start, int64_t stop) {
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);

  if (!CheckLimit(length, &start, &stop)) {
    return Response::Int(0);
  }

  // Optimize
  bool reverse = false;
  uint64_t offset = start, count = stop - start + 1;
  if (offset > length / 2) {
    reverse = true;
    offset = length - stop - 1;
  }

  NSBatch batch(ns);
  auto begin = oset::EncodeMember(kmeta, version, INT64_MIN);
  auto end = oset::EncodeMember(kmeta, version, INT64_MAX);
  auto it = ns->RangeGet(begin, end, offset, count, reverse);
  for (it->Seek(), count = 0; it->Valid(); it->Next(), count++) {
    batch.Delete(it->id());
  }
  NDB_TRY(it->result());

  NDB_COMMAND_UPDATE_LENGTH(vmeta, -count);
  vmeta.SetConfigs(configs);
  batch.Put(kmeta, vmeta);
  NDB_TRY(batch.Commit());
  return Response::Int(count);
}

// OADD key [MAXLEN maxlen] [PRUNING min|max] field [field ...]
Response CommandOADD(const Request& request) {
  size_t idx = 2;

  // Parse options.
  Configs options;
  for (auto i = idx; i < request.argc(); i++) {
    auto opt = request.args(i).c_str();
    if (strcasecmp(opt, "MAXLEN") == 0 || strcasecmp(opt, "FINITY") == 0) {
      uint64_t maxlen = 0;
      if (++i == request.argc() || !ParseUint64(request.args(i), &maxlen)) {
        return Response::InvalidArgument();
      }
      options.set_maxlen(maxlen);
    } else if (strcasecmp(opt, "PRUNING") == 0) {
      Pruning pruning;
      if (++i == request.argc() || !ParsePruning(request.args(i), &pruning)) {
        return Response::InvalidArgument();
      }
      options.set_pruning(pruning);
    } else {
      idx = i;
      break;
    }
  }

  // Parse fields.
  std::set<int64_t> fields;
  if (!oset::ParseFields(request, idx, &fields)) {
    return Response::InvalidArgument();
  }

  NDB_LOCK_KEY(request.args(1));
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);

  std::vector<std::string> members;
  for (const auto& field : fields) {
    members.push_back(oset::EncodeMember(kmeta, version, field));
  }

  NSBatch batch(ns);
  uint64_t count = 0;
  auto results = ns->MultiGet(members, NULL);
  for (size_t i = 0; i < results.size(); i++) {
    const auto& m = members[i];
    const auto& r = results[i];
    if (r.IsNotFound()) {
      batch.Put(m);
      count++;
    } else if (!r.ok()) {
      return r;
    }
  }

  NDB_COMMAND_UPDATE_LENGTH(vmeta, +count);
  vmeta.SetConfigs(configs);
  vmeta.SetConfigs(options);  // options replace configs.
  batch.Put(kmeta, vmeta);
  NDB_TRY(batch.Commit());

  uint64_t start = 0, stop = 0;
  if (vmeta.ExceedMaxlen(&start, &stop)) {
    GenericOREMRANGEBYRANK(request, start, stop);
  }

  return Response::Int(count);
}

// OREM key field [field ...]
Response CommandOREM(const Request& request) {
  // Parse fields.
  std::set<int64_t> fields;
  if (!oset::ParseFields(request, 2, &fields)) {
    return Response::InvalidArgument();
  }

  NDB_LOCK_KEY(request.args(1));
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);

  std::vector<std::string> members;
  for (const auto& field : fields) {
    members.push_back(oset::EncodeMember(kmeta, version, field));
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

// OREMRANGEBYRANK key start stop
Response CommandOREMRANGEBYRANK(const Request& request) {
  int64_t start = 0, stop = 0;
  if (!ParseInt64(request.args(2), &start) ||
      !ParseInt64(request.args(3), &stop)) {
    return Response::InvalidArgument();
  }
  NDB_LOCK_KEY(request.args(1));
  return GenericOREMRANGEBYRANK(request, start, stop);
}

Response GenericORANGEGET(const Request& request,
                          int64_t min,
                          int64_t max,
                          int64_t start,
                          int64_t stop,
                          bool reverse) {
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);

  if (!CheckLimit(length, &start, &stop)) {
    return Response::Size(0);
  }

  std::vector<int64_t> fields;
  uint64_t offset = start, count = stop - start + 1;
  auto begin = oset::EncodeMember(kmeta, version, min);
  auto end = oset::EncodeMember(kmeta, version, max);

  auto it = ns->RangeGet(begin, end, offset, count, reverse);
  for (it->Seek(); it->Valid(); it->Next()) {
    int64_t v = 0;
    auto field = it->id();
    if (!RemovePrefix(&field) || !DecodeInt64(&field, &v)) {
      return NDB_COMMAND_ERROR("corruption");
    }
    fields.push_back(v);
  }
  NDB_TRY(it->result());

  auto res = Response::Size(fields.size());
  for (const auto& field : fields) {
    res.AppendInt(field);
  }
  return res;
}

Response GenericORANGE(const Request& request, bool reverse) {
  int64_t start = 0, stop = 0;
  if (!ParseInt64(request.args(2), &start) ||
      !ParseInt64(request.args(3), &stop)) {
    return Response::InvalidArgument();
  }
  return GenericORANGEGET(request, INT64_MIN, INT64_MAX, start, stop, reverse);
}

// ORANGE key start stop
Response CommandORANGE(const Request& request) {
  return GenericORANGE(request, false);
}

// OREVRANGE key start stop
Response CommandOREVRANGE(const Request& request) {
  return GenericORANGE(request, true);
}

Response GenericORANGEBYMEMBER(const Request& request, bool reverse) {
  int64_t min = 0, max = 0;
  if (!ParseRange(request, 2, &min, &max, reverse)) {
    return Response::InvalidArgument();
  }
  int64_t offset = 0, count = INT64_MAX;
  if (!ParseLimit(request, 4, &offset, &count)) {
    return Response::InvalidArgument();
  }
  int64_t start = offset, stop = 0;
  if (count > INT64_MAX - offset) {
    stop = INT64_MAX;
  } else {
    stop = offset + count - 1;
  }
  return GenericORANGEGET(request, min, max, start, stop, reverse);
}

// ORANGEBYMEMBER key min max [LIMIT offset count]
Response CommandORANGEBYMEMBER(const Request& request) {
  return GenericORANGEBYMEMBER(request, false);
}

// OREVRANGEBYMEMBER key max min [LIMIT offset count]
Response CommandOREVRANGEBYMEMBER(const Request& request) {
  return GenericORANGEBYMEMBER(request, true);
}

// OCARD key
Response CommandOCARD(const Request& request) {
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);
  return Response::Int(length);
}

// OGETMAXLEN key
Response CommandOGETMAXLEN(const Request& request) {
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);
  return Response::Int(vmeta.meta().maxlen());
}

}  // namespace ndb
