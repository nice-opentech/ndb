#include "ndb/ndb.h"

namespace ndb {

namespace zset {

#define NDB_TRY_GETMETA_BYKEY(k, ns, kmeta, vmeta)                  \
  NDB_TRY_GETMETA_TYPE_BYKEY(k, ns, kmeta, vmeta, Meta::ZSET)

std::string EncodeMemberPrefix(const Slice& kmeta, uint64_t version) {
  return ndb::EncodePrefix(kmeta, version, Meta::ZSET, 1);
}

std::string EncodeMember(const Slice& kmeta, uint64_t version, const Slice& field) {
  std::string dst = EncodeMemberPrefix(kmeta, version);
  dst.append(field.data(), field.size());
  return dst;
}

std::string EncodeScorePrefix(const Slice& kmeta, uint64_t version, int64_t score) {
  std::string dst = ndb::EncodePrefix(kmeta, version, Meta::ZSET, 2);
  EncodeInt64(&dst, score);
  return dst;
}

std::string EncodeScore(const Slice& kmeta, uint64_t version, int64_t score, const Slice& field) {
  std::string dst = EncodeScorePrefix(kmeta, version, score);
  dst.append(field.data(), field.size());
  return dst;
}

bool ParseFields(const Request& request, size_t idx,
                 std::vector<Slice>* fields,
                 std::vector<int64_t>* scores = NULL) {
  class Compare {
   public:
    bool operator()(const Slice& a, const Slice& b) {
      return a.compare(b) < 0;
    }
  };
  std::set<Slice, Compare> set;

  auto step = (scores == NULL) ? 1 : 2;

  if (idx == request.argc() || (request.argc() - idx) % step != 0) {
    return false;
  }

  for (size_t i = request.argc() - 1; i >= idx; i -= step) {
    if (!set.insert(request.args(i)).second) {
      continue;
    }
    fields->push_back(request.args(i));
    if (scores != NULL) {
      int64_t score = 0;
      if (!ParseInt64(request.args(i-1), &score)) {
        return false;
      }
      // Reserve INT64_MIN for -inf, INT64_MAX for +inf.
      if (score == INT64_MIN || score == INT64_MAX) {
        return false;
      }
      scores->push_back(score);
    }
  }

  return true;
}

}  // namespace zset

// NOTE: Require external lock.
Response GenericZREMRANGEBYRANK(const Request& request,
                                int64_t min,
                                int64_t max,
                                int64_t start,
                                int64_t stop) {
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);

  // We don't have the field part, so we need to make the score bigger.
  if (max < INT64_MAX) {
    max += 1;
  }

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
  auto begin = zset::EncodeScorePrefix(kmeta, version, min);
  auto end = zset::EncodeScorePrefix(kmeta, version, max);
  auto it = ns->RangeGet(begin, end, offset, count, reverse);
  for (it->Seek(), count = 0; it->Valid(); it->Next(), count++) {
    int64_t score = 0;
    auto field = it->id();
    if (!RemovePrefix(&field) || !DecodeInt64(&field, &score)) {
      return NDB_COMMAND_ERROR("corruption");
    }
    batch.Delete(zset::EncodeMember(kmeta, version, field));
    batch.Delete(zset::EncodeScore(kmeta, version, score, field));
  }
  NDB_TRY(it->result());

  NDB_COMMAND_UPDATE_LENGTH(vmeta, -count);
  vmeta.SetConfigs(configs);
  batch.Put(kmeta, vmeta);
  NDB_TRY(batch.Commit());
  return Response::Int(count);
}

Response GenericZADD(const Request& request, int64_t default_maxlen) {
  size_t idx = 2;

  // Parse options.
  int flags = 0;
  Configs options;
  for (size_t i = idx; i < request.argc(); i++) {
    auto opt = request.args(i).c_str();
    if (strcasecmp(opt, "NX") == 0) {
      flags |= NDB_OPTION_NX;
    } else if (strcasecmp(opt, "XX") == 0) {
      flags |= NDB_OPTION_XX;
    } else if (strcasecmp(opt, "CH") == 0) {
      flags |= NDB_OPTION_CH;
    } else if (strcasecmp(opt, "INCR") == 0) {
      return Response::InvalidOption();
    } else if (strcasecmp(opt, "MAXLEN") == 0 || strcasecmp(opt, "FINITY") == 0) {
      uint64_t maxlen;
      if (++i >= request.argc() || !ParseUint64(request.args(i), &maxlen)) {
        return Response::InvalidArgument();
      }
      options.set_maxlen(maxlen);
    } else if (strcasecmp(opt, "PRUNING") == 0) {
      Pruning pruning;
      if (++i >= request.argc() || !ParsePruning(request.args(i), &pruning)) {
        return Response::InvalidArgument();
      }
      options.set_pruning(pruning);
    } else {
      idx = i;
      break;
    }
  }
  if ((flags & NDB_OPTION_NX) && (flags & NDB_OPTION_XX)) {
    return Response::InvalidOption();
  }

  // Parse fields.
  std::vector<Slice> fields;
  std::vector<int64_t> scores;
  if (!zset::ParseFields(request, idx, &fields, &scores)) {
    return Response::InvalidArgument();
  }

  NDB_LOCK_KEY(request.args(1));
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);

  // Precedence: options > configs > default
  if (default_maxlen != 0) {
    if (!configs.has_maxlen()) {
      configs.set_maxlen(default_maxlen);
    }
  }

  std::vector<std::string> members;
  for (const auto& field : fields) {
    members.push_back(zset::EncodeMember(kmeta, version, field));
  }

  NSBatch batch(ns);
  uint64_t count = 0, changed_count = 0;
  std::vector<Value> values;
  auto results = ns->MultiGet(members, &values);
  for (size_t i = 0; i < results.size(); i++) {
    const auto& s = scores[i];
    const auto& f = fields[i];
    const auto& v = values[i];
    const auto& r = results[i];
    if (r.IsNotFound()) {
      if (flags & NDB_OPTION_XX) {
        continue;
      }
      count++;
    } else if (r.ok()) {
      if (flags & NDB_OPTION_NX) {
        continue;
      }
      if (v.has_int64()) {
        if (v.int64() == s) {
          continue;
        }
        // Delete previous score.
        batch.Delete(zset::EncodeScore(kmeta, version, v.int64(), f));
      }
      changed_count++;
    } else {
      return r;
    }
    batch.Put(members[i], Value::FromInt64(s));
    batch.Put(zset::EncodeScore(kmeta, version, s, f));
  }

  NDB_COMMAND_UPDATE_LENGTH(vmeta, +count);
  vmeta.SetConfigs(configs);
  vmeta.SetConfigs(options);  // options replace configs.
  batch.Put(kmeta, vmeta);
  NDB_TRY(batch.Commit());

  uint64_t start, stop;
  if (vmeta.ExceedMaxlen(&start, &stop)) {
    GenericZREMRANGEBYRANK(request, INT64_MIN, INT64_MAX, start, stop);
  }

  if (flags & NDB_OPTION_CH) {
    return Response::Int(count + changed_count);
  }
  return Response::Int(count);
}

// ZADD key [NX|XX] [CH] [INCR] [MAXLEN maxlen] [PRUNING min|max] score field [score field ...]
Response CommandZADD(const Request& request) {
  return GenericZADD(request, 0);
}
Response CommandXADD(const Request& request) {
  return GenericZADD(request, 2000);
}

// ZREM key field [field ...]
Response CommandZREM(const Request& request) {
  // Parse fields.
  std::vector<Slice> fields;
  if (!zset::ParseFields(request, 2, &fields)) {
    return Response::InvalidArgument();
  }

  NDB_LOCK_KEY(request.args(1));
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);

  std::vector<std::string> members;
  for (const auto& field : fields) {
    members.push_back(zset::EncodeMember(kmeta, version, field));
  }

  NSBatch batch(ns);
  uint64_t count = 0;
  std::vector<Value> values;
  auto results = ns->MultiGet(members, &values);
  for (size_t i = 0; i < results.size(); i++) {
    const auto& f = fields[i];
    const auto& v = values[i];
    const auto& r = results[i];
    if (r.ok()) {
      batch.Delete(members[i]);
      batch.Delete(zset::EncodeScore(kmeta, version, v.int64(), f));
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

// ZREMRANGEBYRANK key start stop
Response CommandZREMRANGEBYRANK(const Request& request) {
  int64_t start, stop;
  if (!ParseInt64(request.args(2), &start) ||
      !ParseInt64(request.args(3), &stop)) {
    return Response::InvalidArgument();
  }
  NDB_LOCK_KEY(request.args(1));
  return GenericZREMRANGEBYRANK(request, INT64_MIN, INT64_MAX, start, stop);
}

// ZREMRANGEBYSCORE key min max
Response CommandZREMRANGEBYSCORE(const Request& request) {
  int64_t min, max;
  if (!ParseRange(request, 2, &min, &max)) {
    return Response::InvalidArgument();
  }
  NDB_LOCK_KEY(request.args(1));
  return GenericZREMRANGEBYRANK(request, min, max, 0, INT64_MAX);
}

// ZINCRBY key increment member
Response CommandZINCRBY(const Request& request) {
  int64_t increment = 0;
  if (!ParseInt64(request.args(2), &increment)) {
    return Response::InvalidArgument();
  }

  NDB_LOCK_KEY(request.args(1));
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);

  Value value;
  auto member = zset::EncodeMember(kmeta, version, request.args(3));
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
  value.set_int64(origin + increment);

  NSBatch batch(ns);
  batch.Delete(zset::EncodeScore(kmeta, version, origin, request.args(3)));
  batch.Put(zset::EncodeScore(kmeta, version, value.int64(), request.args(3)));
  batch.Put(member, value);
  vmeta.SetConfigs(configs);
  batch.Put(kmeta, vmeta);
  NDB_TRY(batch.Commit());
  return Response::Bulk(value.int64());
}

Response GenericZRANGEGET(const Request& request,
                          int64_t min, int64_t max,
                          int64_t start, int64_t stop,
                          bool reverse, bool with_scores) {
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);

  // We don't have the field part, so we need to make the score bigger.
  if (max < INT64_MAX) {
    max += 1;
  }

  if (!CheckLimit(length, &start, &stop)) {
    return Response::Size(0);
  }

  std::vector<std::string> fields;
  std::vector<int64_t> scores;
  uint64_t offset = start, count = stop - start + 1;
  auto begin = zset::EncodeScorePrefix(kmeta, version, min);
  auto end = zset::EncodeScorePrefix(kmeta, version, max);

  auto it = ns->RangeGet(begin, end, offset, count, reverse);
  for (it->Seek(); it->Valid(); it->Next()) {
    int64_t score = 0;
    auto field = it->id();
    if (!RemovePrefix(&field) || !DecodeInt64(&field, &score)) {
      return NDB_COMMAND_ERROR("corruption");
    }
    fields.emplace_back(field.data(), field.size());
    scores.push_back(score);
  }
  NDB_TRY(it->result());

  auto res = Response::Size((1 + with_scores) * fields.size());
  for (size_t i = 0; i < fields.size(); i++) {
    res.AppendBulk(fields[i]);
    if (with_scores) {
      res.AppendBulk(scores[i]);
    }
  }
  return res;
}

Response GenericZRANGE(const Request& request, bool reverse) {
  int64_t start, stop;
  if (!ParseInt64(request.args(2), &start) ||
      !ParseInt64(request.args(3), &stop)) {
    return Response::InvalidArgument();
  }

  bool with_scores = false;
  if (request.argc() == 5) {
    if (strcasecmp(request.args(4).c_str(), "WITHSCORES") == 0) {
      with_scores = true;
    } else {
      return Response::InvalidOption();
    }
  } else if (request.argc() > 5) {
    return Response::InvalidArgument();
  }

  return GenericZRANGEGET(request, INT64_MIN, INT64_MAX, start, stop, reverse, with_scores);
}

// ZRANGE key start stop [WITHSCORES]
Response CommandZRANGE(const Request& request) {
  return GenericZRANGE(request, false);
}

// ZREVRANGE key start stop [WITHSCORES]
Response CommandZREVRANGE(const Request& request) {
  return GenericZRANGE(request, true);
}

Response GenericZRANGEBYSCORE(const Request& request, bool reverse) {
  size_t idx = 2;

  // Parse [min max] or [max min]
  int64_t min, max;
  if (!ParseRange(request, idx, &min, &max, reverse)) {
    return Response::InvalidArgument();
  }
  idx += 2;

  // Parse options
  bool with_scores = false;
  int64_t offset = 0, count = INT64_MAX;
  for (size_t i = 4; i < request.argc(); i++) {
    auto opt = request.args(i).c_str();
    if (strcasecmp(opt, "WITHSCORES") == 0) {
      with_scores = true;
    } else if (strcasecmp(opt, "LIMIT") == 0) {
      if (!ParseLimit(request, i, &offset, &count)) {
        return Response::InvalidArgument();
      }
      i += 2;
    } else {
      idx = i;
      break;
    }
  }

  int64_t start = offset, stop = 0;
  if (count > INT64_MAX - offset) {
    stop = INT64_MAX;
  } else {
    stop = offset + count - 1;
  }

  return GenericZRANGEGET(request, min, max, start, stop, reverse, with_scores);
}

// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
Response CommandZRANGEBYSCORE(const Request& request) {
  return GenericZRANGEBYSCORE(request, false);
}

// ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
Response CommandZREVRANGEBYSCORE(const Request& request) {
  return GenericZRANGEBYSCORE(request, true);
}

// ZSCORE key field
Response CommandZSCORE(const Request& request) {
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);
  if (length == 0) return Response::Null();

  auto member = zset::EncodeMember(kmeta, version, request.args(2));
  Value value;
  NDB_TRY(ns->Get(member, &value));
  return Response::Bulk(value.int64());
}

// ZCARD key
Response CommandZCARD(const Request& request) {
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);
  return Response::Int(length);
}

// ZGETMAXLEN key
Response CommandZGETMAXLEN(const Request& request) {
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);
  return Response::Int(vmeta.meta().maxlen());
}

}  // namespace ndb
