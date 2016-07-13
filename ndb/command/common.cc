#include "ndb/ndb.h"

namespace ndb {

NSStats nsstats;

bool ParseInt64(const std::string& s, int64_t* i) {
  if (s.size() == 0 || isspace(s.front()) || isspace(s.back())) {
    return false;
  }

  errno = 0;
  char* end = NULL;
  *i = strtoll(s.data(), &end, 10);
  if (errno != 0) return false;

  if (*end == '.') {
    errno = 0;
    strtoll(end + 1, &end, 10);
    if (errno != 0) return false;
  }
  return *end == '\0';
}

bool ParseUint64(const std::string& s, uint64_t* u) {
  if (s.size() == 0 || isspace(s.front()) || isspace(s.back())) {
    return false;
  }
  if (s.front() == '-') {
    return false;
  }

  errno = 0;
  char* end = NULL;
  *u = strtoull(s.data(), &end, 10);
  if (errno != 0) return false;
  return *end == '\0';
}

bool ParsePruning(const std::string& s, Pruning* pruning) {
  if (strcasecmp(s.data(), "MIN") == 0 ||
      strcasecmp(s.data(), "MINSCORE") == 0) {
    *pruning = Pruning::MIN;
    return true;
  }
  if (strcasecmp(s.data(), "MAX") == 0 ||
      strcasecmp(s.data(), "MAXSCORE") == 0) {
    *pruning = Pruning::MAX;
    return true;
  }
  return false;
}

// Locates the first occurance of c in the slice s.
inline size_t sfind(const Slice& s, char c) {
  for (size_t i = 0; i < s.size(); i++) {
    if (s[i] == c) {
      return i;
    }
  }
  return s.size();
}

// Split slice s into [begin, pos) and (pos, end].
inline void ssplit(const Slice& s, size_t pos, std::string* foo, std::string* bar) {
  foo->assign(s.data(), pos);
  bar->assign(s.data() + pos + 1, s.size() - pos - 1);
}

// Remove the front part seperated by '_' if it is not an uint64.
bool RemoveFront(Slice* s) {
  if (s->size() == 0) {
    return false;
  }

  auto pos = sfind(*s, '_');

  int64_t v = 0;
  std::string id(s->data(), pos);
  if (ParseInt64(id, &v)) return false;

  s->remove_prefix(std::min(pos + 1, s->size()));
  return true;
}

bool ParseNamespace(const Slice& s, std::string* nsname, std::string* id) {
  // Parse as nsname:id.
  auto pos = sfind(s, ':');
  // Both nsname and id should not be empty.
  if (pos > 0 && pos < s.size() - 1) {
    ssplit(s, pos, nsname, id);
    return true;
  }

  // Parse as nsname_id[_id]*
  auto remain = s;
  while (RemoveFront(&remain));
  // Both nsname and id should not be empty.
  if (remain.size() > 0 && remain.size() < s.size() - 1) {
    ssplit(s, s.size() - remain.size() - 1, nsname, id);
    return true;
  }

  nsname->assign("default");
  id->assign(s.data(), s.size());
  return false;
}

bool ParseLimit(const Request& request, size_t idx, int64_t* offset, int64_t* count) {
  *offset = 0, *count = INT64_MAX;

  if (request.argc() == idx) return true;
  if (request.argc() <= idx + 2) return false;

  if (strcasecmp(request.args(idx).c_str(), "LIMIT") != 0) {
    return false;
  }
  if (!ParseInt64(request.args(idx+1), offset)) {
    return false;
  }
  if (!ParseInt64(request.args(idx+2), count)) {
    return false;
  }

  return *offset >= 0 && *count >= 0;
}

bool CheckLimit(int64_t length, int64_t* start, int64_t* stop) {
  if (length <= 0) return false;

  if (*start < 0) *start += length;
  if (*start < 0) *start = 0;

  if (*stop < 0) *stop += length;
  if (*stop >= length) *stop = length - 1;
  if (*stop < *start) return false;

  NDB_ASSERT(0 <= *start && *start <= *stop && *stop < length);
  return true;
}

static bool ParseMin(const std::string& s, int64_t* i) {
  auto p = s.c_str();
  if (s[0] == '(') {
    p++;
  }

  if (strcasecmp(p, "-inf") == 0) {
    *i = INT64_MIN;
  } else if (!ParseInt64(p, i)) {
    return false;
  }

  if (s[0] == '(') {
    if (*i < INT64_MAX) {
      *i += 1;
    }
  }

  return true;
}

static bool ParseMax(const std::string& s, int64_t* i) {
  auto p = s.c_str();
  if (s[0] == '(') {
    p++;
  }

  if (strcasecmp(p, "+inf") == 0) {
    *i = INT64_MAX;
  } else if (!ParseInt64(p, i)) {
    return false;
  }

  if (s[0] == '(') {
    if (*i > INT64_MIN) {
      *i -= 1;
    }
  }

  return true;
}

bool ParseRange(const Request& request, size_t idx, int64_t* min, int64_t* max, bool reverse) {
  if (request.argc() <= idx + 1) {
    return false;
  }
  if (!reverse) {
    return ParseMin(request.args(idx), min) && ParseMax(request.args(idx+1), max);
  } else {
    return ParseMax(request.args(idx), max) && ParseMin(request.args(idx+1), min);
  }
}

bool CheckIncrementBound(int64_t origin, int64_t increment) {
  if ((origin < 0 && increment < 0 && increment < (INT64_MIN - origin)) ||
      (origin > 0 && increment > 0 && increment > (INT64_MAX - origin))) {
    return false;
  }
  return true;
}

bool ConvertValueToInt64(const Value& value, int64_t* int64) {
  if (value.has_int64()) {
    *int64 = value.int64();
    return true;
  }
  if (value.has_bytes()) {
    return ParseInt64(value.bytes(), int64);
  }
  return false;
}

Response ConvertValueToBulk(const Value& value) {
  if (value.has_int64()) {
    return Response::Bulk(value.int64());
  }
  if (value.has_bytes()) {
    return Response::Bulk(value.bytes());
  }
  return Response::Null();
}

std::string FindNextSuccessor(const std::string& s) {
  auto t = s;
  // Find last character that can be incrementd.
  for (auto i = t.size(); i > 0; i--) {
    uint8_t byte = t[i-1];
    if (byte != (uint8_t)(0xff)) {
      t[i-1] = byte + 1;
      break;
    }
  }
  return t;
}

std::vector<Result> GenericMGET(Engine* engine, const std::vector<Slice>& keys, std::vector<Value>* values) {
  std::vector<NSRef> namespaces;
  std::vector<std::string> ids;
  for (const auto& k : keys) {
    std::string nsname, id;
    ParseNamespace(k, &nsname, &id);
    auto ns = engine->GetNamespace(nsname);
    if (ns == NULL) {
      // FIXME: This is ugly.
      ns = engine->GetNamespace("default");
      id = "__this_id_does_not_exist__";
    }
    namespaces.push_back(ns);
    ids.push_back(id);
  }
  return engine->MultiGet(namespaces, ids, values);
}

}  // namespace ndb
