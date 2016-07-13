#include "units/units.h"

void TestParseInt64() {
  int64_t i = 0;
  NDB_ASSERT(ParseInt64("-123", &i) && i == -123);
  NDB_ASSERT(ParseInt64("123", &i) && i == 123);
  NDB_ASSERT(ParseInt64("321.", &i) && i == 321);
  NDB_ASSERT(ParseInt64("123.123", &i) && i == 123);
  NDB_ASSERT(!ParseInt64("", &i));
  NDB_ASSERT(!ParseInt64(" 123", &i));
  NDB_ASSERT(!ParseInt64("123 ", &i));
  NDB_ASSERT(!ParseInt64("12 3", &i));
}

void TestParseUint64() {
  uint64_t u = 0;
  NDB_ASSERT(ParseUint64("+123", &u) && u == 123);
  NDB_ASSERT(ParseUint64("123", &u) && u == 123);
  NDB_ASSERT(!ParseUint64("321.", &u));
  NDB_ASSERT(!ParseUint64("123.123", &u));
  NDB_ASSERT(!ParseUint64("", &u));
  NDB_ASSERT(!ParseUint64(" 123", &u));
  NDB_ASSERT(!ParseUint64("123 ", &u));
  NDB_ASSERT(!ParseUint64("12 3", &u));
}

void TestParsePruning() {
  Pruning pruning;
  NDB_ASSERT(ParsePruning("min", &pruning) && pruning == Pruning::MIN);
  NDB_ASSERT(ParsePruning("MIN", &pruning) && pruning == Pruning::MIN);
  NDB_ASSERT(ParsePruning("minscore", &pruning) && pruning == Pruning::MIN);
  NDB_ASSERT(ParsePruning("MINSCORE", &pruning) && pruning == Pruning::MIN);
  NDB_ASSERT(ParsePruning("max", &pruning) && pruning == Pruning::MAX);
  NDB_ASSERT(ParsePruning("MAX", &pruning) && pruning == Pruning::MAX);
  NDB_ASSERT(ParsePruning("maxscore", &pruning) && pruning == Pruning::MAX);
  NDB_ASSERT(ParsePruning("MAXSCORE", &pruning) && pruning == Pruning::MAX);
  NDB_ASSERT(!ParsePruning("minmax", &pruning));
}

void TestParseNamespace() {
  std::string name, id;
  NDB_ASSERT(ParseNamespace("ns:abc", &name, &id) && name == "ns" && id == "abc");
  NDB_ASSERT(ParseNamespace("ns:abc:cba", &name, &id) && name == "ns" && id == "abc:cba");
  NDB_ASSERT(!ParseNamespace(":abc", &name, &id) && name == "default" && id == ":abc");
  NDB_ASSERT(!ParseNamespace("abc:", &name, &id) && name == "default" && id == "abc:");
  NDB_ASSERT(!ParseNamespace(":abc:", &name, &id) && name == "default" && id == ":abc:");
  NDB_ASSERT(!ParseNamespace("ns abc", &name, &id) && name == "default" && id == "ns abc");
  NDB_ASSERT(ParseNamespace("ns_123", &name, &id) && name == "ns" && id == "123");
  NDB_ASSERT(ParseNamespace("ns_123_321", &name, &id) && name == "ns" && id == "123_321");
  NDB_ASSERT(ParseNamespace("ns_123_321_123", &name, &id) && name == "ns" && id == "123_321_123");
  NDB_ASSERT(ParseNamespace("ns_abc_123_321", &name, &id) && name == "ns_abc" && id == "123_321");
  NDB_ASSERT(ParseNamespace("ns_123_abc_321", &name, &id) && name == "ns" && id == "123_abc_321");
  NDB_ASSERT(!ParseNamespace("_ns", &name, &id) && name == "default" && id == "_ns");
  NDB_ASSERT(!ParseNamespace("ns_", &name, &id) && name == "default" && id == "ns_");
  NDB_ASSERT(!ParseNamespace("ns_abc", &name, &id) && name == "default" && id == "ns_abc");
  Slice s("ns_abc_123", strlen("ns_abc_123"));
  NDB_ASSERT(ParseNamespace(s, &name, &id) && name == "ns_abc" && id == "123");
  Slice zero;
  NDB_ASSERT(!ParseNamespace(zero, &name, &id) && name == "default" && id == "");
}

void TestParseLimit() {
  int64_t offset, count;
  NDB_ASSERT(!ParseLimit(Request({"test"}), 0, &offset, &count));
  NDB_ASSERT(!ParseLimit(Request({"limit", "1"}), 0, &offset, &count));
  NDB_ASSERT(ParseLimit(Request({}), 0, &offset, &count));
  NDB_ASSERT(offset == 0 && count == INT64_MAX);
  NDB_ASSERT(ParseLimit(Request({"LIMIT", "2", "3"}), 0, &offset, &count));
  NDB_ASSERT(offset == 2 && count == 3);
}

void TestCheckLimit() {
  int64_t start = 1, stop = 2;
  NDB_ASSERT(!CheckLimit(0, &start, &stop));

  start = 0, stop = 0;
  NDB_ASSERT(CheckLimit(1, &start, &stop) && start == 0 && stop == 0);

  start = -1, stop = -1;
  NDB_ASSERT(CheckLimit(1, &start, &stop) && start == 0 && stop == 0);

  start = -1, stop = -2;
  NDB_ASSERT(!CheckLimit(1, &start, &stop));

  start = -2, stop = -1;
  NDB_ASSERT(CheckLimit(1, &start, &stop) && start == 0 && stop == 0);

  start = 2, stop = 1;
  NDB_ASSERT(!CheckLimit(1, &start, &stop));

  start = 2, stop = 4;
  NDB_ASSERT(!CheckLimit(2, &start, &stop));

  start = 2, stop = 4;
  NDB_ASSERT(CheckLimit(3, &start, &stop) && start == 2 && stop == 2);
}

void TestParseRange() {
  int64_t min, max;
  NDB_ASSERT(!ParseRange(Request({"1"}), 0, &min, &max, false));
  NDB_ASSERT(ParseRange(Request({"1", "2"}), 0, &min, &max, false));
  NDB_ASSERT(min == 1 && max == 2);
  NDB_ASSERT(ParseRange(Request({"2", "1"}), 0, &min, &max, true));
  NDB_ASSERT(min == 1 && max == 2);
}

void TestCheckIncrementBound() {
  NDB_ASSERT(CheckIncrementBound(INT64_MAX - 2, 1));
  NDB_ASSERT(CheckIncrementBound(1, INT64_MAX - 2));
  NDB_ASSERT(CheckIncrementBound(INT64_MIN + 2, -1));
  NDB_ASSERT(CheckIncrementBound(-1, INT64_MIN + 2));
  NDB_ASSERT(!CheckIncrementBound(INT64_MAX, 1));
  NDB_ASSERT(!CheckIncrementBound(1, INT64_MAX));
  NDB_ASSERT(!CheckIncrementBound(INT64_MIN, -1));
  NDB_ASSERT(!CheckIncrementBound(-1, INT64_MIN));
}

void TestFindNextSuccessor() {
  std::string begin = "begin";
  auto next = FindNextSuccessor(begin);
  NDB_ASSERT(next == "begio");
  begin.push_back('\0');
  auto end = FindNextSuccessor(begin);
  begin[begin.size()-1] = 1;
  NDB_ASSERT(end == begin);
  begin.push_back(0xff);
  auto t = FindNextSuccessor(begin);
  begin[begin.size()-2] = 2;
  NDB_ASSERT(t == begin);
}

void TestBatchPut(Engine* engine, const std::vector<std::string>& nsnames, int n) {
  Batch batch(engine);
  for (int i = 0; i < n; i++) {
    for (auto nsname : nsnames) {
      auto ns = engine->GetNamespace(nsname);
      auto id = std::to_string(i);
      batch.Put(ns, id, Value::FromBytes(id));
    }
  }
  NDB_ASSERT_OK(batch.Commit());
}

void TestBatchGet(Engine* engine, const std::vector<std::string>& nsnames, int n) {
  for (int i = 0; i < n; i++) {
    for (auto nsname : nsnames) {
      auto ns = engine->GetNamespace(nsname);
      auto id = std::to_string(i);
      Value value;
      NDB_ASSERT_OK(ns->Get(id, &value));
      NDB_ASSERT(value.bytes() == id);
    }
  }
}

void TestBatchDelete(Engine* engine, const std::vector<std::string>& nsnames, int n) {
  Batch batch(engine);
  for (int i = 0; i < n; i++) {
    for (auto nsname : nsnames) {
      auto ns = engine->GetNamespace(nsname);
      auto id = std::to_string(i);
      batch.Delete(ns, id);
    }
  }
  NDB_ASSERT_OK(batch.Commit());
}

void TestGenericMGET() {
  auto engine = new Engine(Engine::Options());
  NDB_ASSERT_OK(engine->Open());

  std::vector<std::string> nsnames {"a", "b", "c"};
  for (auto nsname : nsnames) {
    NDB_ASSERT_OK(engine->NewNamespace(nsname));
  }

  const int n = 1000, step = 10;
  TestBatchPut(engine, nsnames, n);
  TestBatchGet(engine, nsnames, n);

  {
    std::vector<std::string> members;
    for (int i = 0; i < n; i += step) {
      for (auto nsname : nsnames) {
        members.push_back(nsname + ":" + std::to_string(i));
      }
    }
    std::vector<Value> values;
    auto results = GenericMGET(engine, {members.begin(), members.end()}, &values);
    for (size_t i = 0; i < results.size(); i++) {
      NDB_ASSERT_OK(results[i]);
      NDB_ASSERT(values[i].bytes() == std::to_string(i / nsnames.size() * step));
    }
  }

  {
    std::vector<std::string> members;
    for (int i = 0; i < n; i += step) {
      members.push_back("not_exists:" + std::to_string(i));
    }
    std::vector<Value> values;
    auto results = GenericMGET(engine, {members.begin(), members.end()}, &values);
    for (size_t i = 0; i < results.size(); i++) {
      NDB_ASSERT(results[i].IsNotFound());
    }
  }

  TestBatchDelete(engine, nsnames, n);

  delete engine;
  system("rm -rf nicedb");
}

int Test(int argc, char* argv[]) {
  TestParseInt64();
  TestParseUint64();
  TestParsePruning();
  TestParseNamespace();
  TestParseLimit();
  TestCheckLimit();
  TestParseRange();
  TestCheckIncrementBound();
  TestFindNextSuccessor();
  TestGenericMGET();
  return EXIT_SUCCESS;
}
