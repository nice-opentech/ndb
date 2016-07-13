#include "units/units.h"

std::string FormatMember(int i) {
  char member[1024];
  snprintf(member, sizeof(member), "member:%04d", i);
  return member;
}

void TestPut(NSRef ns, int begin, int end) {
  for (int i = begin; i < end; i++) {
    auto member = FormatMember(i);
    auto value = Value::FromInt64(i);
    NDB_ASSERT_OK(ns->Put(member, value));
    NDB_ASSERT_OK(ns->Get(member, &value));
    NDB_ASSERT(value.int64() == i);
  }
}

void TestDelete(NSRef ns, int begin, int end) {
  for (int i = begin; i < end; i++) {
    auto member = FormatMember(i);
    NDB_ASSERT_OK(ns->Delete(member));
    NDB_ASSERT(ns->Get(member, NULL).IsNotFound());
  }
}

void TestNSBatch(NSRef ns, int begin, int end) {
  // Put
  {
    NSBatch batch(ns);
    for (int i = begin; i < end; i++) {
      auto member = FormatMember(i);
      batch.Put(member, Value::FromBytes(member));
    }
    NDB_ASSERT_OK(batch.Commit());
  }

  // Get
  {
    for (int i = begin; i < end; i++) {
      auto member = FormatMember(i);
      Value value;
      NDB_ASSERT_OK(ns->Get(member, &value));
      NDB_ASSERT(value.bytes() == member);
    }
  }

  // Delete
  {
    NSBatch batch(ns);
    for (int i = begin; i < end; i++) {
      auto member = FormatMember(i);
      batch.Delete(member);
    }
    NDB_ASSERT_OK(batch.Commit());
  }

  // Get NotFound
  {
    for (int i = begin; i < end; i++) {
      auto member = FormatMember(i);
      NDB_ASSERT(ns->Get(member, NULL).IsNotFound());
    }
  }
}

void TestMultiGet(NSRef ns, int n, int step) {
  std::vector<std::string> members;
  for (int i = 0; i < n; i += step) {
    members.push_back(FormatMember(i));
  }
  std::vector<Value> values;
  auto results = ns->MultiGet(members, &values);
  for (size_t i = 0; i < results.size(); i++) {
    NDB_ASSERT_OK(results[i]);
    NDB_ASSERT(values[i].int64() == (int64_t) i * step);
  }
}

void TestRangeGet(NSRef ns, int n, int offset, int count, bool reverse) {
  {
    auto it = ns->RangeGet("member:", "member:", offset, count, reverse);
    it->Seek();
    NDB_ASSERT(!it->Valid());
  }
  {
    auto it = ns->RangeGet("member;", "member;", offset, count, reverse);
    it->Seek();
    NDB_ASSERT(!it->Valid());
  }
  {
    auto it = ns->RangeGet("member:0000", "member:0000", 2, 2, false);
    it->Seek();
    NDB_ASSERT(!it->Valid());
  }
  {
    auto it = ns->RangeGet("member:0001", "member:0001", 2, 2, true);
    it->Seek();
    NDB_ASSERT(!it->Valid());
  }

  auto it = ns->RangeGet("member:", "member;", offset, count, reverse);
  NDB_ASSERT(!it->Valid());
  NDB_ASSERT_OK(it->result());
  int c = 0;
  for (it->Seek(); it->Valid(); c++, it->Next()) {
    Value value;
    NDB_ASSERT_OK(value.Decode(it->value()));
    int i = reverse ? n - offset - 1 - c : offset + c;
    auto member = FormatMember(i);
    NDB_ASSERT(it->id().compare(member) == 0);
    NDB_ASSERT(value.int64() == i);
  }
  NDB_ASSERT(c == count);
}

void TestNamespace(Engine* engine, const std::string& name) {
  NDB_ASSERT(engine->GetNamespace(name) == NULL);
  NDB_ASSERT_OK(engine->NewNamespace(name));
  auto ns = engine->GetNamespace(name);

  const int n = 1000;
  TestPut(ns, 0, n);
  TestNSBatch(ns, n, n + n);
  TestMultiGet(ns, n, 10);
  TestRangeGet(ns, n, 10, 10, true);
  TestRangeGet(ns, n, 10, 10, false);
  TestDelete(ns, 0, n);

  NDB_ASSERT_OK(engine->DropNamespace(name));
}

int Test(int argc, char* argv[]) {
  auto engine = new Engine(Engine::Options());
  NDB_ASSERT_OK(engine->Open());

  TestNamespace(engine, "user");
  TestNamespace(engine, "show");
  TestNamespace(engine, "like");

  delete engine;
  system("rm -rf nicedb");
  return EXIT_SUCCESS;
}
