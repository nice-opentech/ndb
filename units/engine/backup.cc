#include "units/units.h"

std::string FormatMember(int i) {
  char member[1024];
  snprintf(member, sizeof(member), "member:%04d", i);
  return member;
}

void FillData(Engine* engine, int n) {
  auto ns = engine->GetNamespace("default");
  for (int i = 0; i < n; i++) {
    auto member = FormatMember(i);
    auto value = Value::FromBytes(member);
    NDB_ASSERT_OK(ns->Put(member, value));
  }
}

void CheckData(Engine* engine, int n) {
  auto ns = engine->GetNamespace("default");
  for (int i = 0; i < n; i++) {
    auto member = FormatMember(i);
    Value value;
    NDB_ASSERT_OK(ns->Get(member, &value));
    NDB_ASSERT(value.bytes() == member);
  }
}

void CheckWAL(Engine* engine, uint64_t begin, uint64_t end) {
  uint64_t sequence = begin;
  auto it = engine->NewWALIterator();
  for (it->Seek(begin); it->Valid(); it->Next()) {
    auto batch = it->batch();
    NDB_ASSERT(batch.sequence == sequence);
    sequence += batch.writeBatchPtr->Count();
  }
  NDB_ASSERT_OK(it->result());
}

void TestBackup(Engine* engine, const std::string& backup) {
  auto b = engine->GetBackup();
  NDB_ASSERT_OK(b->Create(backup));
  NDB_ASSERT(!b->IsFinished());
  printf("%s\n", b->GetStats().Print());
  NDB_ASSERT(!b->Create(backup).ok());
  while (!b->IsFinished()) sleep(1);
  printf("%s\n", b->GetStats().Print());
}

int Test(int argc, char* argv[]) {
  Engine::Options options;
  const int n = 10000;

  // Backup
  {
    auto engine = new Engine(options);
    NDB_ASSERT_OK(engine->Open());
    FillData(engine, n);
    CheckWAL(engine, 1, n);
    CheckWAL(engine, n/2, n);
    TestBackup(engine, "backup");
    delete engine;
  }

  NDB_ASSERT_OK(RestoreDB("backup", "restore"));

  // Restore
  {
    options.dbname = "restore";
    auto engine = new Engine(options);
    NDB_ASSERT_OK(engine->Open());
    CheckData(engine, n);
    delete engine;
  }

  system("rm -rf nicedb");
  system("rm -rf backup");
  system("rm -rf restore");
  return EXIT_SUCCESS;
}
