#include "ndb/ndb.h"

using namespace ndb;
using namespace rocksdb;

Result Open(const std::string& dbname, DB** db, std::vector<ColumnFamilyHandle*>* handles) {
  DBOptions dbopts;
  std::vector<std::string> cfnames;
  auto s = DB::ListColumnFamilies(dbopts, dbname, &cfnames);
  if (!s.ok()) StatusToResult(s);

  std::vector<ColumnFamilyDescriptor> cfds;
  for (const auto& name : cfnames) {
    ColumnFamilyOptions cfopts;
    cfds.emplace_back(name, cfopts);
  }

  s = DB::OpenForReadOnly(dbopts, dbname, cfds, handles, db);
  return StatusToResult(s);
}

ColumnFamilyHandle* FindHandle(const std::vector<ColumnFamilyHandle*>& handles,
                               const std::string& nsname) {
  for (auto handle : handles) {
    if (handle->GetName() == nsname) {
      return handle;
    }
  }
  printf("Namespace %s not found.\n", nsname.c_str());
  exit(EXIT_FAILURE);
}

void CheckStr(DB* db, const std::vector<ColumnFamilyHandle*>& handles,
              const char* nsname, int count) {
  ReadOptions ropts;
  auto handle = FindHandle(handles, nsname);
  std::unique_ptr<Iterator> it(db->NewIterator(ropts, handle));
  it->SeekToFirst();

  for (int i = 0; i < count; i++) {
    // Deleted
    if (i % 3 == 0) continue;
    // Expired
    if (i % 8 == 0) continue;
    NDB_ASSERT(it->Valid());

    char id[128];
    snprintf(id, sizeof(id), "%08d", i);
    NDB_ASSERT(it->key().compare(id) == 0);

    char key[128];
    snprintf(key, sizeof(key), "%s:%08d", nsname, i);

    Value value;
    NDB_ASSERT_OK(value.Decode(it->value()));
    NDB_ASSERT(value.has_bytes() && value.bytes() == key);

    it->Next();
  }

  NDB_ASSERT(!it->Valid());
  printf("Check %s OK\n", nsname);
}

void CheckInt(DB* db, const std::vector<ColumnFamilyHandle*>& handles,
              const char* nsname, int count) {
  ReadOptions ropts;
  auto handle = FindHandle(handles, nsname);
  std::unique_ptr<Iterator> it(db->NewIterator(ropts, handle));
  it->SeekToFirst();

  for (int i = 0; i < count; i++) {
    // Deleted
    if (i % 3 == 0) continue;
    // Expired
    if (i % 8 == 0) continue;
    NDB_ASSERT(it->Valid());

    char id[128];
    snprintf(id, sizeof(id), "%08d", i);
    NDB_ASSERT(it->key().compare(id) == 0);

    int64_t v = 3;
    if (i % 2 == 0) v--;
    if (i % 4 == 0) v--;

    Value value;
    NDB_ASSERT_OK(value.Decode(it->value()));
    NDB_ASSERT(value.has_int64() && value.int64() == v);
    it->Next();
  }

  NDB_ASSERT(!it->Valid());
  printf("Check %s OK\n", nsname);
}

void CheckType(DB* db, const std::vector<ColumnFamilyHandle*>& handles,
               const char* nsname, int count, int members, Meta::Type type) {
  ReadOptions ropts;
  auto handle = FindHandle(handles, nsname);
  std::unique_ptr<Iterator> it(db->NewIterator(ropts, handle));
  it->SeekToFirst();

  for (int i = 0; i < count; i++) {
    // Deleted
    if (i % 3 == 0) continue;
    // Expired
    if (i % 8 == 0) continue;
    NDB_ASSERT(it->Valid());

    char id[9];
    snprintf(id, sizeof(id), "%08d", i);
    NDB_ASSERT(it->key().compare(Slice(id, sizeof(id))) == 0);

    uint64_t version = 1;
    if (i % 2 == 0) version++;
    if (i % 4 == 0) version++;

    Value vmeta;
    NDB_ASSERT_OK(vmeta.Decode(it->value()));
    NDB_ASSERT(vmeta.has_meta() && vmeta.meta().version() == version);

    for (int i = 0; i < members; i++) {
      it->Next();
      NDB_ASSERT(it->Valid());
      auto member = it->key();
      std::string kmeta;
      uint64_t v;
      uint8_t t, st;
      NDB_ASSERT(DecodePrefix(&member, &kmeta, &v, &t, &st));
      NDB_ASSERT(Slice(id, sizeof(id)).compare(kmeta) == 0);
      NDB_ASSERT(v == vmeta.meta().version());
      NDB_ASSERT(t == type);
    }

    it->Next();
  }

  NDB_ASSERT(!it->Valid());
  printf("Check %s OK\n", nsname);
}

int main(int argc, char* argv[])
{
  if (argc != 3) {
    fprintf(stderr, "Usage: compaction_check <dbname> <count>\n");
    exit(EXIT_FAILURE);
  }

  std::string dbname = argv[1];
  int count = atoi(argv[2]);

  DB* db = NULL;
  std::vector<ColumnFamilyHandle*> handles;
  NDB_ASSERT_OK(Open(dbname, &db, &handles));

  CheckStr(db,  handles, "str",  count);
  CheckInt(db,  handles, "int",  count);
  CheckType(db, handles, "set",  count, 3, Meta::SET);
  CheckType(db, handles, "hash", count, 3, Meta::HASH);
  CheckType(db, handles, "list", count, 3, Meta::LIST);
  CheckType(db, handles, "zset", count, 6, Meta::ZSET);
  CheckType(db, handles, "xset", count, 6, Meta::ZSET);
  CheckType(db, handles, "oset", count, 3, Meta::OSET);

  delete db;
}
