#include "ndb/ndb.h"

using namespace ndb;
using namespace rocksdb;

Result Open(const std::string& dbname, DB** db, std::vector<ColumnFamilyHandle*>* handles) {
  DBOptions dbopts;
  std::vector<std::string> cfnames;
  auto s = DB::ListColumnFamilies(dbopts, dbname, &cfnames);
  if (!s.ok()) return StatusToResult(s);

  std::vector<ColumnFamilyDescriptor> cfds;
  for (const auto& name : cfnames) {
    ColumnFamilyOptions cfopts;
    cfds.emplace_back(name, cfopts);
  }

  s = DB::OpenForReadOnly(dbopts, dbname, cfds, handles, db);
  return StatusToResult(s);
}

size_t Compare(Iterator* it1, Iterator* it2) {
  size_t count = 0;
  for (it1->SeekToFirst(), it2->SeekToFirst();
       it1->Valid() && it2->Valid();
       it1->Next(), it2->Next()) {
    NDB_ASSERT(it1->key().compare(it2->key()) == 0);
    NDB_ASSERT(it1->value().compare(it2->value()) == 0);
    count++;
  }
  NDB_ASSERT(!it1->Valid() && !it2->Valid());
  delete it1, delete it2;
  return count;
}

int main(int argc, char *argv[])
{
  if (argc != 3) {
    fprintf(stderr, "Usage: rocksdb_check <db1> <db2>\n");
    exit(EXIT_FAILURE);
  }

  DB* db1 = NULL;
  std::vector<ColumnFamilyHandle*> h1;
  NDB_ASSERT_OK(Open(argv[1], &db1, &h1));

  DB* db2 = NULL;
  std::vector<ColumnFamilyHandle*> h2;
  NDB_ASSERT_OK(Open(argv[2], &db2, &h2));

  NDB_ASSERT(h1.size() == h2.size());
  for (size_t i = 0; i < h1.size(); i++) {
    printf("compare db1:%s db2:%s\n",
           h1[i]->GetName().c_str(),
           h2[i]->GetName().c_str());
    NDB_ASSERT(h1[i]->GetName() == h2[i]->GetName());
    ReadOptions ropts;
    auto it1 = db1->NewIterator(ropts, h1[i]);
    auto it2 = db2->NewIterator(ropts, h2[i]);
    auto count = Compare(it1, it2);
    printf("compare %zu keys OK\n", count);
  }

  printf("OK\n");
  return 0;
}
