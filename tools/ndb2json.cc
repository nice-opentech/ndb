#include "ndb/ndb.h"

using namespace ndb;
using namespace rocksdb;

#define ERROR(fmt, ...) do {                    \
    fprintf(stderr, fmt "\n", ## __VA_ARGS__);  \
    exit(EXIT_FAILURE);                         \
  } while (0)

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

Result Convert(DB* db, ColumnFamilyHandle* handle) {
  ReadOptions ropts;
  std::unique_ptr<Iterator> it(db->NewIterator(ropts, handle));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::string id(it->key().data(), it->key().size());

    Value value;
    auto r = value.Decode(it->value());
    if (!r.ok()) {
      NDB_ASSERT(r.IsNotFound());
      continue;
    }

    int64_t counter = 0;
    NDB_ASSERT(ConvertValueToInt64(value, &counter));

    rapidjson::Document d;
    d.SetObject();
    rapidjson::Value k;
    k.SetString(id.data(), id.size());
    rapidjson::Value v;
    v.AddMember(k, counter, d.GetAllocator());
    rapidjson::Value n;
    n.SetString(handle->GetName().data(), handle->GetName().size());
    d.AddMember(n, v, d.GetAllocator());

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    d.Accept(writer);

    printf("%s\n", buffer.GetString());
  }
  return StatusToResult(it->status());
}

int main(int argc, char *argv[])
{
  if (argc != 2 && argc != 3) {
    ERROR("Usage: ndb2json <dbname> [<backup>]");
  }
  if (argc == 3) {
    NDB_ASSERT_OK(RestoreDB(argv[2], argv[1]));
  }

  DB* db = NULL;
  std::vector<ColumnFamilyHandle*> handles;
  NDB_ASSERT_OK(Open(argv[1], &db, &handles));

  for (auto handle : handles) {
    NDB_ASSERT_OK(Convert(db, handle));
  }

  return 0;
}
