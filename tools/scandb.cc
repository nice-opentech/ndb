#include <fstream>

#include "ndb/ndb.h"

using namespace ndb;

#define ERROR(fmt, ...) do {                    \
    fprintf(stderr, fmt, ## __VA_ARGS__);       \
    exit(EXIT_FAILURE);                         \
  } while (0)

int main(int argc, char *argv[])
{
  if (argc != 3) {
    ERROR("Usage: scandb <dbname> <output>\n");
  }

  auto dbname = argv[1];
  auto output = argv[2];

  // Open db.
  rocksdb::DB* db = NULL;
  auto s = rocksdb::DB::OpenForReadOnly(rocksdb::Options(), dbname, &db);
  NDB_ASSERT_OK(StatusToResult(s));
  std::unique_ptr<rocksdb::DB> auto_free(db);
  rocksdb::ReadOptions ropts(true, false);
  std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(ropts));

  int fileno = -1;
  std::ofstream file;

  size_t count = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next(), count++) {
    auto k = it->key();
    auto v = it->value();

    if (k.size() == 0) {
      printf("k.size %zu\n", k.size());
      continue;
    }

    // Decode the prefix to parse the namespace.
    std::string id;
    std::string kmeta;
    if (!DecodeMeta(&k, &kmeta)) {
      // Simple types
      id.assign(k.data(), k.size());
    } else {
      // Collection types.
      if (k.size() != 0) {
        continue;
      }
      id.assign(kmeta.data(), kmeta.size()-1);
    }

    Value value;
    auto r = value.Decode(v);
    if (r.IsNotFound()) {
      continue;
    }
    NDB_ASSERT_OK(r);

    if (fileno != (int) count / 100000000) {
      fileno = count / 100000000;
      auto filename = std::string(output) + "." + std::to_string(fileno);
      file.close();
      file.open(filename);
      printf("open %s\n", filename.c_str());
    }
    file << TypeName(value) << " " << id << std::endl;
  }

  printf("count %zu\n", count);
  return 0;
}
