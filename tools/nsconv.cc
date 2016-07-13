#include "ndb/ndb.h"

using namespace ndb;

#define ERROR(fmt, ...) do {                    \
    fprintf(stderr, fmt, ## __VA_ARGS__);       \
    exit(EXIT_FAILURE);                         \
  } while (0)

class Writer {
 public:
  Writer(Engine* engine) : engine_(engine) {}

  void Put(NSRef ns, const Slice& id, const Slice& value) {
    if (batch_ == NULL) {
      batch_.reset(new Batch(engine_));
    }
    batch_->Put(ns, id, value);
    if (batch_->GetDataSize() > (1 << 20)) {
      std::unique_lock<std::mutex> lock(lock_);
      batches_.push_back(batch_.release());
    }
  }

  size_t Commit() {
    std::vector<Batch*> batches;
    {
      std::unique_lock<std::mutex> lock(lock_);
      batches.swap(batches_);
    }

    for (auto batch : batches) {
      NDB_ASSERT_OK(batch->Commit());
      delete batch;
    }

    return batches.size();
  }

  void Done() {
    std::unique_lock<std::mutex> lock(lock_);
    if (batch_ != NULL) {
      batches_.push_back(batch_.release());
    }
    done_ = true;
  }

  bool IsDone() const { return done_; }

 private:
  Engine* engine_ {NULL};
  std::unique_ptr<Batch> batch_;
  std::mutex lock_;
  bool done_ {false};
  std::vector<Batch*> batches_;
};

void Commit(Writer& writer) {
  while (!writer.IsDone()) {
    auto size = writer.Commit();
    if (size > 0) {
      printf("Commit batches %zu\n", size);
    } else {
      sleep(1);
    }
  }
  writer.Commit();
}

int main(int argc, char *argv[])
{
  if (argc != 3) {
    ERROR("Usage: nsconv <srcpath> <dstpath>\n");
  }

  // Open src.
  rocksdb::DB* db = NULL;
  auto s = rocksdb::DB::OpenForReadOnly(rocksdb::Options(), argv[1], &db);
  NDB_ASSERT_OK(StatusToResult(s));
  std::unique_ptr<rocksdb::DB> auto_free(db);
  rocksdb::ReadOptions ropts(false, false);
  std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(ropts));

  // Open dst.
  Engine::Options options;
  options.dbname = argv[2];
  options.WAL_size_limit = 0;
  options.WAL_ttl_seconds = 0;
  Engine engine(options);
  NDB_ASSERT_OK(engine.Open());

  Writer writer(&engine);
  std::thread commit(Commit, std::ref(writer));

  NSRef ns;
  size_t count = 0;
  std::map<std::string, size_t> nscount;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    auto k = it->key();
    auto v = it->value();

    if (k.size() == 0) {
      printf("k.size %zu\n", k.size());
      continue;
    }

    // Decode the prefix to parse the namespace.
    auto tmp = k;
    std::string prefix;
    if (!DecodeMeta(&tmp, &prefix)) {
      prefix.assign(k.data(), k.size());
    }

    std::string nsname, id;
    if (!ParseNamespace(prefix, &nsname, &id)) {
      printf("Parse namespace: ns %s id %s\n", nsname.c_str(), id.c_str());
    } else {
      k.remove_prefix(nsname.size()+1);
    }
    id.assign(k.data(), k.size());

    // Namespace changed.
    if (ns == NULL || ns->GetName() != nsname) {
      auto r = engine.NewNamespace(nsname);
      if (r.ok()) {
        printf("Create namespace %s id %s [prefix %s]\n", nsname.c_str(), id.c_str(), prefix.data());
      }
      ns = engine.GetNamespace(nsname);
    }

    count++;
    nscount[nsname]++;
    writer.Put(ns, id, v);

    if (count % 10000000 == 0) {
      printf("COUNT: %zu\n", count);
    }
  }

  // Join commit thread.
  writer.Done();
  commit.join();

  printf("DONE: namespaces %zu keys %zu\n", nscount.size(), count);
  for (const auto& it : nscount) {
    printf("namespace %s: keys %zu\n", it.first.c_str(), it.second);
  }
  return 0;
}
