#include "units/units.h"

void TestMaster(const char* address, const char* cluster, const char* node) {
  Center::Options options;
  options.address = address;
  options.cluster = cluster;
  options.node = node;

  Center center(options);
  NDB_ASSERT_OK(center.PutMaster("0.0.0.0:9736"));

  Options opts;
  NDB_ASSERT_OK(center.GetOptions(&opts));
  opts.Usage();

  std::map<std::string, Configs> nss;
  NDB_ASSERT_OK(center.GetNamespaces(&nss));

  for (const auto& it : nss) {
    printf("[%s]\n", it.first.c_str());
    if (it.second.has_expire()) {
      printf("expire = %d\n", (int) it.second.expire());
    }
    if (it.second.has_maxlen()) {
      printf("maxlen = %d\n", (int) it.second.maxlen());
    }
    if (it.second.has_pruning()) {
      printf("pruning = %s\n", PruningName(it.second.pruning()));
    }
  }
}

void TestSlaves(const char* address, const char* cluster, const char* node) {
  Center::Options options;
  options.address = address;
  options.cluster = cluster;
  options.node = "s01";
  options.slaveof = node;

  Center center(options);
  NDB_ASSERT_OK(center.PutMaster("0.0.0.0:9737"));

  Options opts;
  NDB_ASSERT_OK(center.GetOptions(&opts));
  opts.Usage();
}

int Test(int argc, char *argv[])
{
  if (argc != 4) {
    fprintf(stderr, "Usage: %s <address> <cluster> <node>\n", argv[0]);
    return EXIT_FAILURE;
  }

  printf("[Test Master]\n");
  TestMaster(argv[1], argv[2], argv[3]);

  printf("[Test Slaves]\n");
  TestSlaves(argv[1], argv[2], argv[3]);

  return EXIT_SUCCESS;
}
