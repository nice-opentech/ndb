#include "units/units.h"

int Test(int argc, char* argv[]) {
  const size_t n = 1000000;
  std::set<uint32_t> ids;
  for (size_t i = 0; i < n; i++) {
    std::string k = std::to_string(i);
    ids.insert(BKDRHash(k.data(), k.size()));
  }

  size_t c = n - ids.size();
  printf("total = %zu, collisions = %zu (%.2lf)\n", ids.size(), c, (double) c / n);
  NDB_ASSERT(c == 0);

  return EXIT_SUCCESS;
}
