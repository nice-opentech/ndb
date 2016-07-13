#include "units/units.h"

HashLock hashlock;

void Count1(const Slice& s, int* c, int n) {
  for (int i = 0; i < n; i++) {
    AutoLock lock(hashlock.GetLock(s));
    *c += 1;
  }
}

void Count2(const std::vector<Slice>& ss, int* c, int n) {
  for (int i = 0; i < n; i++) {
    AutoLock lock(hashlock.GetLock(ss));
    *c += 1;
  }
}

int Test(int argc, char* argv[]) {
  {
    int count = 0;
    std::thread c1(Count1, "count:1", &count, 250000);
    std::thread c2(Count1, "count:1", &count, 250000);
    std::thread c3(Count1, "count:1", &count, 250000);
    std::thread c4(Count1, "count:1", &count, 250000);
    c1.join(), c2.join(), c3.join(), c4.join();
    printf("count = %d\n", count);
    NDB_ASSERT(count == 1000000);
  }
  {
    int count = 0;
    std::vector<Slice> ss {"count:2", "count:3"};
    std::thread c1(Count2, ss, &count, 250000);
    std::thread c2(Count2, ss, &count, 250000);
    std::thread c3(Count2, ss, &count, 250000);
    std::thread c4(Count2, ss, &count, 250000);
    c1.join(), c2.join(), c3.join(), c4.join();
    printf("count = %d\n", count);
    NDB_ASSERT(count == 1000000);
  }

  return EXIT_SUCCESS;
}
