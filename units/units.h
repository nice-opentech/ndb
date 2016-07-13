#include "ndb/ndb.h"

using namespace ndb;

#define RED   "\x1B[31m"
#define GREEN "\x1B[32m"
#define RESET "\033[0m"

int Test(int argc, char* argv[]);

int main(int argc, char* argv[]) {
  auto code = Test(argc, argv);
  printf("----------------------------------------\n");
  if (code != EXIT_SUCCESS) {
    printf(RED "TEST FAIL\n" RESET);
  } else {
    printf(GREEN "TEST PASS\n" RESET);
  }

  // Cleanup libraries.
  google::protobuf::ShutdownProtobufLibrary();
  return code;
}
