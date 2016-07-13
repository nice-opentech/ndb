#include "units/units.h"

int Test(int argc, char* argv[]) {
  Logger::Options options;
  options.level = Logger::kDebug;
  options.filename = "logger.log";

  Logger logger(options);
  NDB_ASSERT_OK(logger.Open());
  logger.Printf(Logger::kDebug, "hello");
  logger.Printf(Logger::kInfo,  "hello");
  logger.Printf(Logger::kWarn,  "hello");

  options.filename = ".";
  Logger failed(options);
  auto r = failed.Open();
  NDB_ASSERT(!r.ok());
  logger.Printf(Logger::kError, "error: %s", r.message());
  logger.Printf(Logger::kFatal, "fatal: %s", r.message());

  system("cat logger.log");
  system("rm  logger.log");

  return EXIT_SUCCESS;
}
