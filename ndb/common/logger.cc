#include "ndb/common/logger.h"

namespace ndb {

static const char* level_messages[] = {
  "DEBUG", "INFO", "WARN", "ERROR", "FATAL", NULL
};

const char* Logger::FormatLevel(Level level) {
  return level_messages[level];
}

bool Logger::ParseLevel(const std::string& s, Level* level) {
  for (int i = 0; level_messages[i] != NULL; i++) {
    if (strcasecmp(s.c_str(), level_messages[i]) == 0) {
      *level = Logger::Level(i);
      return true;
    }
  }
  return false;
}

Logger::Logger(const Options& options) : options_(options) {
}

Logger::~Logger() {
  if (fp_ != NULL) {
    fclose(fp_);
  }
}

Result Logger::Open() {
  fp_ = fopen(options_.filename.c_str(), "a");
  if (fp_ == NULL) {
    return Result::Errno("fopen(%s)", options_.filename.c_str());
  }
  return Result::OK();
}

void Logger::Printf(Level level, const char* file, int line, const char* fmt, ...) {
  if (level < options_.level) return;

  time_t t = time(NULL);
  struct tm tm;
  localtime_r(&t, &tm);

  char message[1024];
  va_list ap;
  va_start(ap, fmt);
  vsnprintf(message, sizeof(message), fmt, ap);
  va_end(ap);

  fprintf(fp_, "%04d-%02d-%02d %02d:%02d:%02d [%s:%d] [%1.1s] %s\n",
          tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
          tm.tm_hour, tm.tm_min, tm.tm_sec, file, line,
          level_messages[level], message);
  fflush(fp_);
}

void Logger::Printf(Level level, const char* fmt, ...) {
  va_list ap;
  char message[1024];
  va_start(ap, fmt);
  vsnprintf(message, sizeof(message), fmt, ap);
  Printf(level, __FILE__, __LINE__, message);
  va_end(ap);
}

}  // namespace ndb
