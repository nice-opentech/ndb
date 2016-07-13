#include "ndb/server/request.h"

namespace ndb {

static ssize_t FindCRLF(const char* input, size_t size) {
  for (size_t i = 0; i < size; i++) {
    if (input[i] != '\n') {
      continue;
    }
    if (i == 0 || input[i-1] != '\r') {
      return -1;
    }
    return i-1;
  }
  return 0;
}

static ssize_t ParseSize(const char* input, size_t size) {
  ssize_t value = 0;
  for (size_t i = 0; i < size; i++) {
    if (input[i] > '9' || input[i] < '0') {
      return -1;
    }
    value = value * 10 + (input[i] - '0');
    if (value > 16 * 1024 * 1024) {
      return -2;
    }
  }
  return value;
}

void RequestBuilder::Reset() {
  type_ = kUnknown;
  argc_ = -1;
  argz_ = -1;
  args_.clear();
  finished_ = false;
}

ssize_t RequestBuilder::Parse(const char* input, size_t size) {
  if (size == 0) return 0;
  if (type_ == kUnknown) {
    if (input[0] != '*') {
      type_ = kInline;
    } else {
      type_ = kMultiBulk;
    }
  }
  if (type_ == kInline) {
    return ParseInline(input, size);
  }
  return ParseMultiBulk(input, size);
}

ssize_t RequestBuilder::ParseInline(const char* input, size_t size) {
  const char* pos = input;

  ssize_t offset = FindCRLF(pos, size);
  if (offset <= 0) {
    return offset;
  }
  const char* newline = pos + offset;

  while (pos < newline) {
    if (isspace(*pos)) {
      pos++;
      continue;
    }

    const char* space = NULL;
    for (space = pos + 1; space < newline; space++) {
      if (isspace(*space)) {
        break;
      }
    }

    args_.push_back(std::string(pos, space - pos));
    pos = space + 1;
  }

  finished_ = true;
  return newline - input + 2;
}

ssize_t RequestBuilder::ParseMultiBulk(const char* input, size_t size) {
  const char* pos = input;
  const char* newline = NULL;

  if (argc_ == -1) {
    ssize_t offset = FindCRLF(pos, input + size - pos);
    if (offset < 0) {
      return -1;
    }
    if (offset == 0) {
      return pos - input;
    }
    newline = pos + offset;

    if (*pos != '*') {
      return -2;
    }
    pos++;

    argc_ = ParseSize(pos, newline - pos);
    if (argc_ < 0) {
      return -3;
    }

    pos = newline + 2;
  }

  while ((ssize_t) args_.size() != argc_) {
    if (pos - input >= (ssize_t) size) {
      return pos - input;
    }

    if (argz_ == -1) {
      ssize_t offset = FindCRLF(pos, input + size - pos);
      if (offset < 0) {
        return -1;
      }
      if (offset == 0) {
        return pos - input;
      }
      newline = pos + offset;

      if (*pos != '$') {
        return -5;
      }
      pos++;

      argz_ = ParseSize(pos, newline - pos);
      if (argz_ < 0) {
        return -6;
      }

      pos = newline + 2;
    }

    if (pos - input + argz_ + 2 > (ssize_t) size) {
      return pos - input;
    }

    newline = pos + argz_;
    if (*newline != '\r' || *(newline + 1) != '\n') {
      return -7;
    }

    args_.push_back(std::string(pos, argz_));
    argz_ = -1;

    pos = newline + 2;
  }

  finished_ = true;
  return pos - input;
}

Request RequestBuilder::Finish() {
  return Request(std::move(args_));
}

}  // namespace ndb
