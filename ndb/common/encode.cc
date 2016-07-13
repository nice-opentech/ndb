#include "ndb/common/encode.h"

namespace ndb {

uint32_t BKDRHash(const char* data, size_t size) {
  uint32_t seed = 131;
  uint32_t hash = 0;
  for (size_t i = 0; i < size; i++) {
    hash = hash * seed + data[i];
  }
  return hash;
}

}  // namespace ndb
