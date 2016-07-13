#ifndef NDB_COMMON_ENCODE_H_
#define NDB_COMMON_ENCODE_H_

#include "ndb/common/common.h"

namespace ndb {

uint32_t BKDRHash(const char* data, size_t size);

}  // namespace ndb

#endif /* NDB_COMMON_ENCODE_H_ */
