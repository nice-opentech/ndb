#ifndef NDB_ENGINE_ENCODE_H_
#define NDB_ENGINE_ENCODE_H_

#include "ndb/engine/common.h"

namespace ndb {

Slice EncodeMeta(const std::string& id);
bool DecodeMeta(Slice* dst, std::string* kmeta);

void EncodeInt64(std::string* dst, int64_t i);
bool DecodeInt64(Slice* dst, int64_t* i);

void EncodeUint64(std::string* dst, uint64_t u);
bool DecodeUint64(Slice* dst, uint64_t* u);

std::string EncodePrefix(const Slice& kmeta, uint64_t version, uint8_t type, uint8_t subtype);
bool DecodePrefix(Slice* dst, std::string* kmeta, uint64_t* version, uint8_t* type, uint8_t* subtype);
bool RemovePrefix(Slice* dst);

}  // namespace ndb

#endif /* NDB_ENGINE_ENCODE_H_ */
