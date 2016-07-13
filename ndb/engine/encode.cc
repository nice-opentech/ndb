#include "ndb/engine/encode.h"

namespace ndb {

Slice EncodeMeta(const std::string& id) {
  // C++ 11 make sure that id[size] == '\0'.
  return Slice(id.data(), id.size() + 1);
}

bool DecodeMeta(Slice* dst, std::string* kmeta) {
  for (size_t i = 0; i < dst->size(); i++) {
    if ((*dst)[i] == '\0') {
      kmeta->assign(dst->data(), i+1);
      dst->remove_prefix(i+1);
      return true;
    }
  }
  return false;
}

void EncodeInt64(std::string* dst, int64_t i) {
  uint64_t u = i ^ (1ULL << 63);
  EncodeUint64(dst, u);
}

bool DecodeInt64(Slice* dst, int64_t* i) {
  uint64_t u = 0;
  if (!DecodeUint64(dst, &u)) {
    return false;
  }
  *i = u ^ (1ULL << 63);
  return true;
}

void EncodeUint64(std::string* dst, uint64_t u) {
  u = htobe64(u);
  dst->append((char*) &u, sizeof(u));
}

bool DecodeUint64(Slice* dst, uint64_t* u) {
  if (dst->size() < sizeof(*u)) return false;
  *u = be64toh(*(uint64_t*) dst->data());
  dst->remove_prefix(sizeof(*u));
  return true;
}

inline char* PutVarint64(char* dst, uint64_t v) {
  const unsigned int B = 128;
  auto pos = (unsigned char*) dst;
  while (v >= B) {
    *(pos++) = (v & (B - 1)) | B;
    v >>= 7;
  }
  *(pos++) = v;
  return (char*) pos;
}

void EncodeVarint64(std::string* dst, uint64_t v) {
  char buf[10];
  auto pos = PutVarint64(buf, v);
  dst->append(buf, pos - buf);
}

const char* GetVarint64(const char* pos, const char* end, uint64_t* value) {
  uint64_t result = 0;
  for (uint32_t shift = 0; shift <= 63 && pos < end; shift += 7) {
    uint64_t byte = *(const unsigned char*) (pos++);
    if (byte & 128) {
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return pos;
    }
  }
  return NULL;
}

bool DecodeVarint64(Slice* dst, uint64_t* v) {
  auto begin = dst->data();
  auto end = dst->data() + dst->size();
  auto pos = GetVarint64(begin, end, v);
  if (pos == NULL) return false;
  dst->remove_prefix(pos - begin);
  return true;
}

std::string EncodePrefix(const Slice& kmeta, uint64_t version, uint8_t type, uint8_t subtype) {
  std::string dst;
  // Encode kmeta
  dst.append(kmeta.data(), kmeta.size());
  // Encode version
  EncodeVarint64(&dst, version);
  // Encode type and subtype
  NDB_ASSERT(type < (1 << 5) && subtype < (1 << 3));
  dst.push_back(type << 3 | subtype);
  return dst;
}

bool DecodePrefix(Slice* dst, std::string* kmeta, uint64_t* version, uint8_t* type, uint8_t* subtype) {
  // Decode kmeta
  if (!DecodeMeta(dst, kmeta)) return false;
  // Decode version
  if (!DecodeVarint64(dst, version)) return false;
  // Decode type and subtype
  if (dst->size() == 0) return false;
  *type = (*dst)[0] >> 3;
  *subtype = (*dst)[0] & 0x07;
  dst->remove_prefix(1);
  return true;
}

bool RemovePrefix(Slice* dst) {
  std::string kmeta;
  uint64_t version;
  uint8_t type, subtype;
  return DecodePrefix(dst, &kmeta, &version, &type, &subtype);
}

}  // namespace ndb
