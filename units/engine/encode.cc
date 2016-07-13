#include "units/units.h"

void TestEncodeInt64(int64_t int64) {
  std::string encoded;
  EncodeInt64(&encoded, int64);

  Slice dst(encoded);
  int64_t new_int64;
  NDB_ASSERT(DecodeInt64(&dst, &new_int64));
  NDB_ASSERT(new_int64 == int64);
}

void TestEncodePrefix(const std::string& id, uint64_t version, uint8_t type, uint8_t subtype) {
  auto kmeta = EncodeMeta(id);
  auto encoded = EncodePrefix(kmeta, version, type, subtype);

  Slice dst(encoded);
  std::string new_kmeta;
  uint64_t new_version;
  uint8_t new_type, new_subtype;
  NDB_ASSERT(DecodePrefix(&dst, &new_kmeta, &new_version, &new_type, &new_subtype));
  NDB_ASSERT(dst.size() == 0);
  NDB_ASSERT(new_kmeta.size() == kmeta.size());
  NDB_ASSERT(memcmp(new_kmeta.data(), kmeta.data(), kmeta.size()) == 0);
  NDB_ASSERT(new_version == version);
  NDB_ASSERT(new_type == type && new_subtype == subtype);

  dst = Slice(encoded);
  NDB_ASSERT(RemovePrefix(&dst));
  NDB_ASSERT(dst.size() == 0);
}

int Test(int argc, char* argv[]) {
  TestEncodeInt64(0);
  TestEncodeInt64(1);
  TestEncodeInt64(-1);
  TestEncodeInt64(1ULL << 31);
  TestEncodeInt64(-(1ULL << 31));
  TestEncodeInt64(1ULL << 62);
  TestEncodeInt64(-(1ULL << 62));

  TestEncodePrefix("0", 0, 0, 0);
  TestEncodePrefix("hua", 1, 2, 3);
  TestEncodePrefix("chao", 1ULL << 31, 1 << 2, 3);
  TestEncodePrefix("huang", 1ULL << 63, 1 << 3, 4);

  return EXIT_SUCCESS;
}
