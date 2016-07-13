#include "ndb/ndb.h"

namespace ndb {

namespace set {

#define NDB_TRY_GETMETA_BYKEY(k, ns, kmeta, vmeta)                  \
  NDB_TRY_GETMETA_TYPE_BYKEY(k, ns, kmeta, vmeta, Meta::SET)

std::string EncodePrefix(const Slice& kmeta, uint64_t version) {
  return ndb::EncodePrefix(kmeta, version, Meta::SET, 1);
}

std::string EncodeMember(const Slice& kmeta, uint64_t version, const Slice& field) {
  std::string dst = EncodePrefix(kmeta, version);
  dst.append(field.data(), field.size());
  return dst;
}

}  // namespace set

// SADD key field [field ...]
Response CommandSADD(const Request& request) {
  // Remove duplication.
  std::set<std::string> fields(request.args().begin() + 2, request.args().end());

  NDB_LOCK_KEY(request.args(1));
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);

  std::vector<std::string> members;
  for (const auto& field : fields) {
    members.push_back(set::EncodeMember(kmeta, version, field));
  }

  NSBatch batch(ns);
  uint64_t count = 0;
  auto results = ns->MultiGet(members, NULL);
  for (size_t i = 0; i < results.size(); i++) {
    const auto& m = members[i];
    const auto& r = results[i];
    if (r.IsNotFound()) {
      batch.Put(m);
      count++;
    } else if (!r.ok()) {
      return r;
    }
  }

  NDB_COMMAND_UPDATE_LENGTH(vmeta, +count);
  vmeta.SetConfigs(configs);
  batch.Put(kmeta, vmeta);
  NDB_TRY(batch.Commit());
  return Response::Int(count);
}

// SREM key field [field ...]
Response CommandSREM(const Request& request) {
  // Remove duplication.
  std::set<std::string> fields(request.args().begin() + 2, request.args().end());

  NDB_LOCK_KEY(request.args(1));
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);
  if (length == 0) return Response::Int(0);

  std::vector<std::string> members;
  for (const auto& field : fields) {
    members.push_back(set::EncodeMember(kmeta, version, field));
  }

  NSBatch batch(ns);
  uint64_t count = 0;
  auto results = ns->MultiGet(members, NULL);
  for (size_t i = 0; i < results.size(); i++) {
    const auto& m = members[i];
    const auto& r = results[i];
    if (r.ok()) {
      batch.Delete(m);
      count++;
    } else if (!r.IsNotFound()) {
      return r;
    }
  }

  NDB_COMMAND_UPDATE_LENGTH(vmeta, -count);
  vmeta.SetConfigs(configs);
  batch.Put(kmeta, vmeta);
  NDB_TRY(batch.Commit());
  return Response::Int(count);
}

// SCARD key
Response CommandSCARD(const Request& request) {
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);
  return Response::Int(length);
}

// SMEMBERS key
Response CommandSMEMBERS(const Request& request) {
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);
  if (length == 0) return Response::Size(0);

  auto begin = set::EncodePrefix(kmeta, version);
  auto end = FindNextSuccessor(begin);
  auto res = Response::Size(length);

  auto it = ns->RangeGet(begin, end, 0, length);
  for (it->Seek(); it->Valid(); it->Next(), length--) {
    auto field = it->id();
    if (!RemovePrefix(&field)) {
      return NDB_COMMAND_ERROR("corruption");
    }
    res.AppendBulk(field.data(), field.size());
  }
  NDB_TRY(it->result());
  if (length != 0) {
    NDB_LOG_ERROR("*COMMAND* SMEMBERS namespace %s id %d length %lld",
                  ns->GetName().c_str(), kmeta.data(), (long long) length);
    return Result::Error("corruption");
  }
  return res;
}

// SISMEMBER key field
Response CommandSISMEMBER(const Request& request) {
  NDB_TRY_GETMETA_BYKEY(request.args(1), ns, kmeta, vmeta);
  if (length == 0) return Response::Int(0);

  auto member = set::EncodeMember(kmeta, version, request.args(2));
  auto r = ns->Get(member, NULL);
  if (r.ok()) {
    return Response::Int(1);
  }
  if (r.IsNotFound()) {
    return Response::Int(0);
  }
  return r;
}

}  // namespace ndb
