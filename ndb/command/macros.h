#ifndef NDB_COMMAND_MACROS_H_
#define NDB_COMMAND_MACROS_H_

namespace ndb {

// Lock one or more keys.
#define NDB_LOCK_KEY(k) AutoLock _lock(ndb->hashlock.GetLock(k))

// Get namespace.
// Response if namespace does not exist.
#define NDB_TRY_GETNS(nsname) ({                                    \
      auto ns = ndb->engine->GetNamespace(nsname);                  \
      if (ns == NULL) {                                             \
        return Response::InvalidNamespace();                        \
      }                                                             \
      nsstats.add(nsname, request.args(0));                         \
      ns;                                                           \
    })

// Parse key and get namespace.
// Response if namespace does not exist.
#define NDB_TRY_GETNS_BYKEY(k, ns, id)                              \
  std::string nsname, id;                                           \
  ParseNamespace(k, &nsname, &id);                                  \
  auto ns = NDB_TRY_GETNS(nsname);                                  \
  auto configs = ns->GetConfigs()

// Get collection meta value.
// Response if get meta failed or type mismatch.
#define NDB_TRY_GETMETA(ns, kmeta, vtype) ({                        \
      Value value;                                                  \
      auto r = ns->Get((kmeta), &value);                            \
      if (r.ok()) {                                                 \
        if (!value.has_meta() || value.meta().type() != (vtype)) {  \
          return Response::WrongType();                             \
        }                                                           \
      } else if (!r.IsNotFound()) {                                 \
        return r;                                                   \
      }                                                             \
      value.mutable_meta()->set_type(vtype);                        \
      value;                                                        \
    })

// Parse key and get namespace and collection meta value.
// Response if namespace not exist, get meta failed or type mismatch.
#define NDB_TRY_GETMETA_TYPE_BYKEY(k, ns, kmeta, vmeta, vtype)      \
  NDB_TRY_GETNS_BYKEY(k, ns, id);                                   \
  auto kmeta = EncodeMeta(id);                                      \
  auto vmeta = NDB_TRY_GETMETA(ns, kmeta, vtype);                   \
  auto length = vmeta.meta().length();                              \
  auto version = vmeta.meta().version()

// Log and return command error.
#define NDB_COMMAND_ERROR(fmt, ...) ({                              \
      auto r = Result::Error("cmd=%s ns=%s id=%s " fmt,             \
                             request.args(0).c_str(),               \
                             nsname.c_str(),                        \
                             id.c_str(),                            \
                             ## __VA_ARGS__);                       \
      NDB_LOG_ERROR("*COMMAND* %s", r.message());                   \
      r;                                                            \
    })

// Update meta length.
// Response if length overflow or underflow.
#define NDB_COMMAND_UPDATE_LENGTH(vmeta, increment) do {            \
    int64_t length = vmeta.meta().length();                         \
    if (!CheckIncrementBound(length, increment)) {                  \
      return NDB_COMMAND_ERROR("length %ll increment %ll",          \
                               (long long) length,                  \
                               (long long) increment);              \
    }                                                               \
    length += increment;                                            \
    vmeta.SetLength(length);                                        \
  } while (0)

}  // namespace ndb

#endif /* NDB_COMMAND_MACROS_H_ */
