#ifndef NDB_SERVER_RESPONSE_H_
#define NDB_SERVER_RESPONSE_H_

#include "ndb/common/result.h"

namespace ndb {

class Response {
 public:
  // Simple Strings
  static Response OK();
  static Response PONG();
  static Response Simple(const std::string& s);

  // Errors
  static Response WrongType();
  static Response OutofRange();
  static Response InvalidOption();
  static Response InvalidCommand();
  static Response InvalidArgument();
  static Response InvalidNamespace();
  static Response PermissionDenied();

  // Integers
  static Response Int(int64_t i);

  // Bulk Strings
  static Response Null();
  static Response Bulk(int64_t i);
  static Response Bulk(const std::string& bulk);
  static Response Bulk(const char* data, size_t size);

  // Bulk Arrays
  static Response Size(size_t size);
  static Response Bulks(const std::vector<std::string>& bulks);

  Response() {}

  Response(const Result& r);

  explicit Response(const std::string& s) : s_(s) {}

  const char* data() const { return s_.data(); }

  size_t size() const { return s_.size(); }

  bool IsOK() const;
  bool IsNull() const;

  // Nested responses.
  void Append(const Response& res);

  // Integers
  void AppendInt(int64_t i);

  // Bulk Strings
  void AppendNull();
  void AppendBulk(int64_t i);
  void AppendBulk(const std::string& bulk);
  void AppendBulk(const char* data, size_t size);

  // Bulk Arrays
  void AppendSize(size_t size);
  void AppendBulks(const std::vector<std::string>& bulks);

 private:
  std::string s_;
};

}  // namespace ndb

#endif /* NDB_SERVER_RESPONSE_H_ */
