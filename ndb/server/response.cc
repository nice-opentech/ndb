#include "ndb/server/response.h"

namespace ndb {

const char* kResponseOK   = "+OK\r\n";
const char* kResponsePONG = "+PONG\r\n";
const char* kResponseNULL = "$-1\r\n";

Response::Response(const Result& r) {
  if (r.ok()) {
    s_ = kResponseOK;
  } else if (r.IsNotFound()) {
    s_ = kResponseNULL;
  } else {
    s_ = "-ERR " + std::string(r.message()) + "\r\n";
  }
}

// Simple Strings
Response Response::OK() {
  return Response(kResponseOK);
}
Response Response::PONG() {
  return Response(kResponsePONG);
}
Response Response::Simple(const std::string& s) {
  return Response("+" + s + "\r\n");
}

// Errors
Response Response::WrongType() {
  return Response("-ERR Wrong type.\r\n");
}
Response Response::OutofRange() {
  return Response("-ERR Out of range.\r\n");
}
Response Response::InvalidOption() {
  return Response("-ERR Invalid option.\r\n");
}
Response Response::InvalidCommand() {
  return Response("-ERR Invalid command.\r\n");
}
Response Response::InvalidArgument() {
  return Response("-ERR Invalid argument.\r\n");
}
Response Response::InvalidNamespace() {
  return Response("-ERR Invalid namespace.\r\n");
}
Response Response::PermissionDenied() {
  return Response("-ERR Permission denied.\r\n");
}

// Integers
Response Response::Int(int64_t i) {
  Response r;
  r.AppendInt(i);
  return r;
}

// Bulk Strings
Response Response::Null() {
  Response r;
  r.AppendNull();
  return r;
}
Response Response::Bulk(int64_t i) {
  Response r;
  r.AppendBulk(i);
  return r;
}
Response Response::Bulk(const std::string& bulk) {
  Response r;
  r.AppendBulk(bulk);
  return r;
}
Response Response::Bulk(const char* data, size_t size) {
  Response r;
  r.AppendBulk(data, size);
  return r;
}

// Bulk Arrays
Response Response::Size(size_t size) {
  Response r;
  r.AppendSize(size);
  return r;
}
Response Response::Bulks(const std::vector<std::string>& bulks) {
  Response r;
  r.AppendBulks(bulks);
  return r;
}

bool Response::IsOK() const {
  return s_ == kResponseOK;
}
bool Response::IsNull() const {
  return s_ == kResponseNULL;
}

void Response::Append(const Response& res) {
  s_.append(res.data(), res.size());
}

// Integers
void Response::AppendInt(int64_t i) {
  s_.append(":");
  s_.append(std::to_string(i));
  s_.append("\r\n");
}

// Bulk Strings
void Response::AppendNull() {
  s_.append(kResponseNULL);
}
void Response::AppendBulk(int64_t i) {
  AppendBulk(std::to_string(i));
}
void Response::AppendBulk(const std::string& bulk) {
  AppendBulk(bulk.data(), bulk.size());
}
void Response::AppendBulk(const char* data, size_t size) {
  s_.append("$");
  s_.append(std::to_string(size));
  s_.append("\r\n");
  s_.append(data, size);
  s_.append("\r\n");
}

// Bulk Arrays
void Response::AppendSize(size_t size) {
  s_.append("*");
  s_.append(std::to_string(size));
  s_.append("\r\n");
}
void Response::AppendBulks(const std::vector<std::string>& bulks) {
  AppendSize(bulks.size());
  for (const auto& bulk : bulks) {
    AppendBulk(bulk);
  }
}

}  // namespace ndb
