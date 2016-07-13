#include "ndb/ndb.h"

namespace ndb {

Monitor::~Monitor() {
  for (auto client : clients_) { delete client; }
}

Result Monitor::Run() {
  NDB_TRY(ioloop_.Add(channel_.fd(), IOLoop::kReadable));
  return Loop();
}

void Monitor::AddClient(Client* client) {
  NDB_LOG_INFO("*MONITOR* add client %s", client->name());
  // Response OK.
  client->PutResponse(Response::OK());
  auto r = client->HandleEvent(IOLoop::kWritable);
  if (!r.ok()) {
    NDB_LOG_ERROR("*MONITOR* handle client %s", client->name(), r.message());
    delete client;
    return;
  }
  std::unique_lock<std::mutex> lock(lock_);
  clients_.insert(client);
}

void Monitor::PutRequest(const Client* client) {
  if (clients_.size() == 0) return;
  std::string s = "+";
  // Append timestamp.
  auto timestamp = getustime();
  s.append(std::to_string(timestamp/1000000));
  s.append(".");
  s.append(std::to_string(timestamp%1000000));
  s.append(" ");
  // Append client.
  s.append("[0 " + std::string(client->name()) + "]");
  // Append request.
  const auto& request = client->GetRequest();
  for (size_t i = 0; i < request.argc(); i++) {
    s.append(" \"");
    s.append(request.args(i));
    s.append("\"");
  }
  s.append("\r\n");
  channel_.Send(new Response(s));
}

void Monitor::HandleEvent(int fd, IOLoop::Event event) {
  std::unique_lock<std::mutex> lock(lock_);
  while (true) {
    std::unique_ptr<Response> response(channel_.Recv());
    if (response == NULL) {
      return;
    }

    std::vector<Client*> errors;
    for (auto client : clients_) {
      Response clone = *response;
      client->PutResponse(std::move(clone));
      auto r = client->HandleEvent(IOLoop::kWritable);
      if (!r.ok()) {
        errors.push_back(client);
      }
    }

    for (auto client : errors) {
      NDB_LOG_INFO("*MONITOR* close client %s", client->name());
      clients_.erase(client);
      delete client;
    }
  }
}

}  // namespace ndb
