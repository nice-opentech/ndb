#include "ndb/ndb.h"

namespace ndb {

class Synchro::Replica {
 public:
  Replica(Client* client, WALIterator* it) : client_(client), it_(it) {}

  ~Replica() { delete client_; delete it_; }

  const char* name() const { return client_->name(); }

  bool HasRequest() const { return client_->HasRequest(); }

  bool HasResponse() const { return client_->HasResponse(); }

  Result HandleEvent(IOLoop::Event event);

  Response CommandPSYNC(const Request& request);

 private:
  Client* client_ {NULL};
  WALIterator* it_ {NULL};
};

Result Synchro::Replica::HandleEvent(IOLoop::Event event) {
  NDB_TRY(client_->HandleEvent(event));
  while (client_->HasRequest()) {
    const auto& request = client_->GetRequest();
    if (request.argc() != 3 || request.args(0) != "PSYNC") {
      return Result::Error("Invalid command: %s", request.join().c_str());
    }
    NDB_LOG_DEBUG("*SYNCHRO* client %s: %s", client_->name(), request.join().c_str());
    client_->PutResponse(CommandPSYNC(request));
    client_->PopRequest();
  }
  return Result::OK();
}

// PSYNC sequence limit
Response Synchro::Replica::CommandPSYNC(const Request& request) {
  uint64_t sequence = 0;
  if (!ParseUint64(request.args(1), &sequence)) {
    return Response::InvalidArgument();
  }
  uint64_t limit = 0;
  if (!ParseUint64(request.args(2), &limit)) {
    return Response::InvalidArgument();
  }

  std::vector<std::string> updates {"UPDATES"};
  for (it_->Seek(sequence); it_->Valid(); it_->Next()) {
    auto batch = it_->batch();
    if (batch.sequence != sequence) {
      return Result::Error("sequence unmatch: local=%llu request=%llu",
                           (unsigned long long) batch.sequence,
                           (unsigned long long) sequence);
    }

    updates.push_back(batch.writeBatchPtr->Data());
    sequence += batch.writeBatchPtr->Count();
    if (limit > 0 && updates.size() > limit) {
      break;
    }
  }

  auto r = it_->result();
  if (updates.size() > 1 || r.ok() || r.IsNotFound()) {
    return Response::Bulks(updates);
  }
  return r;
}


Synchro::~Synchro() {
  for (auto replica : replicas_) { delete replica.second; }
}

Result Synchro::Run() {
  return Loop();
}

void Synchro::AddClient(Client* client) {
  NDB_LOG_INFO("*SYNCHRO* add client %s", client->name());
  std::unique_lock<std::mutex> lock(lock_);
  auto fd = client->fd();
  auto r = ioloop_.Add(fd, IOLoop::kReadable);
  if (!r.ok()) {
    delete client;
    return;
  }
  replicas_[fd] = new Replica(client, engine_->NewWALIterator().release());
  HandleEvent(fd, IOLoop::kReadable);
}

void Synchro::CloseClient(int fd, const char* reason) {
  auto replica = replicas_[fd];
  NDB_LOG_ERROR("*SYNCHRO* close client %s: %s", replica->name(), reason);
  replicas_.erase(fd);
  ioloop_.Del(fd);
  delete replica;
}

void Synchro::HandleEvent(int fd, IOLoop::Event event) {
  auto replica = replicas_[fd];
  auto r = replica->HandleEvent(event);
  if (r.ok()) {
    if (replica->HasResponse()) {
      r = ioloop_.Mod(fd, IOLoop::kWritable);
    } else {
      r = ioloop_.Mod(fd, IOLoop::kReadable);
    }
  }
  if (!r.ok()) {
    CloseClient(fd, r.message());
    return;
  }
}

}  // namespace ndb
