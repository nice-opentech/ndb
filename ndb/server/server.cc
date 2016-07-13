#include "ndb/ndb.h"

namespace ndb {

Server::Server(const Options& options)
    : options_(options), uptime_(time(NULL)) {
}

Server::~Server() {
  // Call Stop() before Join() to speedup.
  Stop();
  worker_.Stop();
  Join();
  worker_.Join();
  delete input_;
  delete output_;
  for (auto client : clients_) { delete client.second; }
}

Result Server::Run(ClientCallback cb) {
  // Listen socket.
  NDB_TRY(socket_.Listen(options_.address));
  NDB_TRY(ioloop_.Add(socket_.fd(), IOLoop::kReadable));

  // Listen output.
  input_ = new Channel<Client>(options_.channel_size);
  output_ = new Channel<Client>(options_.channel_size);
  NDB_TRY(ioloop_.Add(output_->fd(), IOLoop::kReadable));

  worker_.Run(options_.num_workers, cb, input_, output_);
  return Loop();
}

void Server::HandleCron() {
  std::vector<int> timeouts;
  for (auto client : clients_) {
    if (client.second->HasTimeout()) {
      timeouts.push_back(client.second->fd());
    }
  }
  for (auto fd : timeouts) {
    CloseClient(fd, "timeout");
  }
}

void Server::HandleEvent(int fd, IOLoop::Event event) {
  if (fd == socket_.fd()) {
    HandleSocket();
  } else if (fd == output_->fd()) {
    HandleOutput();
  } else {
    HandleClient(fd, event);
  }
}

void Server::HandleSocket() {
  int connfd = -1;
  auto r = socket_.Accept(&connfd);
  if (!r.ok()) {
    NDB_LOG_ERROR("*SERVER* accept: %s", r.message());
    return;
  }

  auto client = new Client(connfd);
  NDB_LOG_INFO("*SERVER* accept client %s", client->name());

  if (clients_.size() >= (size_t) options_.max_clients) {
    NDB_LOG_ERROR("*SERVER* client limit exceed %d", options_.max_clients);
    delete client;
    return;
  }

  AddClient(client);
}

void Server::HandleOutput() {
  while (true) {
    auto client = output_->Recv();
    if (client == NULL) {
      break;
    }
    AddClient(client);
  }
}

void Server::HandleClient(int fd, IOLoop::Event event) {
  auto client = clients_[fd];
  auto r = client->HandleEvent(event);
  if (r.ok()) {
    if (client->HasRequest()) {
      input_->Send(TakeClient(fd));
      return;
    }
    if (client->HasResponse()) {
      r = ioloop_.Mod(fd, IOLoop::kWritable);
    } else {
      r = ioloop_.Mod(fd, IOLoop::kReadable);
    }
  }
  if (!r.ok()) {
    CloseClient(fd, r.message());
  }
}

void Server::AddClient(Client* client) {
  int fd = client->fd();
  client->set_bufsize(options_.buffer_size);
  client->set_timeout(seconds(options_.timeout_secs));

  Result r;
  if (client->HasResponse()) {
    r = ioloop_.Add(fd, IOLoop::kWritable);
  } else {
    r = ioloop_.Add(fd, IOLoop::kReadable);
  }
  if (!r.ok()) {
    NDB_LOG_ERROR("*SERVER* add client %s: %s", client->name(), r.message());
    delete client;
    return;
  }

  clients_[fd] = client;
}

Client* Server::TakeClient(int fd) {
  auto client = clients_[fd];
  clients_.erase(fd);
  ioloop_.Del(fd);
  return client;
}

void Server::CloseClient(int fd, const char* reason) {
  auto client = TakeClient(fd);
  NDB_LOG_INFO("*SERVER* close client %s: %s", client->name(), reason);
  delete client;
}

Stats Server::GetStats() const {
  Stats stats;
  stats.insert("pid", getpid());
  stats.insert("uptime", time(NULL) - uptime_);
  stats.insert("address", options_.address);
  stats.insert("requests", input_->size());
  stats.insert("responses", output_->size());
  stats.insert("connections", clients_.size());
  return stats;
}

}  // naemspace ndb
