#ifndef NDB_SERVER_SERVER_H_
#define NDB_SERVER_SERVER_H_

#include "ndb/common/stats.h"
#include "ndb/common/eventd.h"
#include "ndb/server/worker.h"

namespace ndb {

class Server : public Eventd {
 public:
  struct Options {
    std::string address {"0.0.0.0:9736"};
    int num_workers {32};
    int max_clients {8192};
    int timeout_secs {60};
    size_t buffer_size {16 << 20};
    size_t channel_size {4096};
  };

  Server(const Options& options);

  ~Server();

  Result Run(ClientCallback cb);

  Stats GetStats() const;

 private:
  void HandleCron() override;
  void HandleEvent(int fd, IOLoop::Event event) override;
  void HandleSocket();
  void HandleOutput();
  void HandleClient(int fd, IOLoop::Event event);

  // Callee take ownership of client.
  void AddClient(Client* client);
  // Caller take ownership of client.
  Client* TakeClient(int fd);
  void CloseClient(int fd, const char* reason);

 private:
  Options options_;
  time_t uptime_;
  Socket socket_;
  Worker worker_;
  Channel<Client>* input_ {NULL};
  Channel<Client>* output_ {NULL};
  std::map<int, Client*> clients_;
};

}  // namespace ndb

#endif /* NDB_SERVER_SERVER_H_ */
