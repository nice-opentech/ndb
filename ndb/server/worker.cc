#include "ndb/server/worker.h"

namespace ndb {

class Processor : public Thread {
 public:
  Processor(int epfd,
            ClientCallback cb,
            Channel<Client>* input,
            Channel<Client>* output)
      : epfd_(epfd), cb_(cb), input_(input), output_(output) {
  }

  void Main() override;

 private:
  int epfd_ {-1};
  ClientCallback cb_;
  Channel<Client>* input_ {NULL};
  Channel<Client>* output_ {NULL};
};

void Processor::Main() {
  epoll_event e = {0};
  epoll_wait(epfd_, &e, 1, 1000);
  while (true) {
    auto client = input_->Recv();
    if (client == NULL) {
      break;
    }
    client = cb_(client);
    if (client == NULL) {
      continue;
    }
    output_->Send(client);
  }
}


Result Worker::Run(int num_threads,
                   ClientCallback cb,
                   Channel<Client>* input,
                   Channel<Client>* output) {
  epfd_ = epoll_create(1024);
  if (epfd_ == -1) {
    return Result::Errno("epoll_create()");
  }

  // Use edge-triggered mode here to wakeup just one thread at a time.
  epoll_event e = {0};
  e.data.fd = input->fd();
  e.events = EPOLLIN | EPOLLET;
  if (epoll_ctl(epfd_, EPOLL_CTL_ADD, e.data.fd, &e) == -1) {
    return Result::Errno("epoll_ctl()");
  }

  // All threads share the same epoll and io channel.
  for (int i = 0; i < num_threads; i++) {
    threads_.push_back(new Processor(epfd_, cb, input, output));
    NDB_TRY(threads_.back()->Loop());
  }

  return Result::OK();
}

}  // namespace ndb
