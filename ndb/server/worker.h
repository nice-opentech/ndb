#ifndef NDB_SERVER_WORKER_H_
#define NDB_SERVER_WORKER_H_

#include "ndb/common/channel.h"
#include "ndb/common/thread.h"
#include "ndb/server/client.h"

namespace ndb {

// Return NULL if callee take ownership of client.
typedef std::function<Client* (Client*)> ClientCallback;

class Worker {
 public:
  Result Run(int num_threads,
             ClientCallback cb,
             Channel<Client>* input,
             Channel<Client>* output);

  ~Worker()   { for (auto thread : threads_) delete thread; }

  void Stop() { for (auto thread : threads_) thread->Stop(); }

  void Join() { for (auto thread : threads_) thread->Join(); }

 private:
  int epfd_ {-1};
  std::vector<Thread*> threads_;
};

}  // namespace ndb

#endif /* NDB_SERVER_WORKER_H_ */
