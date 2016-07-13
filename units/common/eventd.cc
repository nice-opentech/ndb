#include "units/units.h"

class Echo : public Eventd {
 public:
  Result Run(const std::string& address) {
    NDB_TRY(socket_.Listen(address));
    NDB_TRY(ioloop_.Add(socket_.fd(), IOLoop::kReadable));
    return Loop();
  }

 private:
  void HandleCron() override {
    if (!client_.IsOpen()) return;
    if (timeout_++ > 10) {
      timeout_ = 0;
      CloseClient("timeout");
    }
  }

  void HandleEvent(int fd, IOLoop::Event event) override {
    if (fd == socket_.fd()) {
      HandleSocket();
    } else {
      HandleClient();
    }
  }

  void HandleSocket() {
    int connfd = -1;
    NDB_ASSERT_OK(socket_.Accept(&connfd));
    NDB_ASSERT_OK(ioloop_.Add(connfd, IOLoop::kReadable));
    client_.Open(connfd);
  }

  void HandleClient() {
    char message[1024] = {0};
    auto n = read(client_.fd(), message, sizeof(message));
    if (n < 0) {
      CloseClient(strerror(errno));
      return;
    }
    if (n == 0) {
      CloseClient("closed");
      return;
    }
    NDB_ASSERT(write(client_.fd(), message, n) == n);
  }

  void CloseClient(const char* reason) {
    printf("Client %s.\n", reason);
    ioloop_.Del(client_.fd());
    client_.Close();
  }

 private:
  Socket socket_;
  Socket client_;
  int timeout_ {0};
};

void ClientMain(const std::string& address, int i) {
  Socket client;
  NDB_ASSERT_OK(client.Connect(address));
  char message[1024] = {0};
  int len = snprintf(message, sizeof(message), "Hello, client %d\n", i);
  NDB_ASSERT(write(client.fd(), message, len) == len);
  while (true) {
    auto n = read(client.fd(), message, sizeof(message));
    if (n < 0) {
      if (errno == EAGAIN) {
        continue;
      } else {
        return;
      }
    }
    NDB_ASSERT(n == len);
    printf("Echo: %s", message);
    break;
  }
}

int Test(int argc, char* argv[]) {
  if (argc != 2) {
    fprintf(stderr, "Usage: %s <address>\n", argv[0]);
    return EXIT_FAILURE;
  }

  Echo echo;
  NDB_ASSERT_OK(echo.Run(argv[1]));

  for (int i = 0; i < 20; i++) {
    std::thread client([argv, i]() { ClientMain(argv[1], i); });
    client.join();
    sleep(1);
  }

  return EXIT_SUCCESS;
}
