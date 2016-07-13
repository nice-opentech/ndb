#include "units/units.h"

void Send(Channel<char>* channel, int n) {
  for (int i = 0; i < n; i++) {
    channel->Send(new char('c'));
  }
}

void Recv(Channel<char>* channel, int n) {
  int count = 0;
  while (count != n) {
    auto c = channel->Recv();
    if (c == NULL) {
      usleep(100);
      continue;
    }
    NDB_ASSERT(*c == 'c');
    delete c;
    count++;
  }
}

int Test(int argc, char *argv[])
{
  Channel<char> channel(4096);

  std::thread s1(Send, &channel, 1000);
  std::thread s2(Send, &channel, 1000);
  std::thread s3(Send, &channel, 1000);
  std::thread s4(Send, &channel, 1000);
  std::thread r1(Recv, &channel, 2000);
  std::thread r2(Recv, &channel, 2000);
  s1.join(), s2.join(), s3.join(), s4.join();
  r1.join(), r2.join();

  std::thread t1(Send, &channel, 8192);
  t1.join();

  return EXIT_SUCCESS;
}
