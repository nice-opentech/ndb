#include <fstream>
#include <hiredis/hiredis.h>

#include "ndb/ndb.h"

using namespace ndb;

#define ERROR(fmt, ...) do {                    \
    fprintf(stderr, fmt "\n", ## __VA_ARGS__);  \
    exit(EXIT_FAILURE);                         \
  } while (0)

bool split(const std::string& s, char delim, std::string* a, std::string* b) {
  auto pos = s.find(delim);
  if (pos == s.npos) return false;
  a->assign(s.substr(0, pos));
  b->assign(s.substr(pos+1, s.npos));
  return true;
}

void Check(redisReply* src, redisReply* dst) {
  NDB_ASSERT(src->type == dst->type);
  switch (src->type) {
    case REDIS_REPLY_NIL:
      break;
    case REDIS_REPLY_INTEGER:
      NDB_ASSERT(src->integer == dst->integer);
      break;
    case REDIS_REPLY_STRING:
      NDB_ASSERT(src->len == dst->len && memcmp(src->str, dst->str, dst->len) == 0);
      break;
    case REDIS_REPLY_ARRAY:
      NDB_ASSERT(src->elements == dst->elements);
      for (size_t i = 0; i < src->elements; i++) {
        Check(src->element[i], dst->element[i]);
      }
      break;
    default:
      ERROR("src->type %d dst->type %d", src->type, dst->type);
  }
}

void Compare(const std::string& input, const std::string& src_addr, const std::string& dst_addr) {
  redisContext* src = NULL;
  redisContext* dst = NULL;
  redisReply* src_rep = NULL;
  redisReply* dst_rep = NULL;

  std::string src_host, src_port;
  split(src_addr, ':', &src_host, &src_port);
  std::string dst_host, dst_port;
  split(dst_addr, ':', &dst_host, &dst_port);

  std::ifstream file(input);
  std::string line;
  size_t count = 0;
  while (std::getline(file, line)) {
    char cmd[1024];
    std::string type, key;
    split(line, ' ', &type, &key);
    if (type == "int" || type == "string") {
      snprintf(cmd, sizeof(cmd), "GET %s", key.c_str());
    } else if (type == "set") {
      snprintf(cmd, sizeof(cmd), "SMEMBERS %s", key.c_str());
    } else if (type == "hash") {
      snprintf(cmd, sizeof(cmd), "HGETALL %s", key.c_str());
    } else if (type == "list") {
      snprintf(cmd, sizeof(cmd), "LRANGE %s 0 -1", key.c_str());
    } else if (type == "zset") {
      snprintf(cmd, sizeof(cmd), "ZRANGE %s 0 -1", key.c_str());
    } else if (type == "oset") {
      snprintf(cmd, sizeof(cmd), "ORANGE %s 0 -1", key.c_str());
    } else {
      ERROR("type %s key %s", type.c_str(), key.c_str());
    }

    if (src == NULL) {
      src = redisConnect(src_host.c_str(), std::stoi(src_port));
    }
    if (dst == NULL) {
      dst = redisConnect(dst_host.c_str(), std::stoi(dst_port));
    }

    src_rep = (redisReply*) redisCommand(src, cmd);
    dst_rep = (redisReply*) redisCommand(dst, cmd);

    if (src_rep == NULL) {
      fprintf(stderr, "src cmd %s err %s\n", cmd, src->errstr);
      redisFree(src), src = NULL;
    }
    if (dst_rep == NULL) {
      fprintf(stderr, "dst cmd %s err %s\n", cmd, dst->errstr);
      redisFree(dst), dst = NULL;
    }

    if (src_rep != NULL && dst_rep != NULL) {
      Check(src_rep, dst_rep);
    }

    if (src_rep != NULL) freeReplyObject(src_rep);
    if (dst_rep != NULL) freeReplyObject(dst_rep);

    if (++count % 100000 == 0) {
      printf("input %s count %zu\n", input.c_str(), count);
    }
  }

  if (src != NULL) redisFree(src);
  if (dst != NULL) redisFree(dst);
  printf("DONE: input %s\n", input.c_str());
}

int main(int argc, char *argv[])
{
  if (argc != 4) {
    ERROR("Usage: compare <src_addr> <dst_addr> <nthreads>");
  }

  auto src_addr = argv[1];
  auto dst_addr = argv[2];
  auto nthreads = atoi(argv[3]);

  std::vector<std::thread> threads;
  for (auto i = 0; i < nthreads; i++) {
    auto filename = std::string("output") + "." + std::to_string(i);
    threads.emplace_back(Compare, filename, src_addr, dst_addr);
  }
  for (auto i = 0; i < nthreads; i++) {
    threads[i].join();
  }

  return 0;
}
