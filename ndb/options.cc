#include "ndb/options.h"

namespace ndb {

#define CONFIG(v, t)                          \
  options_[#v].id = id++;                     \
  options_[#v].type = t;                      \
  options_[#v].name = #v;                     \
  options_[#v].value = (void*) &v;            \

Options::Options() {
  int id = 0;
  CONFIG(daemonize, kBool);
  CONFIG(restore, kString);

  CONFIG(logger.level, kInt);
  CONFIG(logger.filename, kString);

  CONFIG(engine.dbname, kString);
  CONFIG(engine.max_open_files, kInt);
  CONFIG(engine.WAL_ttl_seconds, kInt);
  CONFIG(engine.WAL_size_limit, kSize);
  CONFIG(engine.memtable_size, kSize);
  CONFIG(engine.block_cache_size, kSize);
  CONFIG(engine.compaction_cache_size, kSize);
  CONFIG(engine.background_threads, kInt);

  CONFIG(server.address, kString);
  CONFIG(server.num_workers, kInt);
  CONFIG(server.max_clients, kInt);
  CONFIG(server.timeout_secs, kInt);
  CONFIG(server.buffer_size, kSize);
  CONFIG(server.channel_size, kSize);

  CONFIG(replica.address, kString);
  CONFIG(replica.replicate_limit, kSize);

  CONFIG(command.access_mode, kString);
  CONFIG(command.max_arguments, kInt);
  CONFIG(command.slowlogs_maxlen, kInt);
  CONFIG(command.slowlogs_slower_than_usecs, kInt);
}

void Options::Usage() const {
  printf("Usage: ndb [OPTIONS]\n");
  printf("\n");
  printf("  -h --help                             show help\n");
  printf("  -v --version                          show version\n");
  printf("  -c --configfile <filename>            load configfile\n");
  printf("\n");
  printf("  --cluster.center <address>            cluster center address\n");
  printf("  --cluster.name <name>                 cluster unique name\n");
  printf("  --cluster.node <node>                 cluster unique node name\n");
  printf("  --cluster.slaveof <node>              cluster master node name\n");
  printf("\n");
  for (int id = 0; id < (int) options_.size(); id++) {
    for (const auto& opt : options_) {
      if (opt.second.id == id) {
        printf("  --%-35.35s (default: %s)\n",
               opt.second.name.c_str(), Get(opt.second.name).c_str());
        break;
      }
    }
  }
}

Result Options::Parse(int argc, char* argv[]) {
  bool test = false;
  std::string configfile;

  int i;
  for (i = 1; i < argc; i++) {
    auto name = argv[i];

    if (strcmp(name, "-t") == 0) {
      test = true;
      continue;
    }

    if (strcmp(name, "-h") == 0 || strcmp(name, "--help") == 0) {
      Usage();
      exit(EXIT_SUCCESS);
    }

    if (strcmp(name, "-v") == 0 || strcmp(name, "--version") == 0) {
      // UDATEME.
      printf("0.5.0\n");
      exit(EXIT_SUCCESS);
    }

    if (i + 1 >= argc) {
      return Result::Error("Missing option value.");
    }

    if (strcmp(name, "-c") == 0 || strcmp(name, "--configfile") == 0) {
      configfile = argv[++i];
      continue;
    }

    if (strcmp(name, "--cluster.center") == 0) {
      center.address = argv[++i];
      continue;
    }

    if (strcmp(name, "--cluster.name") == 0) {
      center.cluster = argv[++i];
      continue;
    }

    if (strcmp(name, "--cluster.node") == 0) {
      center.node = argv[++i];
      continue;
    }

    if (strcmp(name, "--cluster.slaveof") == 0) {
      center.slaveof = argv[++i];
      continue;
    }

    break;
  }

  // Precedence: Center < Configs < Options

  // Center.
  if (center.address.size() > 0) {
    if (center.cluster.size() == 0) {
      return Result::Error("Missing cluster.name.");
    }
    if (center.node.size() == 0) {
      return Result::Error("Missing cluster.node.");
    }
    center_.reset(new Center(center));
    NDB_TRY(center_->GetOptions(this));
  }

  // Configs.
  if (configfile.size() > 0) {
    NDB_TRY(ParseConfigs(configfile));
  }

  // Options.
  NDB_TRY(ParseOptions(argc - i, argv + i));

  if (test) {
    Usage();
    exit(EXIT_SUCCESS);
  }

  if (center_ != NULL) {
    if (center.slaveof.size() == 0) {
      // Master.
      NDB_TRY(center_->PutMaster(server.address));
    } else {
      NDB_TRY(center_->PutSlaves(server.address));
      NDB_TRY(center_->GetMaster(&replica.address));
    }
  }

  if (replica.address.size() > 0) {
    // slave readonly
    Set("command.access_mode", "r");
  }
  return Result::OK();
}

Result Options::ParseOptions(int argc, char* argv[]) {
  if (argc % 2 != 0) {
    return Result::Error("Invalid number of arguments.");
  }

  for (int i = 0; i < argc; i += 2) {
    if (strstr(argv[i], "--") != argv[i]) {
      return Result::Error("Invalid option: %s", argv[i]);
    }
    std::string name(argv[i] + 2);
    std::string value(argv[i + 1]);
    NDB_TRY(Set(name, value));
  }

  return Result::OK();
}

Result Options::ParseConfigs(const std::string& filename) {
  FILE* fp = fopen(filename.c_str(), "r");
  if (fp == NULL) {
    return Result::Errno("fopen()");
  }

  char* line = NULL;
  size_t linecapp = 0;
  while (getline(&line, &linecapp, fp) != -1) {
    std::istringstream iss(line);
    std::vector<std::string> args = {
      std::istream_iterator<std::string>{iss},
      std::istream_iterator<std::string>{}
    };
    if (args.size() == 0 || args[0][0] == '#') {
      continue;
    }
    if (args.size() != 2) {
      return Result::Error("Invalid option: %s", line);
    }
    NDB_TRY(Set(args[0], args[1]));
  }

  free(line);
  fclose(fp);
  return Result::OK();
}

std::string Options::Get(const std::string& name) const {
  auto it = options_.find(name);
  if (it == options_.end()) {
    return "";
  }

  if (it->second.name == "logger.level") {
    return Logger::FormatLevel(*(Logger::Level*) it->second.value);
  }

  if (it->second.type == kBool) {
    return *(bool*) it->second.value ? "true" : "false";
  }

  if (it->second.type == kInt) {
    return std::to_string(*(int*) it->second.value);
  }

  if (it->second.type == kSize) {
    const char* unit = "";
    auto v = *(size_t*) it->second.value;
    if (v % 1024 == 0) { v /= 1024; unit = "K"; }
    if (v % 1024 == 0) { v /= 1024; unit = "M"; }
    if (v % 1024 == 0) { v /= 1024; unit = "G"; }
    return std::to_string(v) + unit;
  }

  if (it->second.type == kString) {
    return *(std::string*) it->second.value;
  }

  return "";
}

Result Options::Set(const std::string& name, const std::string& value) {
  auto it = options_.find(name);
  if (it == options_.end()) {
    return Result::Error("Unknown option: %s %s", name.c_str(), value.c_str());
  }

  if (name == "logger.level") {
    if (!Logger::ParseLevel(value, (Logger::Level*) it->second.value)) {
      return Result::Error("Invalid option: %s %s", name.c_str(), value.c_str());
    }
    return Result::OK();
  }

  if (it->second.type == kBool) {
    bool v = true;
    if (value == "true") {
      v = true;
    } else if (value == "false") {
      v = false;
    } else {
      return Result::Error("Invalid boolean option: %s %s", name.c_str(), value.c_str());
    }
    *(bool*) it->second.value = v;
    return Result::OK();
  }

  if (it->second.type == kInt) {
    char* end = NULL;
    int v = strtol(value.c_str(), &end, 10);
    if (*end != '\0') {
      return Result::Error("Invalid int option: %s %s", name.c_str(), value.c_str());
    }
    *(int*) it->second.value = v;
    return Result::OK();
  }

  if (it->second.type == kSize) {
    char* end = NULL;
    size_t v = strtoull(value.c_str(), &end, 10);
    if (*end == 'k' || *end == 'K') {
      v <<= 10;
    } else if (*end == 'm' || *end == 'M') {
      v <<= 20;
    } else if (*end == 'g' || *end == 'G') {
      v <<= 30;
    } else if (*end != '\0') {
      return Result::Error("Invalid size option: %s %s", name.c_str(), value.c_str());
    }
    *(size_t*) it->second.value = v;
    return Result::OK();
  }

  if (it->second.type == kString) {
    *(std::string*) it->second.value = value;
    return Result::OK();
  }

  return Result::Error("Invalid option: %s %s", name.c_str(), value.c_str());
}

std::string Options::GetNodeID() const {
  if (center.cluster.size() > 0 && center.node.size() > 0) {
    return center.cluster + "." + center.node;
  }
  std::string host, port;
  if (ParseAddress(server.address, &host, &port)) {
    return port;
  }
  return server.address;
}

Result Options::GetNamespaces(std::map<std::string, Configs>* nss) const {
  if (center_ == NULL) {
    return Result::Error("No center specified.");
  }
  return center_->GetNamespaces(nss);
}

}  // namespace ndb
