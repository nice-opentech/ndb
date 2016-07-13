#ifndef NDB_ENGINE_BACKUP_H_
#define NDB_ENGINE_BACKUP_H_

#include "ndb/engine/common.h"

namespace ndb {

class Backup {
 public:
  Backup(rocksdb::DB* db);

  ~Backup();

  Result Create(const std::string& backup);

  void Stop();

  bool IsFinished() const { return finish_time_ > 0; }

  Stats GetStats() const;

 private:
  void Join();

 private:
  rocksdb::DB* db_ {NULL};
  rocksdb::BackupEngine* engine_ {NULL};
  Status status_;
  std::mutex lock_;
  std::string backup_;
  std::thread thread_;
  time_t create_time_ {0};
  time_t finish_time_ {0};
};

Result RestoreDB(const std::string& backup, const std::string& dbname);

}  // namespace ndb

#endif /* NDB_ENGINE_BACKUP_H_ */
