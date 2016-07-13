#ifndef NDB_COMMON_STATS_H_
#define NDB_COMMON_STATS_H_

#include "ndb/common/common.h"

namespace ndb {

class Stats {
 public:
  template <class T>
  void insert(const std::string& name, T value) {
    names_.push_back(name);
    values_.push_back(std::to_string(value));
  }

  void insert(const std::string& name, const char* value) {
    names_.push_back(name);
    values_.push_back(value);
  }

  void insert(const std::string& name, const std::string& value) {
    names_.push_back(name);
    values_.push_back(value);
  }

  const char* Print() {
    buf_.clear();
    for (size_t i = 0; i < names_.size(); i++) {
      buf_ += names_[i] + " = " + values_[i] + "\r\n";
    }
    return buf_.c_str();
  }

 private:
  std::string buf_;
  std::vector<std::string> names_;
  std::vector<std::string> values_;
};

}  // namespace ndb

#endif /* NDB_COMMON_STATS_H_ */
