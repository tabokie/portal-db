#ifndef PORTAL_CLIENT_H_
#define PORTAL_CLIENT_H_

#include "status.h"
#include "piece.h"

#include <vector>
#include <string>
#include <memory>

namespace portal_db {

class Client {
 public:
  Client() { }
  virtual ~Client() { }
  virtual Status Connect(std::string, std::string, std::string) = 0;
  virtual Status Close() = 0;
  virtual Status Get(const Key& key, KeyValue& ret) = 0;
  virtual Status Put(const Key& key, const Value& value) = 0;
  virtual Status Delete(const Key& key) = 0;
  virtual Status StartScan(const Key& lower, const Key& upper, bool sort) = 0;
  virtual Status FetchScan(std::vector<KeyValue>& ret) = 0;
  static std::unique_ptr<Client> NewClient();
};

} // namespace portal_db

#endif // PORTAL_CLIENT_H_