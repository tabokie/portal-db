#ifndef PORTAL_SERVER_H_
#define PORTAL_SERVER_H_

#include "status.h"

#include <string>
#include <memory>

namespace portal_db {

class Server {
 public:
  Server() { }
  virtual ~Server() { }
  virtual Status Open(std::string) = 0;
  virtual Status Serve(std::string, std::string) = 0;
  virtual Status Close() = 0;
  static std::unique_ptr<Server> NewServer();
};

} // namespace portal_db

#endif // PORTAL_SERVER_H_