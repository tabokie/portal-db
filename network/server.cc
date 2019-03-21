#include "server_impl.h"

#include <memory>

namespace portal_db {

std::unique_ptr<Server> Server::NewServer() {
  return std::make_unique<ServerImpl>();
}

}