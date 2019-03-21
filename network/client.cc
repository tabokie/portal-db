#include "client_impl.h"

#include <memory>

namespace portal_db {

std::unique_ptr<Client> Client::NewClient() {
  return std::make_unique<ClientImpl>();
}

}