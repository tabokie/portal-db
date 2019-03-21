#include "portal_db/server.h"
#include "portal_db/status.h"

using namespace portal_db;

int main(void) {

  std::unique_ptr<Server> s = Server::NewServer();
  Status ret = s.get()->Open("test_network");
  if(!ret.inspect()) {
    exit(1);
  }
  ret *= s.get()->Serve("127.0.0.1", "693");
  if(!ret.inspect()) {
    exit(1);
  }
  ret *= s.get()->Close();
  if(!ret.inspect()) {
    exit(1);
  }

  return 0;
}