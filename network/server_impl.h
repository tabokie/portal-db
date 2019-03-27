#ifndef PORTAL_NETWORK_SERVER_IMPL_H_
#define PORTAL_NETWORK_SERVER_IMPL_H_

#include "socket.h"
#include "portal_db/server.h"
#include "portal_db/status.h"
#include "portal_db/piece.h"
#include "util/util.h"
#include "db/persist_hash_trie.h"
#include "db/hash_trie_iterator.h"

#include <memory>
#include <thread>
#include <cstdint>
#include <functional>
#include <vector>

namespace portal_db {

// Server Protocol //
// 4 bytes of size + 8 + 256 segment = record
// 4 bytes of 0 + 4 bytes of len + message = error / return
class ServerImpl : public Server, public NoMove {
 public:
  ServerImpl() { }
  ~ServerImpl() { }
  // public interface
  Status Open(std::string s);
  Status Serve(std::string ip, std::string port);
  Status Close();
 private:
  // recv buffer length
  static constexpr size_t buffer_len_ = 512;
  SOCKET server_ = INVALID_SOCKET;
  // alpha version use threads to manage connections
  std::vector<std::thread> connections;
  // hold reference to db
  std::unique_ptr<PersistHashTrie> pstore;
  // setup server listener
  Status InitSocket(std::string addr, std::string port);
  // spawned as thread when new connection arrived
  void Spawn(SOCKET socket);
  // send message with padding
  bool Send(SOCKET target, std::string message);
  // send key-value
  bool Send(SOCKET target, const KeyValue& data);
  bool Send(SOCKET target, const Key& key, const Value& value);
};

} // namespace portal_db

#endif // PORTAL_NETWORK_SERVER_IMPL_H_