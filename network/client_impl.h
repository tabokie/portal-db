#ifndef PORTAL_NETWORK_CLIENT_IMPL_H_
#define PORTAL_NETWORK_CLIENT_IMPL_H_

#include "socket.h"
#include "portal_db/client.h"
#include "portal_db/status.h"
#include "util/channel.h"
#include "util/util.h"

#include <thread>
#include <memory>
#include <string>

namespace portal_db {

// Client Protocol //
// 3 bytes of OP code + optional parameter
// GET + 8-byte key
// PUT + 8-byte key + 256-byte value
// DEL + 8-byte key
// SCN + 8-byte lower + 8-byte upper
// MOR + 4-byte size
class ClientImpl : public Client, public NoCopy {
 public:
  ClientImpl() { }
    // : mailbox_(std::make_unique<Channel<std::string, 10>>()) { }
  ClientImpl(ClientImpl&& rhs)
    // : mailbox_(std::move(rhs.mailbox_)), 
    : connection_(std::move(rhs.connection_)) { }
  ~ClientImpl() { }
  Status Connect(std::string ip, std::string port, std::string name);
  Status Close();
  Status Get(const Key& key, KeyValue& ret);
  Status Put(const Key& key, const Value& value);
  Status Delete(const Key& key);
  Status StartScan(const Key& lower, const Key& upper, bool sort);
  Status FetchScan(std::vector<KeyValue>& ret);
 private:
  static constexpr size_t buffer_len_ = 512;
  SOCKET socket_ = INVALID_SOCKET;
  // initially used to coordinate async data
  // std::unique_ptr<Channel<std::string, 10>> mailbox_;
  std::thread connection_; // one connection at a time
  char buffer[buffer_len_ + 1];
  // support message reformat
  uint32_t cur = 0;
  Status Send(std::string message) {
    return Send(message.c_str(), message.length());
  }
  Status Send(const char* p, size_t len);
  Status Receive();
  Status Receive(KeyValue& ret);
  // receive kvs with size header
  Status Receive(std::vector<KeyValue>& ret);
};

} // namespace portal_db

#endif // PORTAL_NETWORK_CLIENT_IMPL_H_