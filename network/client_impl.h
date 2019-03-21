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

class ClientImpl : public Client, public NoCopy {
 public:
  ClientImpl()
    : mailbox_(std::make_unique<Channel<std::string, 10>>()) { }
  ClientImpl(ClientImpl&& rhs)
    : mailbox_(std::move(rhs.mailbox_)), 
      connection_(std::move(rhs.connection_)) { }
  ~ClientImpl() { }
  Status Connect(std::string ip, std::string port, std::string name) {
    if(socket_ != INVALID_SOCKET)
      return Status::Corruption("can't override existing socket");
    struct addrinfo hints, *result, *ptr;
    ZeroMemory( &hints, sizeof(hints) );
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    int iResult = getaddrinfo(ip.c_str(), port.c_str(), &hints, &result);
    if(iResult != 0) {
      return Status::InvalidArgument(
        std::to_string(WSAGetLastError()) + 
        " - error when resolving address"
      );
    } else if(result == NULL) {
      return Status::InvalidArgument(
        "target address is empty"
      );
    }
    for(ptr = result; ptr != NULL; ptr = ptr->ai_next) {
      socket_ = socket(ptr->ai_family, ptr->ai_socktype, ptr->ai_protocol);
      iResult = connect(socket_, ptr->ai_addr, (int)ptr->ai_addrlen);
      if(iResult == SOCKET_ERROR) {
        // colog.error("connect failed", WSAGetLastError());
        closesocket(socket_);
        socket_ = INVALID_SOCKET;
        continue;
      }
      break; // find one suitable
    }
    freeaddrinfo(result);
    if(socket_ == INVALID_SOCKET) {
      return Status::IOError(
        std::to_string(WSAGetLastError()) + 
        " - error when trying to connect"
      );
    }
    return Status::OK();
  }
  Status Close() {
    if(socket_ == INVALID_SOCKET)
      return Status::Corruption("invalid socket");
    int iResult = shutdown(socket_, SD_SEND);
    if(iResult == SOCKET_ERROR) {
      closesocket(socket_);
      return Status::IOError(
        std::to_string(WSAGetLastError()) + 
        " - error when trying to shutdown"
      );
    }
    socket_ = INVALID_SOCKET;
    return Status::OK();
  }
  Status Get(const Key& key, KeyValue& ret) {
    if(socket_ == INVALID_SOCKET)
      return Status::Corruption("invalid socket");
    Status status = Send("GET");
    if(!status.ok()) return status;
    status *= Receive(ret);
    return status;
  }
  Status Put(const Key& key, const Value& value) {
    if(socket_ == INVALID_SOCKET)
      return Status::Corruption("invalid socket");
    return Status::OK();
  }
  Status Delete(const Key& key) {
    if(socket_ == INVALID_SOCKET)
      return Status::Corruption("invalid socket");
    return Status::OK();
  }
  Status StartScan(const Key& lower, const Key& upper, bool sort) {
    if(socket_ == INVALID_SOCKET)
      return Status::Corruption("invalid socket");
    return Status::OK();
  }
  Status FetchScan(std::vector<KeyValue>& ret) {
    if(socket_ == INVALID_SOCKET)
      return Status::Corruption("invalid socket");
    return Status::NotFound("not implemented");
    // return Status::OK();
  }
  Status CloseScan() {
    if(socket_ == INVALID_SOCKET)
      return Status::Corruption("invalid socket");
    return Status::OK();
  }
 private:
  static constexpr size_t buffer_len_ = 256 + 8;
  SOCKET socket_ = INVALID_SOCKET;
  std::unique_ptr<Channel<std::string, 10>> mailbox_;
  std::thread connection_; // one connection at a time
  Status Send(std::string message) {
    if(socket_ == INVALID_SOCKET) return Status::Corruption("invalid socket");
    int iResult = send(socket_, message.c_str(), message.length(), 0);
    if(iResult == SOCKET_ERROR) {
      return Status::IOError(
        std::to_string(WSAGetLastError()) + 
        " - error sending message"
        );
    }
    return Status::OK();
  }
  Status Receive(KeyValue& ret) {
    static char buffer[buffer_len_ + 1];
    if(socket_ == INVALID_SOCKET) 
      return Status::Corruption("invalid socket");
    uint32_t len = 0;
    int cur = 0;
    do {
      int start = 0;
      int iResult = recv(socket_, buffer, buffer_len_, 0);
      if(iResult == 0) {
        socket_ = INVALID_SOCKET;
        return Status::IOError("socket already closed");
      }
      else if(iResult < 0){
        socket_ = INVALID_SOCKET;
        return Status::IOError(
          std::to_string(WSAGetLastError()) + 
          " - error trying to recv"
          );
      }
      // std::cout << "iResult = " << iResult << std::endl;
      if(len == 0) {
        assert(iResult >= 4);
        len = *(reinterpret_cast<uint32_t*>(buffer));
        start = 4;
        // std::cout << "len = " << len << std::endl;
      }
      len -= iResult;
      for(cur; cur < 8 && start < iResult; cur ++, start ++) {
        ret[cur] = buffer[start];
      }
      if(cur >= 8 && cur < 256 + 8 && start < iResult) {
        ret.copy(cur-8, min(iResult - start, 264-cur), buffer + start);
        cur += iResult - start;
      }
    } while(len > 0 && cur < 256 + 8);
    return Status::OK();
  }
};

} // namespace portal_db

#endif // PORTAL_NETWORK_CLIENT_IMPL_H_