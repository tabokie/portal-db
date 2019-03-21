#ifndef PORTAL_NETWORK_SERVER_IMPL_H_
#define PORTAL_NETWORK_SERVER_IMPL_H_

#include "socket.h"
#include "portal_db/server.h"
#include "portal_db/status.h"
#include "portal_db/piece.h"
#include "util/util.h"

#include <memory>
#include <thread>
#include <cstdint>
#include <functional>
#include <vector>

namespace portal_db {

class ServerImpl : public Server, public NoMove {
 public:
  ServerImpl() { }
  ~ServerImpl() { }
  Status Open(std::string s) {
    return Status::OK();
  }
  Status Serve(std::string ip, std::string port) {
    std::cout << "server on" << std::endl;
    Status ret = InitSocket(ip, port);
    if(!ret.ok()) return ret;
    SOCKET connect;
    if( listen(server_, SOMAXCONN) == SOCKET_ERROR ) {
      closesocket(server_);
      return Status::IOError(
        std::to_string(WSAGetLastError()) + 
        " - error while listening"
        );
    }
    // wait for client connections
    do {
      connect = accept(server_, NULL, NULL);
      if(connect == INVALID_SOCKET) {
        if(server_ != INVALID_SOCKET) {
          return Status::IOError("error when accept");
        }
        break; // clean up
      }
      connections.push_back(std::thread(std::mem_fn(&ServerImpl::Spawn), this, connect));
    } while(true);
    if(server_ != INVALID_SOCKET) {
      closesocket(server_);
      server_ = INVALID_SOCKET;
    }
    return Status::OK();
  }
  Status Close() {
    closesocket(server_);
    server_ = INVALID_SOCKET;
    return Status::OK();
  }
 private:
  static constexpr size_t buffer_len_ = 512;
  SOCKET server_ = INVALID_SOCKET;
  std::vector<std::thread> connections;
  Status InitSocket(std::string addr, std::string port) {
    server_ = INVALID_SOCKET;
    if(!WSAProof::Ok()) {
      return Status::Corruption("WSAProof is down");
    }
    struct addrinfo *result, *ptr, hints;
    ZeroMemory(&hints, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    // Resolve the server address and port
    int iResult = getaddrinfo(addr.c_str(), port.c_str(), &hints, &result);
    if ( iResult != 0 ) {
      freeaddrinfo(result);
      return Status::InvalidArgument(
        std::to_string(iResult) +
        " - error resolving address"
        );
    }
    SOCKET ret = socket(result->ai_family, result->ai_socktype, result->ai_protocol);
    iResult = bind(ret, result->ai_addr, (int)result->ai_addrlen);
    if ( iResult == SOCKET_ERROR) {
      freeaddrinfo(result);
      return Status::IOError(
        std::to_string(WSAGetLastError()) +
        " - error binding address"
        );
    }
    freeaddrinfo(result);
    server_ = ret;
    return Status::OK();
  }
  void Spawn(SOCKET socket) {
    KeyValue tmp = KeyValue::from_string("hi", "this is server");
    char recvbuf[buffer_len_ + 1];
    int recvbuflen = buffer_len_;
    do {
      int iResult = recv(socket, recvbuf, recvbuflen, 0);
      if (iResult > 0) {
        recvbuf[iResult] = '\0';
        // process and reply
        std::string message(recvbuf, iResult);
        std::cout << "recv " << message << std::endl;
        if(!Send(socket, tmp)) std::cout << "Send failed" << std::endl;
      } else if( iResult == 0 ){
        break;
      } else {
        closesocket(socket);
        // return Status::IOError("error during recv");
      }
    } while(true);
    if(socket != INVALID_SOCKET) 
      closesocket(socket);
  }
  bool Send(SOCKET target, std::string message) {
    if(message.size() == 0) return true;
    int iSendResult = send(target, message.c_str(), message.length(), 0);
    if(iSendResult == SOCKET_ERROR) return false;
    return true;
  }
  bool Send(SOCKET target, const KeyValue& data) {
    static char buf[256+8];
    uint32_t len = 256 + 8 + 4; // marker size = 4
    int iSendResult = send(target, reinterpret_cast<char*>(&len), 4, 0);
    if(iSendResult == SOCKET_ERROR) return false;
    iSendResult = send(target, data.raw_ptr(), 8, 0);
    if(iSendResult == SOCKET_ERROR) return false;
    iSendResult = send(target, data.pointer_to_slice<0,256>(), 256, 0);
    if(iSendResult == SOCKET_ERROR) return false;
    return true;
  }
};

} // namespace portal_db

#endif // PORTAL_NETWORK_SERVER_IMPL_H_