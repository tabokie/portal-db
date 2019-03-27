#ifndef PORTAL_NETWORK_SOCKET_H_
#define PORTAL_NETWORK_SOCKET_H_

#include "portal_db/port.h"

#include <iostream>
#include <istream>
#include <ostream>
#include <vector>
#include <future>
#include <thread>
#include <string>

#ifndef WIN_PLATFORM
  #error "socket port not implemented"
#endif

// #define WIN32_LEAN_AND_MEAN
// Need to link with Ws2_32.lib, Mswsock.lib, and Advapi32.lib
#pragma comment (lib, "Ws2_32.lib")
// #pragma comment (lib, "Mswsock.lib")
// #pragma comment (lib, "AdvApi32.lib")

namespace portal_db {

// static setup for windows platform
struct WSAProof {
  friend class Socket;
 private:
  WSADATA wsa_data_;
  int startup_result_;
  static const WSAProof wsa_; // shared by Server and Client
 public:
  static bool Ok(void) { // why can't const?
    return wsa_.ok();
  }
  WSAProof(){
    startup_result_ = WSAStartup(MAKEWORD(2,2), &wsa_data_);
  }
  ~WSAProof(){
    WSACleanup();
  }
  bool ok(void) const {
    return startup_result_ == 0;
  }
};

class Socket {
 public:
  static std::string GetSocketAddr(SOCKET socket) {
    sockaddr_in addr;
    memset(&addr,0,sizeof(addr));
    int len = sizeof(addr);
    int ret = getsockname(socket,(sockaddr*)&addr,&len);
    if (ret != 0) {
      return "";
    }
    char ipAddr[INET_ADDRSTRLEN];
    return std::string(
      inet_ntop(AF_INET, &addr.sin_addr, ipAddr, sizeof(ipAddr))) +
       ":" + 
      std::to_string(ntohs(addr.sin_port));
  }
  static std::string GetPeerAddr(SOCKET socket) {
    sockaddr_in addr;
    memset(&addr,0,sizeof(addr));
    int len = sizeof(addr);
    int ret = getpeername(socket,(sockaddr*)&addr,&len);
    if (ret != 0) {
      return "";
    }
    char ipAddr[INET_ADDRSTRLEN];
    return std::string(inet_ntop(AF_INET, &addr.sin_addr, ipAddr, sizeof(ipAddr))) +
      ":"+ 
      std::to_string(ntohs(addr.sin_port));
  }
};

} // namespace portal_db

#endif // PORTAL_NETWORK_SOCKET_H_