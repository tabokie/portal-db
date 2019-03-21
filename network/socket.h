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

// assume WIN_PORT
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

/*
// one client handles at most one connection
class Client {
 private:
  std::atomic<bool> close_ = false;
  SOCKET socket_ = INVALID_SOCKET;
  char* buffer_;
  int buffer_len_;
 public:
  Client(int bufflen = 512): buffer_len_(bufflen) {
    buffer_ = new char[buffer_len_ + 1];
  }
  Client(Client&& rhs)
      : close_(rhs.close_.load()),
        socket_(rhs.socket_),
        buffer_len_(rhs.buffer_len_),
    buffer_(rhs.buffer_){
    rhs.buffer_ = nullptr;
  }
  // Client(const Client& rhs) = delete;
  Client(const Client& rhs)
      : close_(rhs.close_.load()),
        socket_(rhs.socket_),
        buffer_len_(rhs.buffer_len_) {
    buffer_ = new char[buffer_len_ + 1];      
  }
  ~Client() {
    delete [] buffer_;
  }
  bool is_closed(void) {
    return close_.load() && socket_ == INVALID_SOCKET;
  }
  bool Connect(std::string ip, std::string port) {
    if(socket_ != INVALID_SOCKET) return false; // can't just overwrite a using socket
    struct addrinfo hints, *result, *ptr;
    ZeroMemory( &hints, sizeof(hints) );
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    int iResult = getaddrinfo(ip.c_str(), port.c_str(), &hints, &result);
    if(iResult != 0) {
      colog.error("error resolving address: ", WSAGetLastError());
      return false;
    } else if(result == NULL) {
      colog.error("error: target address is empty");
      return false;
    }
    for(ptr = result; ptr != NULL; ptr = ptr->ai_next) {
      socket_ = socket(ptr->ai_family, ptr->ai_socktype, ptr->ai_protocol);
      iResult = connect(socket_, ptr->ai_addr, (int)ptr->ai_addrlen);
      if(iResult == SOCKET_ERROR) {
        colog.error("connect failed", WSAGetLastError());
        closesocket(socket_);
        socket_ = INVALID_SOCKET;
        continue;
      }
      break; // find one suitable
    }
    if(socket_ == INVALID_SOCKET) {
      colog.error("error trying to connect: " , WSAGetLastError());
    }
    freeaddrinfo(result);
    return socket_ != INVALID_SOCKET;
  }
  void Close() {
    if(socket_ == INVALID_SOCKET) return ;
    int iResult = shutdown(socket_, SD_SEND);
    if(iResult == SOCKET_ERROR) {
      colog.error("error trying to shutdown: ", WSAGetLastError());
      closesocket(socket_);
    }
    socket_ = INVALID_SOCKET;
  }
  bool Send(std::string message) {
    if(socket_ == INVALID_SOCKET) return false;
    int iResult = send(socket_, message.c_str(), message.length(), 0);
    if(iResult == SOCKET_ERROR) {
      colog.error("error sending `", message, "`: ", WSAGetLastError());
      return false;
    }
    return true;
  }
  std::string Receive() {
    if(socket_ == INVALID_SOCKET) return "";
    int iResult = recv(socket_, buffer_, buffer_len_, 0);
    if(iResult == 0) {
      socket_ = INVALID_SOCKET;
      colog.error("error: socket already closed trying to receive");
      return "";
    }
    else if(iResult < 0){
      socket_ = INVALID_SOCKET;
      colog.error("error trying to receive: ", WSAGetLastError());
      return "";
    }
    std::string ret(iResult, ' ');
    memcpy((void*)ret.c_str(), (void*)buffer_, iResult);
    return ret;
  }
};

// able to manage multiple sockets
// share connectors infomation
class Server: public NoCopy {
  // static const WSAProof wsa_;
  struct ClientSocketInfo {
    SOCKET handle = INVALID_SOCKET;
    std::string addr; // ip:port form
    bool status = true;
    ClientSocketInfo(SOCKET sck, std::string adr): addr(adr), handle(sck) { }
    std::string toString(int id) {
      return std::string("{") + 
        "id:" + std::to_string(id) +
        ", address:" + addr +
        ", status:" + (status? "on" : "off") +
        "}";
    }
  };
  std::mutex database_lock_;
  std::vector<ClientSocketInfo> database_;
  std::string name_;
  std::atomic<bool> close_ = false; // close all socket and on-going connect
  std::mutex list_lock_;
  std::vector<SOCKET> delegated_list_;;
 public:
  static constexpr size_t kDefaultBufferLen = 512;
  static const char* kDefaultPort;
  Server(std::string n): name_(n) { }
  std::string name(void) {
    return name_;
  }
  void OutputDatabase(std::vector<std::string>& header, std::vector<std::string>& entries) {
    std::lock_guard<std::mutex> lk(database_lock_);
    std::string ret;
    int id = 0;
    header.push_back("id");
    header.push_back("address");
    header.push_back("status");
    for(auto& client: database_) {
      entries.push_back(std::to_string(id));
      entries.push_back(client.addr);
      entries.push_back((client.status ? "on" : "off"));
      id ++;
    }
  }
  void Close(void) {
    close_.store(true);
    list_lock_.lock();
    for(auto socket : delegated_list_) {
      closesocket(socket);
    }
    list_lock_.unlock();
  }

  SOCKET GetClient(int id) {
    std::lock_guard<std::mutex> lk(database_lock_);
    if(id < 0 || id >= database_.size()) {
      return INVALID_SOCKET;
    }
    return database_[id].handle;
  }
  std::string GetAddr(int id) {
    std::lock_guard<std::mutex> lk(database_lock_);
    if(id < 0 || id >= database_.size()) {
      return "";
    }
    return database_[id].addr;
  }
  void CloseClient(int id) {
    std::lock_guard<std::mutex> lk(database_lock_);
    if(id < 0 || id >= database_.size()) return;
    database_[id].status = false;
    closesocket(database_[id].handle);
  }
  bool Send(SOCKET target, std::string message) {
    if(message.size() == 0) return true;
    // message = GetSocketAddr(target) + " reply: " + message;
    int iSendResult = send(target, message.c_str(), message.length(), 0);
    if(iSendResult == SOCKET_ERROR) return false;
    return true;
  }
  template <typename ResponsFunction>
  bool Serve(SOCKET server, ResponsFunction&& response) {
    list_lock_.lock();
    delegated_list_.push_back(server);
    list_lock_.unlock();
    return Serve(server, std::move(response), [&]()-> bool {return close_.load();});
  }
  template <typename ResponsFunction, typename CallbackFunction>
  bool Serve(SOCKET server, ResponsFunction&& response, CallbackFunction&& should_close) {
    SOCKET client;
    // std::vector<std::future<bool>> rets;
    std::vector<std::thread> threads;
    if (listen(server, SOMAXCONN) == SOCKET_ERROR) { // open listen port
        colog.error("error while listening: ", WSAGetLastError());
        closesocket(server);
        return false;
    }
    do {
      client = accept(server, NULL, NULL); // block here
      if(client == INVALID_SOCKET) {
        if(!should_close()) // not due to voluntary closing
          colog.error("error when accepting new connection");
        break; // fatal error
      }
      // add client message to database
      database_lock_.lock();
      database_.push_back(ClientSocketInfo(client, GetPeerAddr(client))) ;
      // int id = database_.size() - 1;
      database_lock_.unlock();
      // create a new thread
      int id = database_.size() - 1;
      threads.push_back(std::thread(std::move([&, c=client, id]()-> bool{
        char recvbuf[kDefaultBufferLen + 1]; // fuck
        int recvbuflen = kDefaultBufferLen;
        do{
          int iResult = recv(c, recvbuf, recvbuflen, 0);
          if(iResult > 0) {
            recvbuf[iResult] = '\0';
            std::vector<std::string> resps;
            response(c, recvbuf, iResult, resps);
            for(auto& r: resps) {
              if(!Send(c, r)) {
                colog.error("error trying to send: ", WSAGetLastError() );
                CloseClient(id);
                return false; 
              }
            }
          }
          else if(iResult == 0) { // closed by client
            break;
          }
          else {
            colog.error("error during recv: ", WSAGetLastError());
            CloseClient(id);
            return false;
          }
        } while(!should_close());
        CloseClient(id);
        return true;
      })));
    } while(!should_close()); // a success connection, continue?
    // join all subthread
    std::for_each(threads.begin(), threads.end(), std::mem_fn(&std::thread::join) );
    closesocket(server);
    return true;
  }

  static SOCKET NewSocket(const char* addr) {
    return NewSocket(addr, kDefaultPort);
  }
  static SOCKET NewSocket(const char* addr, const char* port) {
    if(!WSAProof::Ok())return INVALID_SOCKET;
    struct addrinfo *result, *ptr, hints;
    ZeroMemory(&hints, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    // Resolve the server address and port
    int iResult = getaddrinfo(addr, port, &hints, &result);
    if ( iResult != 0 ) {
      colog.error("error resolving address: ", iResult);
      freeaddrinfo(result);
      return INVALID_SOCKET;
    }
    SOCKET ret = socket(result->ai_family, result->ai_socktype, result->ai_protocol);
    iResult = bind(ret, result->ai_addr, (int)result->ai_addrlen);
    if ( iResult == SOCKET_ERROR) {
      colog.error("error binding: " , WSAGetLastError());
      freeaddrinfo(result);
      return INVALID_SOCKET;
    }
    freeaddrinfo(result);
    return ret;
  }
  static std::string GetSocketAddr(SOCKET socket) {
    sockaddr_in addr;
    memset(&addr,0,sizeof(addr));
    int len = sizeof(addr);
    int ret = getsockname(socket,(sockaddr*)&addr,&len);
    if (ret != 0) {
      colog.error("error getting sock addr");
      return "";
    }
    char ipAddr[INET_ADDRSTRLEN];
    return std::string(inet_ntop(AF_INET, &addr.sin_addr, ipAddr, sizeof(ipAddr))) + ":" + std::to_string(ntohs(addr.sin_port));
  }
  static std::string GetPeerAddr(SOCKET socket) {
    sockaddr_in addr;
    memset(&addr,0,sizeof(addr));
    int len = sizeof(addr);
    int ret = getpeername(socket,(sockaddr*)&addr,&len);
    if (ret != 0) {
      colog.error("error getting peer addr");
      return "";
    }
    char ipAddr[INET_ADDRSTRLEN];
    return std::string(inet_ntop(AF_INET, &addr.sin_addr, ipAddr, sizeof(ipAddr))) +":"+ std::to_string(ntohs(addr.sin_port));
  }

};
*/

} // namespace portal_db

#endif // PORTAL_NETWORK_SOCKET_H_