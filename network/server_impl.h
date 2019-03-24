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
  Status Open(std::string s) {
    pstore = std::make_unique<PersistHashTrie>(s);
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
  std::unique_ptr<PersistHashTrie> pstore;
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
    std::cout << "Spawning" << std::endl;
    char recvbuf[buffer_len_ + 1];
    int recvbuflen = buffer_len_;
    int cur = 0;
    std::string tmp;
    HashTrieIterator tmp_iterator = HashTrieIterator(true);
    Key tmp_key;
    Value tmp_value = Value::from_string("");
    bool in_session = false;
    uint32_t iterate_size;
    do {
     POLL:
      int iResult = recv(socket, recvbuf + cur, recvbuflen - cur, 0);
      if (iResult > 0) {
        cur += iResult;
        recvbuf[cur] = '\0';
        if(cur >= 3) {
          if(tmp.size()==0) tmp = std::string(recvbuf, 3);
          if(tmp == "PUT" && cur >= 3 + 8 + 256) {
            goto PROC_PUT;
          } else if(tmp == "GET" && cur >= 3 + 8) {
            goto PROC_GET;
          } else if(tmp == "DEL" && cur >= 3 + 8) {
            goto PROC_DELETE;
          } else if(tmp == "SCN" && cur >= 3 + 8 + 8) {
            goto PROC_SCAN;
          } else if(tmp == "MOR" && cur >= 3 && in_session) {
            goto PROC_MORE;
          }
        }
      } else if( iResult == 0 ){
        break;
      } else {
        closesocket(socket);
      }
    } while(true);
    if(socket != INVALID_SOCKET) 
      closesocket(socket);
    std::cout << "Exit" << std::endl;
    return;

   PROC_PUT:
    std::cout << "PUT" << std::endl;
    for(int i = 0; i < 8; i++) tmp_key[i]=recvbuf[3+i];
    tmp_value.copy<0,256>(recvbuf + 3 + 8);
    if(!Send(socket, pstore.get()->Put(tmp_key, tmp_value).ToString())) 
      std::cout << "Send Failed" << std::endl;
    for(int i = 0; i + 267 < cur; i++) recvbuf[i] = recvbuf[i+267];
    cur -= 267;
    in_session = false;
    goto PROC_END;
   PROC_GET:
    std::cout << "GET" << std::endl;
    {
      for(int i = 0; i < 8; i++) tmp_key[i]=recvbuf[3+i];
      Status ret = pstore.get()->Get(tmp_key, tmp_value);
      if(!ret.ok()) {
        if(!Send(socket, ret.ToString())) 
          std::cout << "Send Failed" << std::endl;
      } else {
        if(!Send(socket, tmp_key, tmp_value)) 
          std::cout << "Send Failed" << std::endl;
      }
    }
    for(int i = 0; i + 11 < cur; i++) recvbuf[i] = recvbuf[i+11];
    cur -= 11;
    in_session = false;
    goto PROC_END;
   PROC_DELETE:
    std::cout << "DEL" << std::endl;
    for(int i = 0; i < 8; i++) tmp_key[i]=recvbuf[3+i];
    if(!Send(socket, pstore.get()->Delete(tmp_key).ToString())) 
      std::cout << "Send Failed" << std::endl;
    for(int i = 0; i + 11 < cur; i++) recvbuf[i] = recvbuf[i+11];
    cur -= 11;
    in_session = false;
    goto PROC_END;
   PROC_SCAN:
    std::cout << "SCN" << std::endl;
    {
      for(int i = 0; i < 8; i++) tmp_key[i]=recvbuf[3+i];
      Key upper(recvbuf + 3 + 8);
      if(!Send(socket, pstore.get()->Scan(tmp_key, upper, tmp_iterator).ToString()))
        std::cout << "Send Failed" << std::endl;
    }
    for(int i = 0; i + 19 < cur; i++) recvbuf[i] = recvbuf[i+19];
    cur -= 19;
    in_session = true;
    goto PROC_END;
   PROC_MORE:
    std::cout << "MOR" << std::endl;
    {
      uint32_t size = 0;
      if(!tmp_iterator.Next()) {
        size = 0;
      } else size = tmp_iterator.size();
      if(send(socket, (char*)&size, 4, 0) == SOCKET_ERROR) // bug
        std::cout << "Send Failed" << std::endl;
      for(int i = 0; i < size; i++) {
        if(i > 0 && !tmp_iterator.Next()) 
          std::cout << "Early Exaustion" << std::endl;
        const KeyValue& kv = tmp_iterator.Peek();
        if(!Send(socket, kv)) std::cout << "Send Failed" << std::endl;
      }
    }
    for(int i = 0; i + 3 < cur; i++) recvbuf[i] = recvbuf[i+3];
    cur -= 3;
   PROC_END:
    tmp = "";
    goto POLL;
  }
  bool Send(SOCKET target, std::string message) {
    if(message.size() == 0) return true;
    char header[8];
    *(reinterpret_cast<uint32_t*>(header)) = 0;
    *(reinterpret_cast<uint32_t*>(header + 4)) = message.size();
    int iSendResult = send(target, header, 8, 0);
    if(iSendResult == SOCKET_ERROR) return false;
    iSendResult = send(target, message.c_str(), message.length(), 0);
    if(iSendResult == SOCKET_ERROR) return false;
    return true;
  }
  bool Send(SOCKET target, const KeyValue& data) {
    // static char buf[256+8];
    // uint32_t len = 256 + 8 + 4; // marker size = 4
    // int iSendResult = send(target, reinterpret_cast<char*>(&len), 4, 0);
    // if(iSendResult == SOCKET_ERROR) return false;
    int iSendResult = send(target, data.raw_ptr(), 8, 0);
    if(iSendResult == SOCKET_ERROR) return false;
    iSendResult = send(target, data.pointer_to_slice<0,256>(), 256, 0);
    if(iSendResult == SOCKET_ERROR) return false;
    return true;
  }
  bool Send(SOCKET target, const Key& key, const Value& value) {
    int iSendResult = send(target, key.raw_ptr(), 8, 0);
    if(iSendResult == SOCKET_ERROR) return false;
    iSendResult = send(target, value.pointer_to_slice<0,256>(), 256, 0);
    if(iSendResult == SOCKET_ERROR) return false;
    return true;
  }
};

} // namespace portal_db

#endif // PORTAL_NETWORK_SERVER_IMPL_H_