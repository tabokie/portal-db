#include "client_impl.h"

namespace portal_db {

Status ClientImpl::Connect(std::string ip, std::string port, std::string name) {
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
Status ClientImpl::Close() {
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
Status ClientImpl::Get(const Key& key, KeyValue& ret) {
  if(socket_ == INVALID_SOCKET)
    return Status::Corruption("invalid socket");
  Status status = Send(std::string("GET") + key.to_string());
  if(!status.ok()) return status;
  status *= Receive(ret);
  return status;
}
Status ClientImpl::Put(const Key& key, const Value& value) {
  if(socket_ == INVALID_SOCKET)
    return Status::Corruption("invalid socket");
  Status status = Send(std::string("PUT") + key.to_string());
  if(!status.ok()) return status;
  status *= Send(value.pointer_to_slice<0,256>(), 256);
  if(!status.ok()) return status;
  status *= Receive();
  return status;
}
Status ClientImpl::Delete(const Key& key) {
  if(socket_ == INVALID_SOCKET)
    return Status::Corruption("invalid socket");
  return Status::OK();
}
Status ClientImpl::StartScan(const Key& lower, const Key& upper, bool sort) {
  if(socket_ == INVALID_SOCKET)
    return Status::Corruption("invalid socket");
  Status status = Send(std::string("SCN") + lower.to_string() + upper.to_string());
  if(!status.ok()) return status;
  status *= Receive();
  return status;
}
Status ClientImpl::FetchScan(std::vector<KeyValue>& ret) {
  if(socket_ == INVALID_SOCKET)
    return Status::Corruption("invalid socket");
  Status status = Send(std::string("MOR"));
  if(!status.ok()) return status;
  status *= Receive(ret);
  return status;
}

Status ClientImpl::Send(const char* p, size_t len) {
  if(socket_ == INVALID_SOCKET) return Status::Corruption("invalid socket");
  int iResult = send(socket_, p, len, 0);
  if(iResult == SOCKET_ERROR) {
    return Status::IOError(
      std::to_string(WSAGetLastError()) + 
      " - error sending message"
      );
  }
  return Status::OK();
}

Status ClientImpl::Receive() {
  if(socket_ == INVALID_SOCKET) 
    return Status::Corruption("invalid socket");
  int len = buffer_len_;
  while(cur < len) {
    int iResult = recv(socket_, buffer + cur, buffer_len_ - cur, 0);
    if(iResult == 0) {
      closesocket(socket_);
      socket_ = INVALID_SOCKET;
      return Status::IOError("socket already closed");
    }
    else if(iResult < 0){
      closesocket(socket_);
      socket_ = INVALID_SOCKET;
      return Status::IOError(
        std::to_string(WSAGetLastError()) + 
        " - error trying to recv"
        );
    }
    cur += iResult;
    if(cur >= 4 && *(reinterpret_cast<uint32_t*>(buffer)) != 0)
      return Status::IOError("no expected header");
    if(cur >= 8)
      len = *(reinterpret_cast<uint32_t*>(buffer+4)) + 8;
  }
  cur -= len;
  if(buffer[8] == 'O' && buffer[9] == 'K') return Status::OK();
  return Status::IOError(std::string(buffer + 8, len-8));
}
Status ClientImpl::Receive(KeyValue& ret) {
  if(socket_ == INVALID_SOCKET) 
    return Status::Corruption("invalid socket");
  while(cur < 256 + 8) {
    if(cur >= 4 && *(reinterpret_cast<uint32_t*>(buffer)) == 0) {
      return Status::IOError(std::string(buffer + 4, cur-4));
    }
    int iResult = recv(socket_, buffer + cur, buffer_len_ - cur, 0);
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
    cur += iResult;
  }
  cur -= 264;
  for(int i =- 0; i < 8; i++) ret[i] = buffer[i];
  ret.copy<0,256>(buffer + 8);
  return Status::OK();
}
Status ClientImpl::Receive(std::vector<KeyValue>& ret) {
  if(socket_ == INVALID_SOCKET) 
    return Status::Corruption("invalid socket");
  int len = buffer_len_;
  uint32_t size = 0x0fffffff;
  while(cur < len && size > 0) {
    int iResult = recv(socket_, buffer + cur, buffer_len_ - cur, 0);
    if(iResult == 0) {
      closesocket(socket_);
      socket_ = INVALID_SOCKET;
      return Status::IOError("socket already closed");
    }
    else if(iResult < 0){
      closesocket(socket_);
      socket_ = INVALID_SOCKET;
      return Status::IOError(
        std::to_string(WSAGetLastError()) + 
        " - error trying to recv"
        );
    }
    cur += iResult;
    if(cur >= 4 && size == 0x0fffffff) {
      size = *(reinterpret_cast<uint32_t*>(buffer));
      if(size == 0)
        return Status::IOError("no more");
      cur -= 4;
    }
    if(cur >= 264) {
      ret.push_back(KeyValue(buffer, buffer + 8));
      cur -= 264;
      size --;
    }
  }
  return Status::OK();
}

} // namespace portal_db