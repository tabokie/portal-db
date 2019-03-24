#include "portal_db/client.h"

#include <memory>
#include <ostream>
#include <iostream>

using namespace portal_db;

void print(std::ostream& os, const KeyValue& kv) {
  os << "["; 
  for(int i = 0; i < 8 && kv[i] != '\0'; i++) {
    os << kv[i];
  }
  os << "] ";
  os << kv.pointer_to_slice<0,256>();
}

int main(void) {
  std::unique_ptr<Client> c = Client::NewClient();
  std::cout << "`Connect`:" 
    << c.get()->Connect("127.0.0.1", "693", "test_client").ToString() 
    << std::endl;
  size_t size = 10;
  char buf[256];
  memset(buf, 0, sizeof(char) * 256);
  for(int i = 0; i < size; i++) {
    std::string tmp = std::to_string(i);
    tmp += std::string(8-tmp.size(), '\0');
    memset(buf, 0, 256);
    sprintf(buf, "sqrt(%d) is stored here" , i * i);
    Key key(tmp.c_str());
    Value value(buf);
    std::cout << "`PUT`:" 
      << c.get()->Put(key, value).ToString() << std::endl;
  }
  for(int i = size - 1; i >= 0; i--) {
    memset(buf, 0, 256);
    std::string tmp = std::to_string(i);
    tmp += std::string(8-tmp.size(), '\0');
    KeyValue kv = KeyValue::from_string(tmp, "");
    std::cout << "`GET`:" 
      << c.get()->Get(kv, kv).ToString() << std::endl;
    print(std::cout, kv);
    std::cout << std::endl;
  }
  Key empty;
  std::vector<KeyValue> ret;
  std::cout << "`StartScan`:" 
    << c.get()->StartScan(empty, empty, true).ToString() 
    << std::endl;
  Status status;
  while((status *= c.get()->FetchScan(ret)).inspect()) {
    for(auto& v: ret) {
      print(std::cout, v);
      std::cout << std::endl;
    }
    ret.clear();
  }
  return 0;
}