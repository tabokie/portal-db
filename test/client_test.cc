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
  c.get()->Connect("127.0.0.1", "693", "test_client");
  size_t size = 10;
  char buf[256];
  memset(buf, 0, sizeof(char) * 256);
  for(int i = 0; i < size; i++) {
    std::string tmp = std::to_string(i);
    tmp += std::string(8-tmp.size(), '\0');
    sprintf(buf, "sqrt(%d) is stored here" , i * i);
    Key key(tmp.c_str());
    Value value(buf);
    c.get()->Put(key, value);
  }
  for(int i = size - 1; i >= 0; i--) {
    memset(buf, 0, 256);
    std::string tmp = std::to_string(i);
    tmp += std::string(8-tmp.size(), '\0');
    KeyValue kv = KeyValue::from_string(tmp, "");
    c.get()->Get(kv, kv).inspect();
    print(std::cout, kv);
    std::cout << std::endl;
    // std::cout << "result of `Get " << i << "`: "
      // << std::string(buf) << std::endl; 
  }
  Key empty;
  std::vector<KeyValue> ret;
  c.get()->StartScan(empty, empty, true);
  std::cout << "start fetch records:" << std::endl;
  while(c.get()->FetchScan(ret).inspect()) {
    for(auto& v: ret) {
      print(std::cout, v);
      std::cout << std::endl;
    }
    ret.clear();
  }
  return 0;
}