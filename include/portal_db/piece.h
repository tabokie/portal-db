#ifndef PORTAL_PIECE_H_
#define PORTAL_PIECE_H_

#include <cstdint>
#include <string>
#include <cassert>
#include <iostream>

#include "port.h"

namespace portal_db {

class Value {
 public:
  Value(): value_(NULL) { }
  Value(const char* data)
    : value_(NULL) { 
      value_ = (data == NULL) ? NULL : CopyData(data, 256); }
  Value(const char* data, size_t len)
    : value_(NULL) { 
      value_ = (data == NULL || len == 0) ? NULL : CopyData(data, len); }
  Value(const Value& rhs) { 
    value_ = (rhs.value_ == NULL) ? NULL : CopyData(rhs.value_, 256); }
  Value(Value&& rhs): value_(rhs.value_) { rhs.value_ = NULL; }
  Value& operator=(Value&& rhs) {
    value_ = rhs.value_;
    rhs.value_ = NULL;
    return *this;
  }
  Value& operator=(const Value& rhs) {
    if(rhs.value_ == NULL) {
      if(value_ != NULL ) delete[] value_;
      value_ = NULL;
    } else {
      if(value_ == NULL) value_ = CopyData(rhs.value_, 256);
      else memcpy(value_, rhs.value_, 256);      
    }
    return *this;
  }
  virtual ~Value() { delete[] value_; }
  void set_empty() { delete[] value_; value_ = NULL; }
  bool empty() const { return value_ == NULL; }
  template <size_t offset, size_t length>
  void copy(const char* p) {
    static_assert(offset + length <= 256, "access to value slice overflow");
    if(value_ == NULL) value_ = new char[256];
    assert(value_ != NULL);
    memcpy(value_ + offset, p, length);
  }
  void copy(size_t offset, size_t length, const char* p) {
    if(value_ == NULL) value_ = new char[256];
    assert(value_ != NULL);
    assert(offset + length <= 256);
    memcpy(value_ + offset, p, length);
  }
  template <size_t offset, size_t length>
  void write(char* p) const {
    static_assert(offset + length <= 256, "access to value slice overflow");
    assert(value_ != NULL);
    memcpy(p, value_ + offset, length);
  }
  // unsafe, copy right away
  template <size_t offset, size_t length = 1>
  const char* pointer_to_slice() const {
    static_assert(offset + length <= 256, "access to value slice overflow");
    assert(value_ != NULL);
    return value_ + offset;
  }
  static Value from_string(std::string a) {
    Value ret;
    ret.value_ = new char[256];
    memset(ret.value_, 0, 256);
    memcpy(ret.value_, a.c_str(), min(a.size(), 256));
    return std::move(ret);
  }
 protected:
  char* value_;
  static char* CopyData(const char* rhs, size_t len) {
    char* ret = new char[256];
    memcpy(ret, rhs, sizeof(char) * len);
    return ret;
  }
};

class CharwiseAccess {
 public:
  virtual ~CharwiseAccess() { }
  virtual char operator[](size_t) const = 0;
  virtual char& operator[](size_t) = 0;
};

// Any attempt to reference inner data
// will cause a forceful initialize
// Best practice is to use `empty`
// before access
class Key : public CharwiseAccess {
 public:
  Key() = default;
  Key(const char* data) {
    key_ = (data == NULL) ? NULL : CopyData(data, 8);
  }
  Key(const Key& rhs) {
    key_ = (rhs.key_ == NULL) ? NULL : CopyData(rhs.key_, 8);
  }
  Key(Key&& rhs): key_(rhs.key_) {
    rhs.key_ = NULL;
  }
  Key& operator=(Key&& rhs) {
    key_ = rhs.key_;
    rhs.key_ = NULL;
    return *this;
  }
  Key& operator=(const Key& rhs) {
    if(rhs.key_ == NULL) {
      if(key_ != NULL ) delete[] key_;
      key_ = NULL;
    } else {
      if(key_ == NULL) key_ = CopyData(rhs.key_, 8);
      else memcpy(key_, rhs.key_, 8);      
    }
    return *this;
  }
  virtual ~Key() { delete[] key_; }
  bool empty() const { return key_ == NULL; }
  char operator[](size_t idx) const {
    if(!key_) return 0;
    return ((char*)key_)[idx];
  }
  char& operator[](size_t idx) {
    init();
    return ((char*)key_)[idx];
  }
  // unsafe for potential NULL
  const char* raw_ptr() const {
    assert(key_ != NULL);
    return reinterpret_cast<const char*>(key_);
  }
  static Key from_string(std::string a) {
    Key ret;
    ret.init();
    memset(ret.key_, 0, 8);
    memcpy(ret.key_, a.c_str(), min(a.size(), 8) );
    return std::move(ret);
  }
  std::string to_string() const {
    if(!key_) return std::string(8, '\0');
    return std::string((char*)key_, 8);
  }
 #ifdef LITTLE_ENDIAN
  bool operator<(const char* p) const {
    if(!p) return true; // empty as +inf
    if(!key_) return false;
    for(int i = 0; i < 8; i++) 
      if(key_[i] > p[i]) return false; 
      else if(key_[i] < p[i]) return true;
    return false; // eq
  }
  bool operator<(const Key& rhs) const {
    if(!rhs.key_) return true; // empty as +inf
    if(!key_) return false;
    return (*this) < rhs.key_;
  }
  bool operator==(const char* p) const {
    if(!key_ && !p) return true;
    if(!key_ || !p) return false;
    for(int i = 0; i < 8; i++) 
      if(key_[i] != p[i]) return false; 
    return true; // eq
  }
  bool operator==(const Key& rhs) const {
    if(!key_ && !rhs.key_) return true;
    if(!key_ || !rhs.key_) return false;
    return (*this) == rhs.key_;
  }
  bool operator<=(const char* p) const {
    if(!p) return true; // empty as +inf
    if(!key_) return false;
    for(int i = 0; i < 8; i++) 
      if(key_[i] > p[i]) return false; 
      else if(key_[i] < p[i]) return true;
    return true; // eq
  }
  bool operator<=(const Key& rhs) const {
    if(!rhs.key_) return true; // empty as +inf
    if(!key_) return false;
    return (*this) <= rhs.key_;
  }
 protected:
  char *key_ = NULL;
  void init() {
    if(!key_)key_ = new char[8];
  }
  static char* CopyData(const char* p, size_t len) {
    char* ret = new char[8];
    memcpy(ret, p, sizeof(char) * len);
    return ret;
  }
 #else
  bool operator<(const Key& rhs) const {
    if(!rhs.key_) return true; // empty as +inf
    if(!key_) return false;
    return key_[0] < rhs.key_[0] ||
      key_[0] == rhs.key_[0] && key_[1] < rhs.key_[1];
  }
  bool operator==(const Key& rhs) const {
    if(!rhs.key_ && !key_) return true;
    if(!key_ || !rhs.key_) return false;
    return key_[0] == rhs.key_[0] &&
      key_[1] == rhs.key_[1];
  }
  bool operator<=(const Key& rhs) const {
    if(!rhs.key_) return true; // empty as +inf
    if(!key_) return false;
    return key_[0] < rhs.key_[0] ||
      key_[0] == rhs.key_[0] && key_[1] <= rhs.key_[1];
  }
 protected:
  uint32_t *key_ = NULL;
  void init() {
    if(!key_)key_ = new uint32_t[2];
  }
  static uint32_t* CopyData(const char* p, size_t len) {
    uint32_t* ret = new uint32_t[2];
    memcpy(ret, p, sizeof(char) * len);
    return ret;
  }
 #endif
};

class KeyValue: public Key, public Value {
 public:
  KeyValue() { }
  KeyValue(const char* k): Key(k) { }
  KeyValue(const std::string& k): Key(k.c_str()) { }
  KeyValue(const char* k, const char* v, size_t len = 256)
    : Key(k), Value(v,len) { }
  KeyValue(const std::string& k, const char* v, size_t len = 256)
    : KeyValue(k.c_str(), v, len) { }
  KeyValue(const std::string& k, const std::string& v)
    : KeyValue(k.c_str(), v.c_str()) { }
  KeyValue(const KeyValue& rhs): Key(rhs), Value(rhs) { }
  KeyValue(KeyValue&& rhs): Key(rhs), Value(rhs) { }
  KeyValue& operator=(KeyValue&& rhs) {
    Key::operator=(std::move(rhs));
    Value::operator=(std::move(rhs));
    return *this;
  }
  static KeyValue from_string(std::string a, std::string b) {
    KeyValue ret;
    memset(ret.key_, 0, 8);
    memcpy(ret.key_, a.c_str(), min(a.size(), 8) );
    ret.value_ = new char[256];
    memset(ret.value_, 0, 256);
    memcpy(ret.value_, b.c_str(), min(b.size(), 256));
    return std::move(ret);
  }
  ~KeyValue() { }
};


} // namespace portal_db

#endif // PORTAL_PIECE_H_