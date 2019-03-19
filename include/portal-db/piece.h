#ifndef PORTAL_PIECE_H_
#define PORTAL_PIECE_H_

#include <cstdint>
#include <string>
#include <cassert>
#include <iostream>

#include "util/port.h"

namespace portal_db {

class Value {
 public:
	Value(): data_(NULL) { }
	Value(const char* data)
		: data_(NULL) { data_ = (data == NULL) ? NULL : CopyData(data, 256); }
	Value(const char* data, size_t len)
		: data_(NULL) { data_ = (data == NULL || len == 0) ? NULL : CopyData(data, len); }
	Value(const Value& rhs) { data_ = (rhs.data_ == NULL) ? NULL : CopyData(rhs.data_, 256); }
	virtual ~Value() { delete[] data_; }
	bool empty() const { return data_ == NULL; }
	template <size_t offset, size_t length>
	bool copy(const char* p) {
		static_assert(offset + length <= 256);
		// if(data_ == NULL) data_ = new char[256];
		assert(data_ != NULL);
		memcpy(data_ + offset, p, length);
	}
	// unsafe, copy right away
	template <size_t offset, size_t length>
	const char* pointer_to_slice() const {
		static_assert(offset + length <= 256);
		assert(data_ != NULL);
		return data_ + offset;
	}
 private:
 	char* data_;
 	static char* CopyData(const char* rhs, size_t len) {
		char* ret = new char[256];
		memcpy(ret, rhs, sizeof(char) * len);
		return ret;
	}
};

class Key {
 public:
 	Key() = default;
 	Key(const char* data) {
 		if(data != NULL) memcpy((void*) data_, data, sizeof(char) * 8);
 	}
	Key(const Key& rhs) {
		memcpy((void*)data_, (void*)rhs.data_, sizeof(char) * 8);
	}
	virtual ~Key() { }
	char operator[](size_t idx) const {
		return ((char*)data_)[idx];
	}
	char& operator[](size_t idx) {
		return ((char*)data_)[idx];
	}
#ifdef LITTLE_ENDIAN
	bool operator<(const Key& rhs) const {
		for(int i = 0; i < 8; i++) if(data_[i] > rhs.data_[i]) return false; else if(data_[i] < rhs.data_[i]) return true;
		return false; // eq
	}
	bool operator==(const Key& rhs) const {
		for(int i = 0; i < 8; i++) if(data_[i] != rhs.data_[i]) return false; 
		return true; // eq
	}
	bool operator<=(const Key& rhs) const {
		for(int i = 0; i < 8; i++) if(data_[i] > rhs.data_[i]) return false; else if(data_[i] < rhs.data_[i]) return true;
		return true; // eq
	}
	std::string to_string() const {
		return std::string((char*)data_, 8);
	}
 private:
 	char data_[8];
#else
	bool operator<(const Key& rhs) const {
		return data_[0] < rhs.data_[0] ||
			data_[0] == rhs.data_[0] && data_[1] < rhs.data_[1];
	}
	bool operator==(const Key& rhs) const {
		return data_[0] == rhs.data_[0] &&
			data_[1] == rhs.data_[1];
	}
	bool operator<=(const Key& rhs) const {
		return data_[0] < rhs.data_[0] ||
			data_[0] == rhs.data_[0] && data_[1] <= rhs.data_[1];
	}
	std::string to_string() const {
		return std::string((char*)data_, 8);
	}
 private:
 	uint32_t data_[2];
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
 	~KeyValue() { }
};


} // namespace portal_db

#endif // PORTAL_PIECE_H_