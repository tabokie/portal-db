#ifndef PORTAL_PIECE_H_
#define PORTAL_PIECE_H_

#include <string>

namespace portal_db {

template <size_t size = 256>
class Piece {
	Piece(): data_(NULL) { }
	Piece(const char* data)
		: data_(NULL) { data_ = (data == NULL) ? NULL : CopyData(data); }
	Piece(const std::string& data)
		: data_(NULL) { data_ = (data.size() < size) ? NULL : CopyData(data.c_str()); }
	Piece(const Piece& rhs) { data_ = (rhs.data_ == NULL) ? NULL : CopyData(rhs.data_); }
	~Piece() { delete[] data_; }
	template <size_t offset, size_t length>
	const char* pointer_to_slice() {
		static_assert(offset + length <= size);
		return data_ + offset;
	}
 private:
 	const char* data_;

 	static const char* CopyData(const char* rhs) {
		char* ret = new char[size];
		memcpy(ret, rhs, sizeof(char) * size);
		return ret;
	}
};

using Key = Piece<8>;
using Value = Piece<256>;
using KeyValue = Piece<8 + 256>;
using KeyRange = Piece<8+8>;

} // namespace portal_db

// PORTAL_PIECE_H_