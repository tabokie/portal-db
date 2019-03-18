#ifndef COPLUS_UTIL_H_
#define COPLUS_UTIL_H_

class NoCopy{
 private:
 	NoCopy(const NoCopy& rhs) = delete;
 public:
 	NoCopy() = default;
 	virtual ~NoCopy() { }
};

class NoMove: public NoCopy{
 private:
 	NoMove(NoMove&& rhs) = delete;
 public:
 	NoMove() = default;
 	virtual ~NoMove() { }
};

#include <cassert>

#ifdef COPLUS_DEBUG_
#define COPLUS_ASSERT(x)		assert(x)
#else
#define COPLUS_ASSERT(x)		x
#endif

// compiler branch prediction //
#ifdef __GNUC__
#define likely(x)       __builtin_expect(!!(x), 1)
#define unlikely(x)     __builtin_expect(!!(x), 0)
#else
#define likely(x) 			x
#define unlikely(x)			x
#endif

#include <vector>
#include <iostream>
#include <string>

struct StringUtil {
	static int split(std::string& str, std::vector<std::string>& ret, int max_partition = -1) {
		int cur = 0;
		int to = 0;
		int count = ret.size();
		while( (max_partition <= 0 || ret.size() - count < max_partition - 1 ) && (cur = str.find_first_not_of(' ', cur)) != std::string::npos) {
			to = str.find_first_of(' ', cur); // find space
			if(to == std::string::npos) to = str.size();
			if(to > cur) {
				ret.push_back(str.substr(cur, to - cur));
			}
			cur = to;
		}
		if(cur < str.size()) ret.push_back(str.substr(cur)); // don;t split tail
		return ret.size() - count;
	}
	static int split(std::string& str, std::string token, std::vector<std::string>& ret) {
		int cur = 0;
		int to = 0;
		int count = ret.size();
		while((cur = str.find_first_not_of(token, cur)) != std::string::npos) {
			to = str.find_first_of(token, cur); // find space
			if(to == std::string::npos) to = str.size();
			if(to > cur) {
				ret.push_back(str.substr(cur, to - cur));
			}
			cur = to + 1;
		}
		return ret.size() - count;
	}
	static bool starts_with(std::string& a, std::string b) {
		if(a.size() < b.size()) return false;
		return a.compare(0, b.size(), b) == 0;
	}
	static bool ends_with(std::string& a, std::string b) {
		if(a.size() < b.size()) return false;
		return b.compare(0, b.size(), a.c_str() + a.size() - b.size(), b.size()) == 0;
	}

};

#endif // COPLUS_UTIL_H_