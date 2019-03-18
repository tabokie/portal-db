#ifndef COPLUS_PROTOCOL_H_
#define COPLUS_PROTOCOL_H_

#include "coplus/util.h"
#include "coplus/colog.h"

#include <string>
#include <vector>
#include <istream>
#include <ostream>
#include <iostream>

#ifndef max
#define max(a,b)		((a)>(b) ? (a) : (b))
#endif

struct Protocol {
	// head(4) | type(1) | len(4) | raw_type(1)(optional) | sender(::x:y:z:zz:p::) | msg(str)(optional) | tail(4)
	static constexpr size_t app_head_len = 4;
	static constexpr unsigned int app_head = 0xDEADC0DE; // may have endian issue
	static constexpr size_t app_terminator_len = 4;
	static constexpr unsigned int app_terminator = 0xDEADBEEF;
	static constexpr size_t minimum_frame = 4+4; // only terminator
	static constexpr size_t minimum_len = 1+4; // without terminator
	enum MessageType {
		kDefault = 0,
		kRequest = 1,
		kDirect = 2,
		kResponse = 3
	};
	static std::string pickle_message(MessageType type, std::string request, char raw_type = 't') {
		std::string ret = std::string(app_head_len, ' ') + (char)type + std::string(4, ' ') + raw_type + request + std::string(app_terminator_len, ' ');
		memcpy((void*)ret.c_str(), (void*)&app_head, app_head_len);
		int length = ret.length() - minimum_frame;
		memcpy((void*)(ret.c_str() + 5), (void*)&length, 4);
		memcpy((void*)(ret.c_str() + ret.size() - app_terminator_len), (void*)&app_terminator, app_terminator_len);
		return ret;
	}
	static std::string pickle_message(MessageType type, std::string addr, std::string request) {
		return pickle_message(type, std::string("::") + addr + "::" + request, 'd');
	}
	static std::string pickle_message(MessageType type, std::vector<std::string>& header, std::vector<std::string>& entries) {
		if(header.size() == 0) return "";
		std::string ret = header[0];
		for(int i = 1; i < header.size(); i ++) {
			ret += "," + header[i];
		}
		ret += ";";
		for(int i = 0; i + header.size() <= entries.size(); ) {
			ret += entries[i];
			int tmp = i;
			for(i++; i < tmp + header.size() && i < entries.size(); i ++) {
				ret += "," + entries[i];
			}
			ret += ";";
		}
		return pickle_message(type, ret, 'l');
	}
	enum GetType {
		kGetNothing = 0,
		kGetType = 1,
		kGetContent = 2,
		kGetLength = 4
	} ;
	static std::string format_message(std::ostream& os, std::string& completeStr, GetType get = kGetNothing) {
		std::string ret = "";
		if(completeStr.size() < minimum_len) {
			os << "broken package: " << completeStr << std::endl;
		}
		// type
		char type = completeStr[0];
		os << "type: ";
		switch(type) {
			case 0: os << "default"; if(get & kGetType) ret="default"; break;
			case 1: os << "request"; if(get & kGetType) ret="request"; break;
			case 2: os << "direct"; if(get & kGetType) ret="direct"; break;
			case 3: os << "response"; if(get & kGetType) ret="response"; break;
		}
		os << std::endl;
		// length
		os << "length: ";
		const int* lenPtr = (const int*)(completeStr.c_str() + 1);
		os << (*lenPtr) << std::endl;
		if(get & kGetLength) ret = std::to_string(*lenPtr);
		// content
		if(completeStr.size() == minimum_len) {
			os << "no message" << std::endl;
		} else if(completeStr[minimum_len] == 'l'){ // list
			os << "list: " << std::endl;
			std::vector<std::string> entries;
			StringUtil::split(completeStr.substr(minimum_len + 1), ";", entries);
			std::vector<int> minLayout;
			// parse header
			std::vector<std::string> header;
			StringUtil::split(entries[0], ",", header);
			if(header.size() == 0) {os<<"[null]"<<std::endl; return "";}
			for(int i = 0; i < header.size(); i++) minLayout.push_back(header[i].size());
			// parse entry
			std::vector<std::string> content;
			for(int i = 1; i < entries.size(); i++) {
				int cnt = StringUtil::split(entries[i], ",", content);
				if(cnt != header.size()) {os<<"[broken]"<<std::endl; return "";}
				for(int j = 0; j < cnt; j++)
					minLayout[j] = max(minLayout[j], content[header.size() * (i-1) + j].size());
			}
			for(int i = 0; i < minLayout.size(); i++) minLayout[i] += 2; // breath
			// print
			for(int i = 0; i < header.size(); i++) 
				os << "|" 
					<< std::string((minLayout[i]-header[i].size()) / 2.0, ' ') 
					<< header[i] 
					<< std::string((minLayout[i]-header[i].size()) / 2.0 + 0.9, ' ');
			os << "|" << std::endl;
			for(int i = 0; i + header.size() <= content.size(); ) {
				int tmp = i;
				for(i; i < tmp + header.size() && i < content.size(); i ++) {
					os << "|" 
						<< std::string((minLayout[i%header.size()]-content[i].size()) / 2.0, ' ') 
						<< content[i]
						<< std::string((minLayout[i%header.size()]-content[i].size()) / 2.0 + 0.9, ' ');
				}
				os << "|" << std::endl;
			}
		} else if(completeStr[minimum_len] == 'd'){
			int addr_end = completeStr.find("::", minimum_len + 3);
			if(addr_end == std::string::npos) os << "sender broke" << std::endl;
			else os << "sender: " << completeStr.substr(minimum_len + 3, addr_end - minimum_len - 3) << std::endl;
			os << "message: ";
			os << (completeStr.c_str() + addr_end + 2) << std::endl;
		} else {
			os << "message: " ;
			os << (completeStr.c_str() + minimum_len + 1) << std::endl;
		}
		if(get & kGetContent && completeStr.size() > minimum_len) ret = completeStr.substr(minimum_len + 1);
		return ret;
	}
};

#endif // COPLUS_PROTOCOL_H_