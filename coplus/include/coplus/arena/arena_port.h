#ifndef COPLUS_ARENA_ARENA_PORT_H_
#define COPLUS_ARENA_ARENA_PORT_H_

#include <cstdlib>
#include <cstdio>
#include <iostream>

#include "coplus/port.h"

namespace coplus {
namespace arena {

#if defined(__WIN32) || defined(__WIN64) || defined(_MSC_VER)

char* GetReserved(size_t size){
	LPVOID ret = VirtualAlloc(NULL, size, MEM_RESERVE | MEM_TOP_DOWN, PAGE_READWRITE);
	if(ret == NULL)std::cout << "Last error: " << GetLastError() << std::endl;
	if(ret == NULL)return nullptr;
	return static_cast<char*>(ret);
}
bool Commit(char* ptr, size_t size){
	LPVOID ret = VirtualAlloc(ptr, size, MEM_COMMIT, PAGE_READWRITE);
	if(ret == NULL)return false;
	return true;
}
bool Decommit(char* ptr, size_t size){
	return true;
	bool ret = VirtualFree(ptr, size, MEM_DECOMMIT);
	return ret;
}
bool FreeHeap(char* ptr, size_t size){
	bool ret = VirtualFree(ptr, size, MEM_RELEASE);
	return ret;
}

#elif defined(__linux)

char* GetReserved(size_t size){
	return mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_ANONYMOUS, -1, 0);
}

bool Commit(char* ptr, size_t size){
	return true;
}

bool Decommit(char* ptr, size_t size){
	return true;
}

bool FreeHeap(char* ptr, size_t size){
	int ret = munmap(ptr, size);
	if(ret == 0)return true;
	return false;
}


#endif

} // namespace arena
} // namespace coplus

#endif // COPLUS_ARENA_ARENA_PORT_H_