#pragma once

#include <functional> // hash

#include "coplus/util.h"

namespace coplus{

namespace { // anonymous np

// override implementation of hash
template<typename T>
struct _hash{ 
  size_t operator()(const T& key) const{
    return std::hash<T>{}(key);
  }
};
// Hash function for string
template<> struct _hash<std::string>{
  typedef size_t result_type;
  typedef std::string argument_type;
  size_t operator()(const std::string& str) const{
    const char* ptr = (str.c_str());
    unsigned int seed = 131;
    unsigned int hash = 0;
    size_t cur = 0;
    while(ptr[cur]){
      hash = hash * seed + (ptr[cur++]);
    }
    return hash;  
  }
};

}

// two level structure to speed up resize and optimize redundent write
// example:
// RL: [0,4][2,6]...
// L1:   [0,2|1,3]
// L2:    [0 | 1]

template <typename ElementType, size_t slot = 4>
class LeveledHashMap: public NoMove{
	template <typename ET, size_t slot>
	struct HashNode: public NoMove{
		ET data[slot];
		HashNode<ET, slot>* shared;
		HashNode(HashNode<ET,slot>* target): shared(target) { } 
		~HashNode() { }
	};
	template <typename ElementType, size_t slot>
	struct HashNodeBuffer{
		HashNodeBuffer(size_t size)
	};

	HashNodeBuffer L1, L2;
	

};

}