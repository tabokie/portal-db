#include <gtest/gtest.h>

#include "db/hash_trie.h"
#include "portal-db/piece.h"

#include <string>

using namespace portal_db;

TEST(HashTrieTest, BasicTest) {
	HashTrie store;
	size_t size = 100;
	char buf[256];
	for(int i = 0; i < size; i++) {
		std::string tmp = std::to_string(i);
		tmp += std::string(8-tmp.size(), ' ');
		*(reinterpret_cast<int*>(buf)) = i;
		Key key(tmp.c_str());
		Value value(buf);
		EXPECT_TRUE(store.Put(key, value).inspect());
		EXPECT_TRUE(store.Get(key, value).inspect());
		EXPECT_EQ(*(reinterpret_cast<const int*>(value.pointer_to_slice<0,4>())), i);
	}
}