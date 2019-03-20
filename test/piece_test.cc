#include <gtest/gtest.h>

#include "portal-db/piece.h"
#include "util.h"

#include <vector>
#include <string>
#include <iostream>

using namespace portal_db;

test::Random rnd;

TEST(KeyTest, RAII) {
	size_t size = 10000;
	std::vector<Key> buffer;
	while(size --) {
		Key key( std::string(8, ((size & 127) + 1) + '\0').c_str() );
		buffer.push_back(key);
	}
	buffer.clear();
}

TEST(KeyTest, Comparable) {
	size_t size = 100;
	while(size--) {
		std::string a = rnd.NumericString(8);
		std::string b = rnd.NumericString(8);
		ASSERT_EQ(a.size(), 8);
		ASSERT_EQ(b.size(), 8);
		Key keya(a.c_str());
		Key keyb(b.c_str());
		EXPECT_EQ(a < b, keya < keyb);
		EXPECT_EQ(a <= b, keya <= keyb);
		EXPECT_EQ(a == b, keya == keyb);
	}
}

TEST(KeyTest, CharwiseAccess) {
	size_t size = 100;
	while(size--) {
		std::string s = rnd.NumericString(8);
		Key key(s.c_str());
		for(int i = 0; i < 8; i++) {
			EXPECT_EQ(s[i], key[i]);
		}
	}
	size = 100;
	while(size--) {
		Key key;
		std::string s(8, '0');
		for(int i = 0; i < 8; i ++) {
			char c = '0' + rnd.UInt(10);
			key[i] = c;
			s[i] = c;
		}
		EXPECT_EQ(key.to_string(), s);
	}
}