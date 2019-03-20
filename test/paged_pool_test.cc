#include <gtest/gtest.h>

#include "db/paged_pool.h"

#include <cstring>
#include <iostream>

using namespace portal_db_impl;

TEST(PagedPoolTest, RAII) {
	PagedPool<256> pool;
	for(int i = 0; i < pool.capacity() / 4; i++) {
		char* tmp = pool.New();
		memset(tmp, 0, sizeof(char) * 256);
		EXPECT_EQ(pool.size(), i + 1);
	}
}

TEST(PagedPoolTest, Retrieve) {
	PagedPool<32> pool;
	for(size_t i = 0; i < pool.capacity() / 4; i++) {
		char* tmp = pool.New();
		memcpy(tmp, reinterpret_cast<char*>(&i), sizeof(char) * 4);
	}
	for(size_t i = 0; i < pool.capacity() / 4; i++) {
		char* tmp = pool.Get(i);
		size_t retrieve = *reinterpret_cast<size_t*>(tmp);
		EXPECT_EQ(retrieve, i);
	}
}