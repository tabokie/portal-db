#include <gtest/gtest.h>

#include "util/concurrent_vector.h"

#include <memory>

using namespace portal_db;

TEST(ConcurrentVectorTest, SingleThreadTest) {
	ConcurrentVector<int> vec;
	size_t size = 10000;
	for(int i = 0; i < size; i++) {
		EXPECT_EQ(vec.push_back(std::make_unique<int>(i)), i);
		EXPECT_EQ(vec.size(), i + 1);
	}
	for(int i = 0; i < size; i++) {
		EXPECT_EQ(i, *vec[i]);
	}
	for(int i = 0; i < size; i++) {
		std::unique_ptr<int> p = std::move(vec.own(i));
		EXPECT_EQ(*p.get(), i);
		EXPECT_EQ(NULL, vec[i]);
	}
}