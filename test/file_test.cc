#include <gtest/gtest.h>

#include "util/file.h"

using namespace portal_db;

TEST(FileTest, Test) {
	size_t size = 20000;
	char* test_data = new char[size];
	memset(test_data, 'x', size);

	SequentialFile file("unique_tmp");
	ASSERT_TRUE(file.Open().inspect());
	ASSERT_TRUE(file.SetEnd(size).inspect());
	ASSERT_TRUE(file.Write(0, size, test_data).inspect());
	ASSERT_TRUE(file.Close().inspect());
	ASSERT_TRUE(file.Open().inspect());
	ASSERT_TRUE(file.Read(0, size, test_data).inspect());
	for(int i = 0; i < size; i++) {
		EXPECT_EQ(test_data[i], 'x');
	}
	ASSERT_TRUE(file.Close().inspect());
	ASSERT_TRUE(file.Delete().inspect());
	delete [] test_data;
}