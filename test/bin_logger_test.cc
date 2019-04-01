#include <gtest/gtest.h>

#include "db/bin_logger.h"
#include "util.h"
#include "portal_db/status.h"
#include "portal_db/piece.h"

#include <cstring>
#include <string>

using namespace portal_db;

TEST(BinLoggerTest, Write) {
  BinLogger logger("unique.bin");
  size_t size = 10000;
  char buffer[256];
  memset(buffer, 'x', sizeof(char) * 256);
  Value value(buffer);
  for(int i = 0; i < size; i++) {
    Key key(rnd.NumericString(8).c_str());
    if(rnd.UInt(10) > 8) EXPECT_TRUE(logger.AppendDelete(key).inspect());
    else EXPECT_TRUE(logger.AppendPut(key, value).inspect());
  }
  logger.Checkpoint();
  EXPECT_TRUE(logger.Compact().inspect());
  EXPECT_TRUE(logger.Close().inspect());
  EXPECT_TRUE(logger.Delete().inspect());
}