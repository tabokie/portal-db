#include <gtest/gtest.h>

// #define PORTAL_DEBUG
#include "db/hash_trie.h"
#include "portal_db/piece.h"
#include "db/hash_trie_iterator.h"

#include <string>

using namespace portal_db;

TEST(HashTrieTest, BasicTest) {
  HashTrie store("test_hash_trie");
  size_t size = 10000;
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


TEST(HashTrieTest, DeleteTest) {
  HashTrie store("test_hash_trie");
  size_t size = 1000;
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
    EXPECT_TRUE(store.Delete(key).inspect());
    EXPECT_TRUE(store.Get(key, value).IsNotFound());
  }
}

TEST(HashTrieTest, PutThenRetrieve) {
  HashTrie store("test_hash_trie");
  size_t size = 100000;
  char buf[256];
  for(int i = 0; i < size; i++) {
    std::string tmp = std::to_string(i);
    tmp += std::string(8-tmp.size(), ' ');
    *(reinterpret_cast<int*>(buf)) = i;
    Key key(tmp.c_str());
    Value value(buf);
    EXPECT_TRUE(store.Put(key, value).inspect());
  }
  for(int i = 0; i < size; i++) {
    std::string tmp = std::to_string(i);
    tmp += std::string(8-tmp.size(), ' ');
    Key key(tmp.c_str());
    Value value(buf);
    EXPECT_TRUE(store.Get(key, value).inspect());
    EXPECT_EQ(*(reinterpret_cast<const int*>(value.pointer_to_slice<0,4>())), i);
  }
}

TEST(HashTrieTest, ScanTest) {
  HashTrie store("test_hash_trie");
  size_t size = 1000;
  char buf[256];
  for(int i = 0; i < size; i++) {
    std::string tmp = std::to_string(i);
    tmp += std::string(8-tmp.size(), ' ');
    *(reinterpret_cast<int*>(buf)) = i;
    Key key(tmp.c_str());
    Value value(buf);
    EXPECT_TRUE(store.Put(key, value).inspect());
  }
  // store.Dump();
  Key empty;
  HashTrieIterator iterator = HashTrieIterator(true);
  EXPECT_TRUE(store.Scan(empty, empty, iterator).inspect());
  int count = 0;
  std::string last = "";
  while(iterator.Next()) {
    std::string tmp = iterator.Peek().to_string();
    EXPECT_TRUE(last <= tmp);
    last = tmp;
    count ++;
  }
  EXPECT_EQ(count, size);
}