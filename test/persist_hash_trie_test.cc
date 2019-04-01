#include <gtest/gtest.h>

// #define PORTAL_DEBUG
#include "db/hash_trie.h"
#include "portal_db/piece.h"
#include "db/hash_trie_iterator.h"
#include "db/persist_hash_trie.h"
#include "util.h"

#include <string>

using namespace portal_db;

TEST(PersistHashTrieTest, BasicTest) {
  PersistHashTrie store("test_persist_hash_trie");
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


TEST(PersistHashTrieTest, DeleteTest) {
  PersistHashTrie store("test_persist_hash_trie");
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

TEST(PersistHashTrieTest, PutThenRetrieve) {
  PersistHashTrie store("test_persist_hash_trie");
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

TEST(PersistHashTrieTest, ScanTest) {
  PersistHashTrie store("test_persist_hash_trie");
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

TEST(PersistHashTrieBenchmark, PutGetScan) {
  PersistHashTrie store("test_persist_hash_trie");
  size_t size = 100'0000;
  char buf[256];
  Value value(buf);
  timer.start();
  for(int i = 0; i < size; i++) {
    std::string tmp = std::to_string(i);
    tmp += std::string(8-tmp.size(), ' ');
    Key key(tmp.c_str());
    EXPECT_TRUE(store.Put(key, value).inspect());
  }
  std::cout << timer.end() << std::endl;

  timer.start();
  for(int i = 0; i < size; i++) {
    std::string tmp = std::to_string(i);
    tmp += std::string(8-tmp.size(), ' ');
    Key key(tmp.c_str());
    EXPECT_TRUE(store.Get(key, value).inspect());
  }
  std::cout << timer.end() << std::endl;

  Key empty;
  HashTrieIterator iterator1 = HashTrieIterator(true);
  timer.start();
  EXPECT_TRUE(store.Scan(empty, empty, iterator1).inspect());
  while(iterator1.Next()) { }
  std::cout << timer.end() << std::endl;

  HashTrieIterator iterator2 = HashTrieIterator(false);
  timer.start();
  EXPECT_TRUE(store.Scan(empty, empty, iterator2).inspect());
  while(iterator2.Next()) { }
  std::cout << timer.end() << std::endl;

  timer.start();
  for(int i = 0; i < size; i++) {
    std::string tmp = std::to_string(i);
    tmp += std::string(8-tmp.size(), ' ');
    Key key(tmp.c_str());
    EXPECT_TRUE(store.Delete(key).inspect());
  }
  std::cout << timer.end() << std::endl;
}

TEST(PersistHashTrieBenchmark, Recovery) {
  PersistHashTrie* pstore = new PersistHashTrie("test_persist_hash_trie");
  PersistHashTrie& store = *pstore;
  size_t size = 100'0000;
  char buf[256];
  Value value(buf);
  timer.start();
  for(int i = 0; i < size; i++) {
    std::string tmp = std::to_string(i);
    tmp += std::string(8-tmp.size(), ' ');
    Key key(tmp.c_str());
    EXPECT_TRUE(store.Put(key, value).inspect());
  }
  std::cout << timer.end() << std::endl;
  delete pstore;

  PersistHashTrie new_store("test_persist_hash_trie");

  timer.start();
  EXPECT_TRUE(new_store.RecoverSnapshot().inspect());
  std::cout << timer.end() << std::endl;

  timer.start();
  EXPECT_TRUE(new_store.RecoverBinLog().inspect());
  std::cout << timer.end() << std::endl;

}