#ifndef PORTAL_DB_HASH_TRIE_H_
#define PORTAL_DB_HASH_TRIE_H_

#include "portal_db/piece.h"
#include "paged_pool.h"
#include "util/util.h"
#include "util/readwrite_lock.h"
#include "util/debug.h"
#include "hash_trie_iterator.h"
#include "util/concurrent_vector.h"
#include "util/atomic_lock.h"

#include <atomic>
#include <iostream>

namespace portal_db {

// + pointer
// |  + 0 --------- physically deleted
// |  + 0x0fff ---- linked-list tail
// |  + n --------- next index + 1
// + value
// |  + 0x0fff ---- not initialized
// |  + (0,prefix)- logically deleted
struct HashNode {
  std::atomic<int32_t> pointer;
  uint32_t value;
  HashNode(const HashNode& rhs)
    : pointer(rhs.pointer.load()),
      value(rhs.value) { }
  HashNode(): pointer(0), value(0x0fffffff) { }
  ~HashNode() { }
};


// + forward
// |  + +x ------ hash index + 1
// |  + -x ------ node index
// |  + 0x0fff -- null
template <size_t hashSize = 512>
struct HashTrieNode: public NoMove {
  using Holder = std::unique_ptr<HashTrieNode>;
  using UnsafeRef = HashTrieNode*;
  static Holder MakeNode(int id, int parent, int level, char branch) {
    auto& p = std::make_unique<HashTrieNode>();
    p->parent = parent;
    p->level = level;
    p->id = id;
    p->branch = branch;
    return std::move(p);
  }
  HashTrieNode() {
    for(int i = 0; i < 256; i++) forward[i].store(0x0fffffff);
    for(int i = 0; i < segment_size; i++) segment[i].store(0);
  }
  static constexpr size_t segment_size = 16;
  int32_t parent; // parent node inedx
  int32_t id; // current node index
  unsigned char level; // starts from 0
  char branch; // the branch of parent
  std::atomic<int32_t> forward[256];
  HashNode table[hashSize];
  std::atomic<bool> segment[segment_size]; // mutation locks
};

class HashTrie {
  friend HashTrieIterator;
 public:
  HashTrie(const std::string& filename)
    : values_(filename + ".snapshot") { 
      nodes_.push_back(HashTrieNode<hash_size_>::MakeNode(0, 0, 0, 0)); 
    }
  virtual ~HashTrie() { }
  // Access Routine Family //
  // possible error return values includes
  // Curruption, NotFound

  // put return value into `ret`
  Status Get(const Key& key, Value& ret);
  Status Put(const Key& key, const Value& value);
  Status Delete(const Key& key);
  // scan in range [lower, upper)
  Status Scan(const Key& lower, const Key& upper, HashTrieIterator& ret);
  // for debug
 #ifdef PORTAL_DEBUG
  void Dump() const {
    Dump(0);
  }
  void Dump(uint32_t idx) const {
    auto node = nodes_[idx];
    int interval = 7;
    int count = 0;
    std::cout << std::string((int)(node->level), ' ');
    for(int i = 0; i < hash_size_; i++) {
      auto hnode = node->table[i];
      if(hnode.pointer != 0 && hnode.value != 0x0fffffff){
        if( count % interval == 0 && count > 0)
          std::cout  << std::endl << std::string((int)(node->level), ' ');
        std::cout << std::string(values_.Get(hnode.value), 8) << "|";
        count ++;
      }
    }
    std::cout << std::endl;
    for(int i = 0; i <= 127; i++) {
      if(node->forward[i] < 0) Dump(-node->forward[i]);
    }
  }
 #endif // PORTAL_DEBUG
 protected:
  // size of hash table in single node
  static constexpr size_t hash_size_ = 512;
  // number of probing before declaring a full node
  static constexpr size_t probe_depth_ = 4;
  // stores HashTrieNode in linked vector
  ConcurrentVector<HashTrieNode<hash_size_>> nodes_;
  // stores key-value pair in compact manner
  // convenient to snapshot
  PagedPool<8+256> values_;

  // hash functions //
  // guarantee no hash collision on only one element
  uint32_t hash(const Key& key, int start, int end) {
    uint32_t seed = 131;
    uint32_t ret = 0;
    while(start < end) {
      ret = ret * seed + key[start++];
    }
    return ret;
  }
  uint32_t hash(const char* p, int start, int end) {
    uint32_t seed = 131;
    uint32_t ret = 0;
    while(start < end) {
      ret = ret * seed + p[start++];
    }
    return ret;
  }
  // check routine family //
  // check if hit a key
  bool check(uint32_t index, const Key& key) {
    if(index == 0x0fffffff) return false;
    char* p = values_.Get(index);
    return p != NULL && key == p;
  }
  // check and pull value if hit
  bool check_pull(uint32_t index, const Key& key, Value& value) {
    if(index == 0x0fffffff) return false;
    char* p = values_.Get(index);
    if(p && key == p) {
      value.copy<0, 256>(p + 8);
      return true;
    }
    return false;
  }
  // check and delete value if hit
  bool check_delete(uint32_t index, const Key& key, int level) {
    if(index == 0x0fffffff) return false;
    char* p = values_.Get(index);
    if(p && key == p) {
      *(reinterpret_cast<uint64_t*>(p)) = 0;
      *(p+8) = key[level];
      return true;
    }
    return false;
  }
  // chech and update value if hit
  bool check_push(uint32_t index, const Key& key, const Value& value, int level) {
    if(index == 0x0fffffff) return false;
    char* p = values_.Get(index);
    if(p) {
      if(key == p) {
        value.write<0, 256>(p + 8);
        return true;
      } else if( *(reinterpret_cast<uint64_t*>(p)) == 0
        && *(p+8) == key[level]) {
        memcpy(p, key.raw_ptr(), 8);
        value.write<0,256>(p+8);
        return true;
      }
    }
    return false;
  }
  // put record slice into tree
  // used in single thread
  Status PutRecover(size_t value_idx);
  // put key value into node with mutation lock
  Status PutWithMutationLock(const Key& key, 
                             const Value& value, 
                             HashTrieNode<hash_size_>::UnsafeRef node,
                             AtomicLock&& lock);
  // put record slice into node with exclusive access
  // bug: use uint32 as node_idx
  Status PutToIsolatedNode(uint32_t value_idx, int32_t node_idx);
};


} // namespace portal_db

#endif // PORTAL_DB_HASH_TRIE_H_