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

// _pointer___|_indication__________________|
//  0         | physically deleted, usable  |
//  0x0fff    | linked-list tail            |
//  n         | (n-1) point to next         |
// ------------------------------------------
// _value_____|_indication__________________|
//  0x0fff    | not initialized             |
// (0,prefix) | logically deleted           |
struct HashNode {
  std::atomic<int32_t> pointer;
  uint32_t value;
  HashNode(const HashNode& rhs)
    : pointer(rhs.pointer.load()),
      value(rhs.value) { }
  HashNode(): pointer(0), value(0x0fffffff) { }
  ~HashNode() { }
};


// all naked
// raw version
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
  int32_t parent;
  int32_t id;
  unsigned char level;
  char branch;
  // >0 if pointed to local hash (-1), consistent with pointer
  // <0 if pointed to global node
  // ==0x0fff if null
  std::atomic<int32_t> forward[256];
  HashNode table[hashSize];
  ReadWriteLock latch;
  std::atomic<bool> segment[segment_size];
};

class HashTrie {
  friend HashTrieIterator;
 public:
  HashTrie(const std::string& filename)
    : values_(filename) { 
      nodes_.push_back(HashTrieNode<hash_size_>::MakeNode(0, 0, 0, 0)); 
    }
  ~HashTrie() { }
  Status Get(const Key& key, Value& ret) {
    HashTrieNode<hash_size_>::UnsafeRef node = nodes_[0];
    uint32_t level = 0;
    int32_t tmp;
    // traverse by level
    while(true) {
      // find decend path
      if( (tmp = node->forward[key[level]]) < 0) {
        // invalid path
        if(-tmp >= nodes_.size())
          return Status::Corruption("access exceeds `HashTrieNode` vector");
        node = nodes_[-tmp];
      } else if(tmp == 0x0fffffff) { // no prefix match
        break;
      } else { // find match
        // quadratic proding
        unsigned int hash_val  = hash(key, level, 8) % hash_size_;
        unsigned int cur = hash_val;
        int offset = 1;
        do{
          HashNode& hnode = node->table[cur];
          // not deleted and pull
          if(hnode.pointer != 0 && check_pull(hnode.value, key, ret)) 
            return Status::OK();
          cur = (cur+ 2*offset - 1) % hash_size_;
          offset++;
        } while( offset < probe_depth_ && cur != hash_val);
        return Status::NotFound("missing key match");
      }
      level ++;
    }
    return Status::NotFound("missing level match");
  }
  Status Put(const Key& key, const Value& value) {
    HashTrieNode<hash_size_>::UnsafeRef node = nodes_[0];
    uint32_t level = 0;
    int32_t forward_node;
    // traverse by level
   CHECK_LEVEL:
    // find descend path //
    if( (forward_node = node->forward[key[level]]) < 0) {
      // invalid path
      if( - forward_node >= nodes_.size())
        return Status::Corruption("access exceeds `HashTrieNode` vector");
      node = nodes_[-forward_node];
      level ++;
      goto CHECK_LEVEL;
    }
    // hit current level //
    // quadratic proding
    unsigned int hash_val  = hash(key, level, 8) % hash_size_;
    int32_t cur = hash_val;
    int offset = 1;
    int vacant = 0;
    COMMENT("first pass: find match")
   FIND_MATCH:
    do{
      HashNode& hnode = node->table[cur];
      if(hnode.pointer == 0) {
        vacant ++;
      } else if(check_push(hnode.value, key, value, level)) {
        // check mutation
        // no need to delete
        // linked-list is managed by mutater
        // only value may be stale
        if(node->forward[key[level]] < 0)
          goto CHECK_LEVEL; // no bad mod to pointer
        return Status::OK();
      }
      cur = (cur+ 2*offset - 1) % hash_size_;
      offset++;
    } while( offset < probe_depth_ && cur != hash_val);
    COMMENT("second pass: find vacant")
   FIND_VACANT:
    if(vacant <= 0) goto MUTATE; // skip
    cur = hash_val;
    offset = 1;
    do {
      HashNode& hnode = node->table[cur];
      int32_t tmp;
      // node is deleted
      if( (tmp=hnode.pointer) == 0) {
        // point to existing linked-list
        if(std::atomic_compare_exchange_strong(&hnode.pointer, 
          &tmp, 
          node->forward[key[level]]
          )) {
          // allocate new memory
          if((hnode.value = values_.New()) == 0x0fffffff)
            return Status::Corruption("failed to create new value"); 
          // write data to memory
          char* p = values_.Get(hnode.value);
          memcpy(p, key.raw_ptr(), 8);
          value.write<0, 256>(p + 8);
          tmp = hnode.pointer; // assumed old head
          // insert as linked-list head
          while(!std::atomic_compare_exchange_strong(
            &node->forward[key[level]], 
            &tmp,
            cur + 1
            )) {
            if(tmp < 0) { // mutated
              // not in linker-list
              // need delete
              hnode.pointer = 0;
              goto CHECK_LEVEL;
            }
            hnode.pointer = tmp; // point to new head
          }
          return Status::OK();
        }
        // or preempted
        vacant --;
      }
      cur = (cur+ 2*offset - 1) % hash_size_;
      offset++;
    } while (vacant > 0 && offset < probe_depth_ && cur != hash_val);
    COMMENT("third pass: mutate")
   MUTATE:
    AtomicLock mutation_lock(node->segment[key[level] % node->segment_size]);
    if(node->forward[key[level]] < 0) {
      goto CHECK_LEVEL; // mutated
      // release out-of-scope lock
    }
    return PutWithMutationLock(key, value, node, std::move(mutation_lock));
  }
  Status Delete(const Key& key) {
    HashTrieNode<hash_size_>::UnsafeRef node = nodes_[0];
    unsigned char level = 0;
    int32_t tmp;
    // traverse by level
    while(true) {
      // find decend path
      if( (tmp = node->forward[key[level]]) < 0) {
        // invalid path
        if(-tmp >= nodes_.size())
          return Status::Corruption("access exceeds `HashTrieNode` vector");
        node = nodes_[-tmp];
      } else if(tmp == 0x0fffffff) { // no prefix match
        break;
      } else { // find match
        // quadratic proding
        unsigned int hash_val  = hash(key, level, 8) % hash_size_;
        unsigned int cur = hash_val;
        int offset = 1;
        do{
          HashNode& hnode = node->table[cur];
          if(hnode.pointer != 0 && check_delete(hnode.value, key, level)) {
            return Status::OK();
          }
          cur = (cur+ 2*offset - 1) % hash_size_;
          offset++;
        } while( offset < probe_depth_ && cur != hash_val);
        return Status::NotFound("missing key match");
      }
      level ++;
    }
    return Status::NotFound("missing key match");
  }
  Status Scan(const Key& lower, const Key& upper, HashTrieIterator& ret) {
    ret.set_hash_trie(this);
    ret.upper = upper;
    ret.lower = lower;
    ret.path_ = lower;
    HashTrieNode<hash_size_>::UnsafeRef node = nodes_[0];
    int32_t tmp;
    char c = lower[0];
    while(c >= 0) {
      if(( tmp=node->forward[c]) < 0) {
        assert(-tmp < nodes_.size());
        node = nodes_[-tmp];
        c = lower[node->level];
      } else if(tmp == 0x0fffffff) {
        c ++;
      } else { // hit
        ret.node_id_ = node->id;
        ret.path_[node->level] = c;
        for(int i = node->level + 1; i < 8; i++)
          ret.path_[i] = 0;
        return Status::OK();
      }
    }
    ret.node_id_ = -1; // no hit at all
    return Status::NotFound("no key found in this range");
  }
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
    if(node->forward['9'] != 0x0fffffff && node->forward['9'] > 0) {
      std::cout << "(";
      int32_t idx = node->forward['9'];
      while(idx != 0x0fffffff) {
        if(idx == 0) {
          std::cout << "ZERO";
          break;
        }
        std::cout << std::string(values_.Get(node->table[idx-1].value), 8) << ",";
        idx = node->table[idx-1].pointer;
      }
      std::cout << ")";
    }
    for(int i = 0; i <= 127; i++) {
      if(node->forward[i] < 0) Dump(-node->forward[i]);
    }
  }
 #endif // PORTAL_DEBUG
 private:
  static constexpr size_t hash_size_ = 512;
  static constexpr size_t probe_depth_ = 4;
  ConcurrentVector<HashTrieNode<hash_size_>> nodes_;
  PagedPool<8+256> values_;
  bool track = false;
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
  bool check(uint32_t index, const Key& key) {
    if(index == 0x0fffffff) return false;
    char* p = values_.Get(index);
    return p != NULL && key == p;
  }
  bool check_pull(uint32_t index, const Key& key, Value& value) {
    if(index == 0x0fffffff) return false;
    char* p = values_.Get(index);
    if(p && key == p) {
      value.copy<0, 256>(p + 8);
      return true;
    }
    return false;
  }
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
  Status PutWithMutationLock(const Key& key, 
                             const Value& value, 
                             HashTrieNode<hash_size_>::UnsafeRef node,
                             AtomicLock&& lock) {
    AtomicLock mutation_lock = std::move(lock); // takeover lock ownership
    // restore context info
    uint32_t level = node->level;
    // create new node
    int32_t new_node_idx = nodes_.push_back(
      HashTrieNode<hash_size_>::MakeNode(
        0,
        node->id,
        level + 1,
        key[level]
      ));
    HashTrieNode<hash_size_>::UnsafeRef new_node = nodes_[new_node_idx];
    new_node->id = new_node_idx;
    // traverse old list and copy to new node
    int32_t old_idx = node->forward[key[level]]; // snapshot
    int32_t cur_idx = old_idx;
    assert(cur_idx > 0);
    Status status;
    while(cur_idx != 0x0fffffff) {
      HashNode& hnode = node->table[cur_idx - 1];
      cur_idx = hnode.pointer; // linked-list.next
      assert(values_.Get(hnode.value) != NULL);
      // handle all potential mutation
      status *= PutToIsolatedNode(hnode.value, new_node_idx);
      if(!status.ok()) return status;
    }
    // now insert current record
    uint32_t new_record = values_.New();
    if(new_record == 0x0fffffff)
      return Status::Corruption("failed to create new entry");

    char* p = values_.Get(new_record);
    memcpy(p, key.raw_ptr(), 8);
    value.write<0, 256>(p + 8);
    status *= PutToIsolatedNode(new_record, new_node_idx);
    if(!status.ok())
      return status;
    // mutate old forward pointer
    int32_t new_idx = old_idx;
    while(!std::atomic_compare_exchange_strong(
      &node->forward[key[level]],
      &new_idx,
      -new_node_idx
      )) {
      // if new value is inserted, copy to new node
      cur_idx = new_idx;
      while(cur_idx != old_idx && cur_idx != 0x0fffffff) {
        HashNode& hnode = node->table[cur_idx - 1];
        cur_idx = hnode.pointer;
        status *= PutToIsolatedNode(hnode.value, new_node_idx);
        if(!status.ok()) return status;
      }
      old_idx = new_idx;
    }
    // physically delete old item
    cur_idx = new_idx;
    while(cur_idx != 0x0fffffff) {
      HashNode& hnode = node->table[cur_idx - 1];
      cur_idx = hnode.pointer;
      hnode.pointer = 0;
    }
    return Status::OK();
  }
  // bug: use uint32 as node_idx
  Status PutToIsolatedNode(uint32_t value_idx, int32_t node_idx) {
    char* p = values_.Get(value_idx);
    if(!p) return Status::Corruption("null entry");
    if(*(reinterpret_cast<uint64_t*>(p)) == 0) // deleted
      return Status::OK();
    HashTrieNode<hash_size_>::UnsafeRef node = nodes_[node_idx];
    int level = node->level;
    // descend if needed
    while( (node_idx = -node->forward[p[level]]) > 0) {
      if(node_idx >= nodes_.size()) return Status::Corruption("forward pointer overflow");
      node = nodes_[node_idx];
      level ++; // bug
      assert(level == node->level);
    }
    // quadratic proding
    unsigned int hash_val  = hash(p, level, 8) % hash_size_;
    int32_t cur = hash_val;
    int offset = 1;
    do {
      HashNode& hnode = node->table[cur];
      int32_t tmp;
      if( (tmp=hnode.pointer) == 0) {
        hnode.pointer = node->forward[p[level]].load();
        hnode.value = value_idx;
        node->forward[p[level]] = cur + 1;
        return Status::OK();
      }
      cur = (cur+ 2*offset - 1) % hash_size_;
      offset++;
    } while (offset < probe_depth_ && cur != hash_val);
    // create new node
    int32_t new_node_idx = nodes_.push_back(
      HashTrieNode<hash_size_>::MakeNode(
        0,
        node->id,
        level + 1,
        p[level]
      ));
    HashTrieNode<hash_size_>::UnsafeRef new_node = nodes_[new_node_idx];
    new_node->id = new_node_idx;
    // wipe history record
    int32_t cur_idx = node->forward[p[level]];
    assert(cur_idx > 0);
    node->forward[p[level]] = -new_node_idx; // mutate forward path
    Status status;
    // move old record
    while(cur_idx != 0x0fffffff) {
      HashNode& hnode = node->table[cur_idx-1];
      cur_idx = hnode.pointer; // next
      hnode.pointer = 0; // delete
      status *= PutToIsolatedNode(hnode.value, new_node_idx);
      if(!status.ok())
        return status;
    }
    // put requested record
    return PutToIsolatedNode(value_idx, new_node_idx);
  }
};


} // namespace portal_db

#endif // PORTAL_DB_HASH_TRIE_H_