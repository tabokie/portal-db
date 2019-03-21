#ifndef PORTAL_DB_HASH_TRIE_H_
#define PORTAL_DB_HASH_TRIE_H_

#include "portal_db/piece.h"
#include "paged_pool.h"
#include "util/util.h"
#include "util/readwrite_lock.h"
#include "util/debug.h"

#include <atomic>
#include <iostream>

namespace portal_db {

namespace {

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
struct HashTrieNode {
  using Holder = std::unique_ptr<HashTrieNode>;
  using UnsafeRef = HashTrieNode*;
  static Holder MakeNode(int id, int parent, int level) {
    auto& p = std::make_unique<HashTrieNode>();
    p->parent = parent;
    p->level = level;
    p->id = id;
    return std::move(p);
  }
  HashTrieNode() {
    for(int i = 0; i < 256; i++) forward[i].store(0x0fffffff);
  }
  int32_t parent;
  int32_t id;
  unsigned char level;
  // >0 if pointed to local hash (-1)
  // <0 if pointed to global node
  // ==0x0fff if null
  std::atomic<int32_t> forward[256];
  HashNode table[hashSize];
  ReadWriteLock latch;
};

} // lambda namespace


class HashTrie {
 public:
  HashTrie(const std::string& filename)
    : values_(filename) { 
      nodes_.push_back(HashTrieNode<hash_size_>::MakeNode(0, 0, 0)); 
    }
  ~HashTrie() { }
  Status Get(const Key& key, Value& ret) {
    HashTrieNode<hash_size_>::UnsafeRef node = nodes_[0].get();
    unsigned char level = 0;
    int32_t tmp;
    // traverse by level
    while(true) {
      // find decend path
      if( (tmp = -node->forward[key[level]]) > 0) {
        // invalid path
        if(tmp >= nodes_.size())
          return Status::Corruption("access exceeds `HashTrieNode` vector");
        node = nodes_[tmp].get();
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
    HashTrieNode<hash_size_>::UnsafeRef node = nodes_[0].get();
    unsigned char level = 0;
    int32_t tmp;
    // traverse by level
    while(true) {
      // find descend path
      if( (tmp = -node->forward[key[level]]) > 0) {
        // invalid path
        if(tmp >= nodes_.size())
          return Status::Corruption("access exceeds `HashTrieNode` vector");
        node = nodes_[tmp].get();
      } else { // hit level
        // quadratic proding
        unsigned int hash_val  = hash(key, level, 8) % hash_size_;
        int32_t cur = hash_val;
        int offset = 1;
        int vacant = 0;
        COMMENT("first pass: find match")
        do{
          HashNode& hnode = node->table[cur];
          if(hnode.pointer == 0) {
            vacant ++;
          } else if(check_push(hnode.value, key, value, level)) {
            return Status::OK();
          }
          cur = (cur+ 2*offset - 1) % hash_size_;
          offset++;
        } while( offset < probe_depth_ && cur != hash_val);
        COMMENT("second pass: find vacant")
        if(vacant > 0) {
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
                while(hnode.value == 0x0fffffff ) {
                  hnode.value = values_.New();
                }
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
                  )) { hnode.pointer = tmp; }
                return Status::OK();
              }
              // or preempted
              vacant --;
            }
            cur = (cur+ 2*offset - 1) % hash_size_;
            offset++;
          } while (vacant > 0 && offset < probe_depth_ && cur != hash_val);
        }
        COMMENT("third pass: mutate")
        nodes_.push_back(
          HashTrieNode<hash_size_>::MakeNode(
            nodes_.size(),
            node->id, 
            level + 1
          ));
        HashTrieNode<hash_size_>::UnsafeRef new_node = nodes_.back().get();
        int32_t old_idx = node->forward[key[level]];
        assert(old_idx > 0);
        int32_t cur_idx = old_idx;
        while(cur_idx != 0x0fffffff) {
          HashNode& hnode = node->table[cur_idx - 1];
          cur_idx = hnode.pointer;
          assert(values_.Get(hnode.value) != NULL);
          if(!put_to_node(hnode.value, nodes_.size() - 1)) {
            return Status::Corruption("mutation failed");
          }
        }
        // now insert current record
        uint32_t new_record = 0x0fffffff;
        while(new_record == 0x0fffffff ) {
          new_record = values_.New();
        }
        char* p = values_.Get(new_record);
        memcpy(p, key.raw_ptr(), 8);
        value.write<0, 256>(p + 8);
        if(!put_to_node(new_record, nodes_.size() - 1)) {
          return Status::Corruption("failed to insert after mutation");
        }
        int32_t new_idx = old_idx;
        while(!std::atomic_compare_exchange_strong(
          &node->forward[key[level]],
          &new_idx,
          -nodes_.size()+1
          )) {
          cur_idx = new_idx;
          while(cur_idx != old_idx && cur_idx != 0x0fffffff) {
            HashNode& hnode = node->table[cur_idx - 1];
            cur_idx = hnode.pointer;
            if(!put_to_node(hnode.value, nodes_.size() - 1)) {
              return Status::Corruption("incremental mutation failed");
            }
          }
          old_idx = new_idx;
        }
        return Status::OK();
      }
      level ++;
    }
    return Status::NotFound("missing key");
  }
  Status Delete(const Key& key) {
    HashTrieNode<hash_size_>::UnsafeRef node = nodes_[0].get();
    unsigned char level = 0;
    int32_t tmp;
    // traverse by level
    while(true) {
      // find decend path
      if( (tmp = -node->forward[key[level]]) > 0) {
        // invalid path
        if(tmp >= nodes_.size())
          return Status::Corruption("access exceeds `HashTrieNode` vector");
        node = nodes_[tmp].get();
      } else if(tmp == 0) { // no prefix match
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
        return Status::NotFound("missing key");
      }
      level ++;
    }
    return Status::NotFound("missing key");
  }
 private:
  static constexpr size_t hash_size_ = 512;
  static constexpr size_t probe_depth_ = 100;
  std::vector<HashTrieNode<hash_size_>::Holder> nodes_;
  PagedPool<8+256> values_;
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
  // exit on full
  // no latch
  // no overwrite
  bool put_to_node(uint32_t value_idx, uint32_t node_idx) {
    HashTrieNode<hash_size_>::UnsafeRef node = nodes_[node_idx].get();
    char* p = values_.Get(value_idx);
    if(p && *(reinterpret_cast<uint64_t*>(p)) == 0) { return true; }
    int level = node->level;
    // quadratic proding
    unsigned int hash_val  = hash(p, level, 8) % hash_size_;
    int32_t cur = hash_val;
    int offset = 1;
    cur = hash_val;
    offset = 1;
    do {
      HashNode& hnode = node->table[cur];
      int32_t tmp;
      if( (tmp=hnode.pointer) == 0) {
        // std::cout << "found null at " << cur << std::endl;
        if(std::atomic_compare_exchange_strong(&hnode.pointer, 
          &tmp, 
          node->forward[p[level]]
          )) {
          hnode.value = value_idx;
          tmp = hnode.pointer;
          while(!std::atomic_compare_exchange_strong(
            &node->forward[p[level]], 
            &tmp,
            cur + 1
            )) { hnode.pointer = tmp; }
          return true;
        }
      }
      cur = (cur+ 2*offset - 1) % hash_size_;
      offset++;
    } while (offset < probe_depth_ && cur != hash_val);
    return false;
  }
};


} // namespace portal_db

#endif // PORTAL_DB_HASH_TRIE_H_