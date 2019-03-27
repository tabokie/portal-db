#include "hash_trie.h"

namespace portal_db {

Status HashTrie::Get(const Key& key, Value& ret) {
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
Status HashTrie::Put(const Key& key, const Value& value) {
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
Status HashTrie::Delete(const Key& key) {
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
Status HashTrie::Scan(const Key& lower, const Key& upper, HashTrieIterator& ret) {
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

Status HashTrie::PutRecover(size_t value_idx) {
  HashTrieNode<hash_size_>::UnsafeRef node = nodes_[0];
  uint32_t level = 0;
  int32_t forward_node;
  char* key = values_.Get(value_idx);
  if(key == NULL) return Status::Corruption("invalid value");
  // traverse by level
 CHECK_LEVEL:
  if(level >= 8) return Status::OK(); // WOW
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
  // no first pass
  COMMENT("second pass: find vacant")
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
        hnode.value = value_idx;
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
    }
    cur = (cur+ 2*offset - 1) % hash_size_;
    offset++;
  } while (offset < probe_depth_ && cur != hash_val);
  COMMENT("third pass: mutate")
  if(node->forward[key[level]] < 0) {
    goto CHECK_LEVEL; // mutated
    // release out-of-scope lock
  }
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
  status *= PutToIsolatedNode(value_idx, new_node_idx);
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
Status HashTrie::PutWithMutationLock(const Key& key, 
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
Status HashTrie::PutToIsolatedNode(uint32_t value_idx, int32_t node_idx) {
  char* p = values_.Get(value_idx);
  if(!p) return Status::Corruption("null entry");
  if(*(reinterpret_cast<uint64_t*>(p)) == 0) // deleted
    return Status::OK();
  HashTrieNode<hash_size_>::UnsafeRef node = nodes_[node_idx];
  int level = node->level;
  if(level >= 8) return Status::OK(); // WOW
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
    if(cur_idx == 0) break; // WOW
    hnode.pointer = 0; // delete
    status *= PutToIsolatedNode(hnode.value, new_node_idx);
    if(!status.ok())
      return status;
  }
  // put requested record
  return PutToIsolatedNode(value_idx, new_node_idx);
}

} // namespace portal_db