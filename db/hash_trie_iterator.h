#ifndef PORTAL_DB_HASH_TRIE_ITERATOR_H_
#define PORTAL_DB_HASH_TRIE_ITERATOR_H_

#include "portal_db/slice.h"
#include "portal_db/piece.h"
#include "portal_db/status.h"

#include <cstdint>
#include <vector>

namespace portal_db {

class HashTrie;

class HashTrieIterator : public ReadIterator<KeyValue>{
  friend HashTrie;
 public:
  HashTrieIterator(bool sort = false): sort(sort) { }
  HashTrieIterator(const HashTrieIterator& rhs)
      : ref_(rhs.ref_), 
        sort(rhs.sort),
        upper(rhs.upper),
        lower(rhs.lower),
        node_id_(rhs.node_id_),
        path_(rhs.path_),
        current_(rhs.current_),
        buffer_(rhs.buffer_) { }
  HashTrieIterator(HashTrieIterator&& rhs)
      : ref_(rhs.ref_), 
        sort(rhs.sort),
        upper(rhs.upper),
        lower(rhs.lower),
        node_id_(rhs.node_id_),
        path_(rhs.path_),
        current_(rhs.current_),
        buffer_(std::move(rhs.buffer_)) { 
    rhs.ref_ = NULL; rhs.node_id_ = -1;
  }
  bool Next() {
    if(current_ + 1 >= buffer_.size()) {
      if(!Update().inspect()) return false;
    }
    if(current_ + 1 < buffer_.size()) {
      current_ ++;
      return true;
    } else return false;
  }
  // called after `Next` returns true
  const KeyValue& Peek() const {
    return buffer_[current_];
  }
  size_t size() const {
    return buffer_.size();
  }
  void set_hash_trie(HashTrie* p) { ref_ = p; }
 private:
  HashTrie* ref_ = NULL;
  const bool sort;
  Key upper; // empty for inf
  Key lower;
  // cursor
  int32_t node_id_ = -1; // node is hit for current prefix
  Key path_;
  // epoch
  int32_t current_ = -1;
  std::vector<KeyValue> buffer_;
  // read data from cursor, then update cursor
  Status Update();
};

} // namespace portal_db
#endif // PORTAL_DB_HASH_TRIE_ITERATOR_H_