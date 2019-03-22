#ifndef PORTAL_DB_HASH_TRIE_ITERATOR_H_
#define PORTAL_DB_HASH_TRIE_ITERATOR_H_

#include "portal_db/slice.h"
#include "portal_db/piece.h"
#include "portal_db/status.h"

#include <cstdint>
#include <vector>
#include <algorithm>

namespace portal_db {

class HashTrie;

class HashTrieIterator : public ReadIterator<KeyValue>, public NoCopy{

 public:
  HashTrieIterator(bool sort = false): sort(sort) { }
  bool Next() {
    if(buffer_.size() == 0) return Update().ok();
    return true;
  }
  // called after `Next` returns true
  const KeyValue& Peek() const {
    return buffer_[0];
  }
  size_t size() {
    return buffer_.size();
  }
  void set_hash_trie(HashTrie* p) { ref_ = p; }
  void set_upper(const Key& rhs) { upper = rhs; }
 private:
  HashTrie* ref_ = NULL;
  const bool sort;
  Key upper; // empty for inf
  // cursor
  uint32_t node_id_;
  Key prefix_;
  uint32_t prefix_len_;
  // epoch
  std::vector<KeyValue> buffer_;
  Status Update();
};

} // namespace portal_db
#endif // PORTAL_DB_HASH_TRIE_ITERATOR_H_