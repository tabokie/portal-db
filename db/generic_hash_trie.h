#ifndef PORTAL_DB_GENERIC_HASH_TRIE_H_
#define PORTAL_DB_GENERIC_HASH_TRIE_H_

#include "portal_db/piece.h"

// generic version of HashTrie //

namespace portal_db {

namespace {

// all naked
// raw version
template <typename ElementType, size_t hashSize = 512>
struct HashTrieNode {
  using Holder = std::unique_ptr<HashTrieNode>;
  using UnsafeRef = HashTrieNode*;
  static Holder MakeNode() {
    return std::make_unique<HashTrieNode>();
  }
  HashTrieNode() { }
  int32_t parent;
  unsigned char level;
  // positive / 0 if pointed to local hash
  // negative if pointed to global node
  int32_t forward[256];
  ElementType table[hashSize];
};

} // lambda namespace

template <typename ElementType>
class GenericHashTrie {
 public:
  GenericHashTrie() { }
  ~GenericHashTrie() { }
  Status Get(const CharwiseAccess& key, ElementType& ret) { return Status::OK(); }
  Status Put(const CharwiseAccess& key, const ElementType& value) { return Status::OK(); }
  Status Delete(const CharwiseAccess& key) { return Status::OK(); }
  virtual uint32_t hash(const CharwiseAccess& key, int start, int end) {
    uint32_t seed = 131;
    uint32_t ret = 0;
    while(start < end) {
      ret = ret * seed + key[start++];
    }
    return ret;
  }
  virtual void make_null(ElementType& value) = 0;
  virtual bool swap_null(ElementType& a) = 0; // concurrent compete for value
  virtual bool is_null(const ElementType& a) = 0;
  virtual bool is_hit(const ElementType& a, const ElementType& b) = 0;
 private:
  static constexpr size_t hash_size_ = 512;
  std::vector<HashTrieNode<ElementType, hash_size_>::Holder> nodes_;
};


} // namespace portal_db

#endif // PORTAL_DB_GENERIC_HASH_TRIE_H_