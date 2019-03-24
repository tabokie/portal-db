#include "hash_trie_iterator.h"
#include "hash_trie.h"

#include <algorithm>

namespace portal_db {

Status HashTrieIterator::Update() {
  buffer_.clear();
  current_ = -1;
  if(ref_ == NULL) 
    return Status::NotFound("index is invalidated");
  if(node_id_ < 0) 
    return Status::NotFound("node is invalidated");
  // read data
  auto node = ref_->nodes_[node_id_];
  int32_t idx = node->forward[path_[node->level]];
  while(idx != 0x0fffffff) {
    HashNode& hnode = node->table[idx-1];
    idx = hnode.pointer;
    char* p = ref_->values_.Get(hnode.value);
    if(p && (lower <= p || lower.empty()) && (!(upper <= p) || upper.empty()) ) {
      buffer_.push_back(KeyValue(p, p + 8));
    }
  }
  if(sort) 
    std::sort(buffer_.begin(), buffer_.end());
  // find next
  int32_t tmp;
  int32_t level = node->level;
  while(path_ < upper || upper.empty()) {
    // increment path
    if(path_[level] < 0) {
      // ascend
      if(level == 0) {
        node_id_ = -1;
        return Status::OK();
      }
      path_[level-1] = node->branch;
      path_[level] = 0;
      node = ref_->nodes_[node->parent];
      level = node->level;
      continue; // next loop
    } else path_[level] ++;
    // check this path
    for(; path_[level] >= 0; path_[level] ++) {
      tmp = node->forward[path_[level]];
      if(tmp < 0) { // descend
        assert(-tmp < ref_->nodes_.size());
        node = ref_->nodes_[-tmp];
        assert(level + 1 == node->level);
        level ++;
        path_[level] = 0;
      } else if(tmp != 0x0fffffff) { // hit
        node_id_ = node->id;
        return Status::OK();
      }
    }
  }
  node_id_ = -1;
  return Status::OK();
}

} // namespace portal_db