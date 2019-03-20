#ifndef PORTAL_DB_HASH_TRIE_H_
#define PORTAL_DB_HASH_TRIE_H_

#include "portal-db/piece.h"
#include "paged_pool.h"

#include <atomic>
#include <iostream>

namespace portal_db {

namespace {

struct HashNode {
	std::atomic<int32_t> pointer; // 0 means null, f means tail
	uint32_t value; // start from 0, f means invalid
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
	// positive / 0 if pointed to local hash
	// negative if pointed to global node
	// 0fffffff if null
	std::atomic<int32_t> forward[256];
	HashNode table[hashSize];
};

} // lambda namespace


class HashTrie {
 public:
 	HashTrie(): values_("unique") { nodes_.push_back(HashTrieNode<hash_size_>::MakeNode(0, 0, 0)); }
 	~HashTrie() { }
 	Status Get(const Key& key, Value& ret) {
 		HashTrieNode<hash_size_>::UnsafeRef node = nodes_[0].get();
 		unsigned char level = 0;
 		int32_t tmp;
 		while(true) {
 			if( (tmp = -node->forward[key[level]]) > 0) {
 				if(tmp >= nodes_.size())
 					return Status::Corruption("access exceeds `HashTrieNode` vector");
 				node = nodes_[tmp].get();
 			} else if(tmp == 0) {
 				break;
 			} else { // hit
 				unsigned int hash_val  = hash(key, level, 8) % hash_size_;
		    unsigned int cur = hash_val;
		    int offset = 1;
		    // quadratic proding
		    do{
		    	HashNode& hnode = node->table[cur];
		    	if(hnode.pointer != 0 && check_pull(hnode.value, key, ret)) return Status::OK();
		      cur = (cur+ 2*offset - 1) % hash_size_;
		      offset++;
		    } while( offset < probe_depth_ && cur != hash_val);
		    return Status::NotFound("missing key");
 			}
 			level ++;
 		}
 		return Status::NotFound("missing key");
 	}
 	Status Put(const Key& key, const Value& value) {
 		HashTrieNode<hash_size_>::UnsafeRef node = nodes_[0].get();
 		unsigned char level = 0;
 		int32_t tmp;
 		while(true) {
 			// traverse by level //
 			if( (tmp = -node->forward[key[level]]) > 0) { // find descend path
 				if(tmp >= nodes_.size()) // invalid path
 					return Status::Corruption("access exceeds `HashTrieNode` vector");
 				node = nodes_[tmp].get();
 			} else { // hit level
 				unsigned int hash_val  = hash(key, level, 8) % hash_size_;
		    int32_t cur = hash_val;
		    int offset = 1;
		    int rest = 0;
		    // quadratic proding //
		    // first pass: find match
		    // std::cout << "first pass: find match" << std::endl;
		    do{
		    	HashNode& hnode = node->table[cur];
		    	if(check(hnode.value, key)) {
		    		// std::cout << "found match at " << cur << std::endl;
		    		int32_t tmp;
		    		if( (tmp=hnode.pointer) == 0) { // fix linked list
		    			// std::cout << "it's deleted" << std::endl;
		    			if(std::atomic_compare_exchange_strong(
		    				&hnode.pointer, 
		    				&tmp,
		    				node->forward[key[level]]
		    				)) { // fixed
		    				if(!check_push(hnode.value, key, value)) 
		    					return Status::Corruption("volatile fixed pair on hit entry");
		    				tmp = hnode.pointer;
		    				while(!std::atomic_compare_exchange_strong(
			    				&node->forward[key[level]], 
			    				&tmp,
			    				cur
			    				)) { hnode.pointer = tmp; }
		    				return Status::OK();
		    			}
		    			// or taken by others
		    		}
		    		if(check_push(hnode.value, key, value)) {
		    			return Status::OK();
		    		}
		    		// taken by other key
		    	} else if(hnode.pointer == 0) {
		    		// std::cout << "found null at "<< cur << std::endl;
		    		rest ++;
		    	}
		      cur = (cur+ 2*offset - 1) % hash_size_;
		      offset++;
		    } while( offset < probe_depth_ && cur != hash_val);
		    // second pass: find vacant
		    if(rest > 0) {
		    	// std::cout << "second pass: find vacant" << std::endl;
			    cur = hash_val;
			    offset = 1;
			    do {
		    		HashNode& hnode = node->table[cur];
			    	int32_t tmp;
			    	if( (tmp=hnode.pointer) == 0) {
			    		// std::cout << "found null at " << cur << std::endl;
			    		if(std::atomic_compare_exchange_strong(&hnode.pointer, 
			    			&tmp, 
			    			node->forward[key[level]]
			    			)) {
			    			while(hnode.value == 0x0fffffff ) {
			    				hnode.value = values_.New();
			    			}
			    			char* p = values_.Get(hnode.value);
			    			memcpy(p, key.raw_ptr(), 8);
			    			value.write<0, 256>(p + 8);
		    				tmp = hnode.pointer;
			    			while(!std::atomic_compare_exchange_strong(
			    				&node->forward[key[level]], 
			    				&tmp,
			    				cur
			    				)) { hnode.pointer = tmp; }
		    				return Status::OK();
			    		}
			    		rest --;
			    		cur = (cur+ 2*offset - 1) % hash_size_;
		     	 		offset++;
			    	}
			    }	while (rest > 0 && offset < probe_depth_ && cur != hash_val);
		    }
		    // third pass: mutate
		    nodes_.push_back(HashTrieNode<hash_size_>::MakeNode(nodes_.size(), node->id, level + 1));
		    HashTrieNode<hash_size_>::UnsafeRef new_node = nodes_.back().get();
		    int old_idx = node->forward[key[level]];
		    while(old_idx != 0x0fffffff) {

		    }
			  return Status::IOError("full hash table");
 			}
 			level ++;
 		}
 		return Status::NotFound("missing key");
 	}
 	Status Delete(const Key& key) {
 		return Status::OK();
 	}
 private:
 	static constexpr size_t hash_size_ = 512;
 	static constexpr size_t probe_depth_ = 20;
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
 	bool check_push(uint32_t index, const Key& key, const Value& value) {
 		if(index == 0x0fffffff) return false;
 		char* p = values_.Get(index);
 		if(p && key == p) {
 			value.write<0, 256>(p + 8);
 			return true;
 		}
 		return false;
 	}
};


} // namespace portal_db

#endif // PORTAL_DB_HASH_TRIE_H_