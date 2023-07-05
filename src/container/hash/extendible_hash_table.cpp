//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  auto b = std::make_shared<Bucket>(bucket_size, 0);
  dir_.push_back(b);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  // UNREACHABLE("not implemented");
  size_t index = IndexOf(key);
  if(index >= dir_.size()) {
    return false;
  }
  
  return dir_[index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  size_t index = IndexOf(key);
  if(index >= dir_.size()) {
    return false;
  }
  return dir_[index]->Remove(key);
  
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  // UNREACHABLE("not implemented");
  size_t index = IndexOf(key);
  // printf("INSERT KEY HASH index: %ld\n", index);
  if(index >= dir_.size()) {
    return;
  }
    //遍历输出dir_
  
  // printf("==========\n");
  // printf("dir_ size: %ld\n", dir_.size());
  for(size_t i=0; i<dir_.size(); i++) {
    // printf("dir_[%ld]: ", i);
    // printf("local depth: %d size: %ld %p\n", dir_[i]->GetDepth(), dir_[i]->GetItems().size(), dir_[i].get());
  }
  // printf("==========\n");
  if(!(dir_[index]->Insert(key, value))) {
    // printf("Insert failed, cause full.\n");
    // If is full

    // If the local depth of the bucket is equal to the global depth
    if(dir_[index]->GetDepth() == global_depth_) {
      // printf("local depth == global depth\n");
      // Increment the global depth and double the size of the directory
      global_depth_ ++;
      // printf("global depth: %d\n", global_depth_);
      // Increment the local depth
      dir_[index]->IncrementDepth();

      size_t beforeDirSize = dir_.size();
      // printf("beforeDirSize: %ld\n", beforeDirSize);
      int beforeMask = (1 << (global_depth_-1)) -1;
      int curMask = (1 << global_depth_) -1;
      size_t newInsertIndex = IndexOf(key); //因为gd++了，所以要重新得一次。

      for(int i=(int)beforeDirSize; i<(1 << global_depth_); i++){
        if(((i&beforeMask) == (int)index) && ((i&curMask) != (int)index)) {
          newInsertIndex = i;
          std::shared_ptr<Bucket> before_b = dir_[index];
          std::list<std::pair<K, V>> before_b_list = before_b->GetItems();
          auto b = std::make_shared<Bucket>(bucket_size_, global_depth_);
          // printf("new bucket: %p\n", b.get());
          num_buckets_ ++;
          //遍历这个桶
          for(auto it=before_b_list.begin(); it!=before_b_list.end(); it++) {
            //查询其中每一个元素的key
            if(IndexOf(it->first) == newInsertIndex) {
              // printf("桶替换: %ld\n", IndexOf(it->first));
              b->Insert(it->first, it->second);
              before_b->Remove(it->first);
            }
          }
          dir_.push_back(b);
        }else{
          dir_.push_back(nullptr); 
        }
      }

      dir_[IndexOf(key)]->Insert(key, value);
      
      for(int i=(int)beforeDirSize; i<(1 << global_depth_); i++){
        if(dir_[i] == nullptr) {
          dir_[i] = dir_[i & beforeMask];
        }
      }
      

    } else {
      // printf("local depth < global depth\n");
      // dir_不用double了. local_depth++就行
      std::shared_ptr<Bucket> before_b = dir_[index];
      std::list<std::pair<K, V>> before_b_list = before_b->GetItems();
      int newlocalDepth = before_b->GetDepth() + 1;

      //取出这个桶内第一个元素的新index作为样板,之后和这个相同的就保留,
      //不相同的就放到新建的桶上.
      int newlocalDepthMask = (1 << newlocalDepth) - 1;
      size_t sampleIndex = std::hash<K>()(before_b_list.front().first) & newlocalDepthMask;
      //新建一个桶
      auto b = std::make_shared<Bucket>(bucket_size_, newlocalDepth);
      num_buckets_ ++;
      //遍历这个桶, 相同的保留,不相同的放到新建的桶上
      for(auto it=++before_b_list.begin(); it!=before_b_list.end(); it++) {
        if((std::hash<K>()(it->first) & newlocalDepthMask) != sampleIndex) {
            b->Insert(it->first, it->second);
            before_b->Remove(it->first);
        }
      }
      //然后遍历dir_,重新指向
      for(size_t i=0; i<dir_.size(); i++) {
        if((dir_[i]==before_b) && ((i & newlocalDepthMask) != sampleIndex)) {
          dir_[i] = b;
        }
      }
      before_b->IncrementDepth();
    }
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for(auto it = list_.begin(); it != list_.end(); it++) {
      // std::cout << it->first << " " << it->second << std::endl;
      if(it->first == key) {
        value = it->second;
        return true;
      }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for(auto it = list_.begin(); it != list_.end(); it++) {
    if(it->first == key) {
      it = list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if(IsFull()) {
    return false;
  }
  for(auto it = list_.begin(); it != list_.end(); it++) {
    if(it->first == key) {
      // update
      it->second = value;
      return true;
    }
  }
  list_.push_back({key, value});
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
