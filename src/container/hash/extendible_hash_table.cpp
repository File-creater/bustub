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
#include <memory>
#include <utility>

#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.push_back(std::make_shared<Bucket>(bucket_size_));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
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

  std::scoped_lock<std::mutex> lock(latch_);

  auto index = IndexOf(key);

  return dir_[index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");

  std::scoped_lock<std::mutex> lock(latch_);

  auto index = IndexOf(key);

  return dir_[index]->Remove(key);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  auto &ori_list = bucket->GetItems();

  auto tmp_list = ori_list;

  ori_list.clear();

  for (const auto &p : tmp_list) {
    auto this_key = p.first;
    auto this_value = p.second;
    InsertInternal(this_key, this_value);
  }
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::InsertInternal(const K &key, const V &value) {
  auto index = IndexOf(key);

  // std::cout << "insert key(" << key << ") and its index is " << index << std::endl;

  V tmp_v;

  if (dir_[index]->Find(key, tmp_v)) {  // exist
    if (!dir_[index]->Insert(key, value)) {
      LOG_DEBUG("impossible way : key/value pair exists but update failed!");
      return;
    }

    return;
  }

  // not exist, try insert

  if (dir_[index]->IsFull()) {
    if (dir_[index]->GetDepth() == global_depth_) {
      global_depth_++;

      for (int i = 0; i < num_buckets_; i++) {
        dir_.push_back(dir_[i]);
      }

      num_buckets_ = dir_.size();
    }

    dir_[index]->IncrementDepth();

    // 原来的满的bucket
    auto full_bucket = dir_[index];

    // 新bucket的索引入口
    auto new_index = IndexOf(key);

    // 分配新指针到新的bucket
    // TODO(liangyao): ：这里可能有多个兄弟指针，或许需要优化
    dir_[new_index] = std::make_shared<Bucket>(bucket_size_, full_bucket->GetDepth());

    // std::cout << "start RedistributeBucket " << index << std::endl;

    // 分配原来的bucket中的kv
    RedistributeBucket(full_bucket);
    // std::cout << "end RedistributeBucket " << index << std::endl;

    InsertInternal(key, value);

  } else {
    if (!dir_[index]->Insert(key, value)) {
      LOG_DEBUG("not full but insert failed");
    }
  }
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  // UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);
  InsertInternal(key, value);

  // std::cout << "after insert " << key << "/" << value << std::endl;
  // std::string res;

  // std::cout << "after insert\n";
  // std::cout << "num_buckets_ is " << num_buckets_ << ";    ";
  // for (const auto & p : dir_) {
  //   std::cout << p->GetDepth() << "," << p->IsFull() <<";  ";
  // }
  // std::cout << "\n";
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (const auto &p : list_) {
    if (p.first == key) {
      value = p.second;
      return true;
    }
  }

  return false;

  // UNREACHABLE("not implemented");
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if (it->first == key) {
      list_.erase(it);
      return true;
    }
  }

  return false;

  // UNREACHABLE("not implemented");
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for (auto &p : list_) {
    if (p.first == key) {
      p.second = value;  // update value

      std::cout << "target k/v exists\n";
      return false;
    }
  }

  if (IsFull()) {  // full
    std::cout << "bucket is full\n";
    return false;
  }

  list_.emplace_back(key, value);  // insert
  return true;

  // UNREACHABLE("not implemented");
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
