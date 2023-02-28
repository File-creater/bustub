//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/logger.h"
#include "common/macros.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  BUSTUB_ASSERT(frame_id != nullptr, "");

  for (auto &node : inf_list_) {
    if (node.evictable_) {
      *frame_id = node.frame_id_;
      RemoveInternal(*frame_id);
      return true;
    }
  }

  for (auto &node : countable_list_) {
    if (node.evictable_) {
      *frame_id = node.frame_id_;
      RemoveInternal(*frame_id);
      return true;
    }
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);

  BUSTUB_ASSERT(frame_id <= (int)replacer_size_, "invalid frame id");

  if (hash_.find(frame_id) == hash_.end()) {  // first record
    if (k_ == 1) {
      countable_list_.emplace_back(frame_id);
      hash_[frame_id] = --countable_list_.end();
    } else {
      inf_list_.emplace_back(frame_id);
      hash_[frame_id] = --inf_list_.end();
    }
  } else {
    auto it = hash_[frame_id];

    auto new_access_count = ++it->access_count_;
    auto evictable = it->evictable_;

    if (new_access_count < k_) {  // still inf
      inf_list_.erase(it);
      inf_list_.emplace_back(frame_id, new_access_count, evictable);
      hash_[frame_id] = --inf_list_.end();
    } else {
      if (new_access_count == k_) {  // inf to countable
        inf_list_.erase(it);
      } else /* new_access_count > k_ */ {  // stll countable
        countable_list_.erase(it);
      }

      countable_list_.emplace_back(frame_id, new_access_count, evictable);
      hash_[frame_id] = --countable_list_.end();
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);

  BUSTUB_ASSERT(frame_id <= (int)replacer_size_, "invalid frame id");

  if (hash_.find(frame_id) == hash_.end()) {  // not found, return
    LOG_DEBUG("set evictable failed not found %d", frame_id);
    return;
  }

  auto it = hash_[frame_id];

  // LOG_DEBUG("before :frame_id is %d, set_evictable is %d, evictable_ is %d, curr_size_ is %zu", frame_id, set_evictable, it->evictable_, curr_size_);

  if (!it->evictable_) {
    if (set_evictable) {
      it->evictable_ = set_evictable;
      curr_size_++;
    }
  } else {
    if (!set_evictable) {
      it->evictable_ = set_evictable;
      curr_size_--;
    }
  }

  // LOG_DEBUG("after :frame_id is %d, set_evictable is %d, evictable_ is %d, curr_size_ is %zu", frame_id, set_evictable, it->evictable_, curr_size_);

}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  RemoveInternal(frame_id);
}

void LRUKReplacer::RemoveInternal(frame_id_t frame_id) {
  if (hash_.find(frame_id) == hash_.end()) {  // not found, return
    return;
  }

  auto it = hash_[frame_id];

  BUSTUB_ASSERT(it->evictable_, "remove an unevictable page");

  if (it->access_count_ >= k_) {
    countable_list_.erase(it);
  } else {
    inf_list_.erase(it);
  }

  hash_.erase(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
