//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::GetNewPage(page_id_t page_id) -> page_id_t {
  frame_id_t frame_id = -1;
  Page *page = nullptr;

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Size() > 0) {
    replacer_->Evict(&frame_id);
  } else {
    return INVALID_PAGE_ID;
  }

  page = &pages_[frame_id];

  if (page_id == INVALID_PAGE_ID) {
    page_id = AllocatePage();
  } else {
  }

  if (page->IsDirty()) {
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
  }

  page_table_->Remove(page->GetPageId());

  page->is_dirty_ = false;
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->ResetMemory();

  page_table_->Insert(page->GetPageId(), frame_id);

  replacer_->SetEvictable(frame_id, false);
  replacer_->RecordAccess(frame_id);

  return page_id;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  *page_id = GetNewPage(INVALID_PAGE_ID);

  if (*page_id == INVALID_PAGE_ID) {
    return nullptr;
  }

  frame_id_t frame_id = -1;
  page_table_->Find(*page_id, frame_id);

  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id = -1;

  Page *page = nullptr;

  if (!page_table_->Find(page_id, frame_id)) {  // if not found, new one
    page_id = GetNewPage(page_id);
    if (page_id == INVALID_PAGE_ID) {  // if new one failed
      return nullptr;
    }
  }

  page_table_->Find(page_id, frame_id);
  page = &pages_[frame_id];

  disk_manager_->ReadPage(page_id, page->data_);

  return page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id = -1;

  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  Page *page = &pages_[frame_id];

  if (page->GetPinCount() <= 0) {
    return false;
  }

  page->is_dirty_ |= is_dirty;

  if (--page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id = -1;

  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  Page *page = &pages_[frame_id];

  disk_manager_->WritePage(page_id, page->GetData());

  page->is_dirty_ = false;

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);

  for (size_t i = 0; i < pool_size_; ++i) {
    Page *page = &pages_[i];
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
    page->is_dirty_ = false;
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id = -1;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }

  Page *page = &pages_[frame_id];

  if (page->GetPinCount() > 0) {
    return false;
  }

  // page_table_ remove
  page_table_->Remove(page_id);

  // replacer_ remove
  replacer_->Remove(frame_id);

  // add to free_list;
  free_list_.push_back(frame_id);

  // reset data;
  page->is_dirty_ = false;
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->ResetMemory();

  // deallocate
  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
