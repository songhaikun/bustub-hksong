//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);
  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::GetUsablePage(frame_id_t *frame_id, std::unique_lock<std::mutex> &lock) -> bool {
  if (replacer_ == nullptr || disk_manager_ == nullptr || (free_list_.empty() && replacer_->Size() == 0)) {
    return false;
  }
  // get pages from the free list
  Page *pg = nullptr;
  if (!free_list_.empty()) {
    // get new page_id, record frame id in lru_replacer
    *frame_id = free_list_.front();
    free_list_.pop_front();
    pg = GetPages() + *frame_id;
  } else if (replacer_->Evict(frame_id)) {  // get pages by replacer
    pg = GetPages() + *frame_id;
    if (pg->IsDirty()) {
      disk_manager_->WritePage(pg->GetPageId(), pg->GetData());
    }
    page_table_.erase(pg->GetPageId());
  } else {
    return false;
  }
  // write back dirty page

  pg->pin_count_ = 1;  // TODO(hksong): may need change later
  pg->is_dirty_ = false;
  pg->ResetMemory();
  replacer_->RecordAccess(*frame_id);
  replacer_->SetEvictable(*frame_id, false);
  return true;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  // init with big lock
  std::unique_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = 0;
  if (!GetUsablePage(&frame_id, lock)) {
    // page_id = nullptr;
    return nullptr;
  }
  Page *pg = GetPages() + frame_id;
  // set page metadata record page
  *page_id = AllocatePage();
  pg->page_id_ = *page_id;
  page_table_[*page_id] = frame_id;  // record it
  lock.unlock();
  // return page address
  BUSTUB_ASSERT(pg->pin_count_ == 1 && pg->is_dirty_ == false && pg->page_id_ != INVALID_PAGE_ID,
                "newpage: error new page");
  return pg;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  // init with big lock
  std::unique_lock<std::mutex> lock(latch_);
  auto iter = page_table_.find(page_id);
  // can get pages directly from pool
  if (iter != page_table_.end()) {
    // bpin
    frame_id_t frame_id = iter->second;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    Page *pg = GetPages() + frame_id;
    pg->pin_count_++;
    return pg;
  }
  // need get it from other place(disk)
  frame_id_t frame_id = -1;
  if (!GetUsablePage(&frame_id, lock)) {
    return nullptr;
  }
  BUSTUB_ASSERT(frame_id != -1, "fetchpage: wrong frame_id");
  Page *pg = GetPages() + frame_id;
  pg->page_id_ = page_id;
  // read from disk
  disk_manager_->ReadPage(page_id, pg->GetData());
  page_table_[page_id] = frame_id;
  lock.unlock();
  BUSTUB_ASSERT(pg->pin_count_ == 1 && pg->is_dirty_ == false && pg->page_id_ != INVALID_PAGE_ID,
                "fetchpage: error get page");
  return pg;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  // init with big lock
  std::unique_lock<std::mutex> lock(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = iter->second;
  Page *pg = GetPages() + frame_id;
  if (pg->GetPinCount() <= 0) {
    return false;
  }
  pg->pin_count_--;
  if (!pg->IsDirty()) {
    pg->is_dirty_ = is_dirty;
  }
  if (0 == pg->GetPinCount()) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> lock(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  Page *pg = GetPages() + iter->second;
  disk_manager_->WritePage(pg->GetPageId(), pg->GetData());
  pg->is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::unique_lock<std::mutex> lock(latch_);
  for (auto &iter : page_table_) {
    Page *pg = GetPages() + iter.second;
    disk_manager_->WritePage(pg->GetPageId(), pg->GetData());
    pg->is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  // init with big lock
  std::unique_lock<std::mutex> lock(latch_);
  auto iter = page_table_.find(page_id);
  // not in pool
  if (iter == page_table_.end()) {
    if (removed_pages_index_.find(page_id) == removed_pages_index_.end() && page_id < next_page_id_) {
      removed_pages_.push_front(page_id);
      removed_pages_index_.insert(page_id);
    }
    return true;
  }
  // in pool
  frame_id_t frame_id = iter->second;
  Page *pg = GetPages() + frame_id;
  if (pg->GetPinCount() != 0) {
    return false;
  }
  // write back
  if (pg->is_dirty_) {
    BUSTUB_ASSERT(page_id == pg->GetPageId(), "pageid is wrong");
    disk_manager_->WritePage(page_id, pg->GetData());
    pg->is_dirty_ = false;
  }
  replacer_->Remove(frame_id);
  pg->ResetMemory();
  pg->page_id_ = INVALID_PAGE_ID;
  BUSTUB_ASSERT(pg->GetPageId() == INVALID_PAGE_ID && pg->GetPinCount() == 0 && pg->is_dirty_ == false,
                "deletepage: delete wrong");
  page_table_.erase(page_id);
  removed_pages_index_.insert(page_id);
  removed_pages_.push_front(page_id);
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t {
  if (!removed_pages_.empty()) {
    page_id_t page_id = removed_pages_.back();
    removed_pages_index_.erase(page_id);
    removed_pages_.pop_back();
    return page_id;
  }
  return next_page_id_++;
}

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  Page *p;
  if (nullptr == (p = FetchPage(page_id))) {
    return {this, nullptr};
  }
  return {this, p};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *p;
  if (nullptr == (p = FetchPage(page_id))) {
    return {this, nullptr};
  }
  p->RLatch();
  return {this, p};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *p;
  if (nullptr == (p = FetchPage(page_id))) {
    return {this, nullptr};
  }
  p->WLatch();
  return {this, p};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page *p;
  if (nullptr == (p = NewPage(page_id))) {
    return {this, nullptr};
  }
  return {this, p};
}

}  // namespace bustub
