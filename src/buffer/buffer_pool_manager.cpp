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

#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::GetUsablePage(frame_id_t *frame_id) -> bool {
  // no lock, regard this
  // check if can allocate page
  if (replacer_ == nullptr || disk_scheduler_ == nullptr || (free_list_.empty() && replacer_->Size() == 0)) {
    return false;
  }
  // get pages from the free list
  if (!free_list_.empty()) {
    // get new page_id, record frame id in lru_replacer
    *frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Evict(frame_id)) {  // get pages by replacer
    return false;
  }
  Page *pg = GetPages() + *frame_id;
  // write back dirty page
  if (pg->IsDirty()) {
    auto promise_t = disk_scheduler_->CreatePromise();
    auto future_t = promise_t.get_future();
    disk_scheduler_->Schedule({true, pg->GetData(), pg->GetPageId(), std::move(promise_t)});
    bool expr = future_t.get();
    BUSTUB_ASSERT(expr, "write disk failed");
    // truncate the page
    page_table_.erase(pg->GetPageId());
    pg->ResetMemory();
  }
  pg->is_dirty_ = false;
  pg->pin_count_ = 1;  // TODO(hksong): may need change later
  replacer_->RecordAccess(*frame_id);
  replacer_->SetEvictable(*frame_id, false);
  return true;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  // init with big lock
  std::unique_lock<std::mutex> bflock(pool_latch_);
  frame_id_t frame_id = 0;
  if (!GetUsablePage(&frame_id)) {
    page_id = nullptr;
    return nullptr;
  }
  Page *pg = GetPages() + frame_id;
  // set page metadata record page
  *page_id = AllocatePage();
  pg->page_id_ = *page_id;
  page_table_[*page_id] = frame_id;  // record it
  // return page address
  return pg;
}

// 存在的问题: 异常流处理
auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  // init with big lock
  std::unique_lock<std::mutex> bflock(pool_latch_);
  auto iter = page_table_.find(page_id);
  // can get pages directly from pool
  if (iter != page_table_.end()) {
    // bpin
    frame_id_t frame_id = iter->second;
    replacer_->RecordAccess(frame_id);
    return GetPages() + frame_id;
  }
  // need get it from other place(disk)
  frame_id_t frame_id = 0;
  if (!GetUsablePage(&frame_id)) {
    return nullptr;
  }
  Page *pg = GetPages() + frame_id;
  pg->page_id_ = page_id;
  // read from disk
  auto promise_t = disk_scheduler_->CreatePromise();
  auto future_t = promise_t.get_future();
  disk_scheduler_->Schedule({false, pg->GetData(), page_id, std::move(promise_t)});
  // 异步处理，need change
  bool expr = future_t.get();
  if (expr) {
    // read success
    page_table_[page_id] = frame_id;
    return GetPages() + frame_id;
  }

  // TODO(hksong): this page not in disk, should fix it carefully
  // LOG_DEBUG("page not found\n");
  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  // init with big lock
  std::unique_lock<std::mutex> bflock(pool_latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    // LOG_DEBUG("iter == page_table_.end\n");
    return false;
  }
  frame_id_t frame_id = iter->second;
  Page *pg = GetPages() + frame_id;
  if (pg->GetPinCount() == 0) {
    // LOG_DEBUG("PinCount = 0\n");
    return false;
  }
  pg->pin_count_--;
  pg->is_dirty_ = is_dirty;
  if (0 == pg->GetPinCount()) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> bflock(pool_latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  Page *pg = GetPages() + iter->second;
  auto promise_t = disk_scheduler_->CreatePromise();
  auto future_t = promise_t.get_future();
  disk_scheduler_->Schedule({true, pg->GetData(), page_id, std::move(promise_t)});
  return future_t.get();
}

void BufferPoolManager::FlushAllPages() {
  for (auto &iter : page_table_) {
    FlushPage(iter.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  // init with big lock
  std::unique_lock<std::mutex> bflock(pool_latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return true;
  }

  frame_id_t frame_id = iter->second;
  Page *pg = GetPages() + frame_id;
  if (pg->GetPinCount() > 0) {
    return false;
  }

  replacer_->Remove(frame_id);
  pg->ResetMemory();
  page_table_.erase(page_id);
  free_list_.push_back(frame_id);

  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

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
