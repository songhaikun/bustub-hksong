#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept
    : bpm_(that.bpm_), page_(that.page_), is_dirty_(that.is_dirty_) {
  that.bpm_ = nullptr;
  that.page_ = nullptr;
}

void BasicPageGuard::Drop() {
  // pre check
  if (nullptr == bpm_ || nullptr == page_) {
    return;
  }
  // Unpin the page
  bpm_->UnpinPage(PageId(), is_dirty_);
  // TODO(hksong): unlatch page
  // clear the page guard
  bpm_ = nullptr;
  page_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this != &that) {
    // clear this
    this->Drop();
    // copy member variables
    this->bpm_ = that.bpm_;
    this->page_ = that.page_;
    this->is_dirty_ = that.is_dirty_;
    // clear that
    that.bpm_ = nullptr;
    that.page_ = nullptr;
  }
  return *this;
}

BasicPageGuard::~BasicPageGuard() { this->Drop(); };  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default;

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    this->Drop();
    this->guard_ = std::move(that.guard_);
  }
  return *this;
}

void ReadPageGuard::Drop() {
  // pre check
  if (nullptr == guard_.bpm_ || nullptr == guard_.page_) {
    return;
  }
  // TODO(hksong): unlatch page
  guard_.page_->RUnlatch();
  // Unpin the page
  guard_.bpm_->UnpinPage(PageId(), guard_.is_dirty_);
  // clear the page guard
  guard_.bpm_ = nullptr;
  guard_.page_ = nullptr;
}

ReadPageGuard::~ReadPageGuard() { this->Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    this->Drop();
    this->guard_ = std::move(that.guard_);
  }
  return *this;
}

void WritePageGuard::Drop() {
  // pre check
  if (nullptr == guard_.bpm_ || nullptr == guard_.page_) {
    return;
  }
  // TODO(hksong): unlatch page
  guard_.page_->WUnlatch();
  // Unpin the page
  guard_.bpm_->UnpinPage(PageId(), guard_.is_dirty_);
  // clear the page guard
  guard_.bpm_ = nullptr;
  guard_.page_ = nullptr;
}

WritePageGuard::~WritePageGuard() { this->Drop(); }  // NOLINT

}  // namespace bustub
