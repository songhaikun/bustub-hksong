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
#include <ctime>
#include <shared_mutex>
#include "common/exception.h"

namespace bustub {

LRUKNode::LRUKNode(size_t k, frame_id_t fid) : k_(k), fid_(fid) {}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  young_list_ = node_list_.end();
}

// insert history
auto LRUKNode::PushHistory(size_t time_val) -> bool {
  if (history_.empty()) {
    history_.push_back(time_val);
    history_size_++;
    return true;
  }
  size_t old_time = history_.front();
  if (old_time > time_val) {
    return false;
  }
  history_.push_back(time_val);
  history_size_++;
  if (history_size_ > k_) {
    history_.pop_front();
    history_size_--;
  }
  return true;
}

auto LRUKNode::GetBackwardK(double &bk, size_t timeval) -> bool {
  if (history_.empty() || history_.front() > timeval) {
    return false;
  }
  if (history_size_ < k_) {
    bk = INFINITY;
    return true;
  }
  bk = timeval - history_.front();
  return true;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  // init with big lock
  std::unique_lock<std::mutex> lock(latch_);
  if (0 == curr_size_) {
    return false;
  }
  // find in young list
  if (young_list_ != node_list_.end()) {
    for (auto nodeidx = young_list_; nodeidx != node_list_.end(); ++nodeidx) {
      if (nodeidx->GetIsEvictable()) {
        *frame_id = nodeidx->GetFrameId();
        // clean the record and change current size;
        curr_size_--;
        auto it_temp = nodeidx;
        if (young_list_ == nodeidx) {
          young_list_++;
        }
        node_store_.erase(*frame_id);
        node_list_.erase(it_temp);

        return true;
      }
    }
  }
  // find in ori list
  for (auto nodeidx = node_list_.begin(); nodeidx != node_list_.end(); ++nodeidx) {
    if (nodeidx->GetIsEvictable()) {
      *frame_id = nodeidx->GetFrameId();
      // clean the record and change current size;
      curr_size_--;
      auto it_temp = nodeidx;
      node_store_.erase(*frame_id);
      node_list_.erase(it_temp);
      return true;
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  // init with a big lock
  std::unique_lock<std::mutex> lock(latch_);
  auto it = node_store_.find(frame_id);
  // make and insert node
  time_t t = ::time(nullptr);
  tm *cur_time = localtime(&t);
  current_timestamp_ = cur_time->tm_gmtoff;
  if (node_store_.end() == it) {
    // check if can add this entry
    bool expr = (frame_id <= static_cast<int>(replacer_size_));
    BUSTUB_ASSERT(expr, "replacer if full");
    node_list_.emplace_back(k_, frame_id);
    auto iter = std::prev(node_list_.end());
    if (node_list_.end() == young_list_) {
      young_list_ = iter;
    }
    iter->PushHistory(current_timestamp_);
    node_store_[frame_id] = iter;
    return;
  }

  // change node
  auto iter = it->second;
  iter->PushHistory(current_timestamp_);

  // change list in old list
  if (iter->GetHistorySize() == k_) {
    if (iter == young_list_) {
      young_list_++;
      return;
    }
    // iter 插入young_list_之前（old最后）
    node_list_.splice(young_list_, node_list_, iter);
    return;
  }

  // change list in young list, iter must in young list
  // not only one in young list, put iter
  if (young_list_ != std::prev(node_list_.end())) {
    if (young_list_ == iter) {
      young_list_++;
    }
    node_list_.splice(node_list_.end(), node_list_, iter);
  }
  // only one in young list, return
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  // init with a big lock
  std::unique_lock<std::mutex> lock(latch_);
  // if frame_id is not valid, panic
  auto it = node_store_.find(frame_id);
  bool expr = (node_store_.end() != it);
  BUSTUB_ASSERT(expr, "frame id doens't exist");
  auto iter = it->second;
  if (set_evictable) {
    if (iter->GetIsEvictable()) {
      return;  // do nothing
    }
    iter->SetIsEvictable(true);
    curr_size_++;
  } else {
    if (!iter->GetIsEvictable()) {
      return;
    }
    iter->SetIsEvictable(false);
    curr_size_--;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  // init with a big lock
  std::unique_lock<std::mutex> lock(latch_);
  auto it = node_store_.find(frame_id);
  if (node_store_.end() == it) {
    return;
  }
  auto iter = it->second;
  if (!iter->GetIsEvictable()) {
    throw("error remove: remove the non-evictable frame");
    return;
  }
  node_store_.erase(it);

  if (young_list_ == iter) {
    young_list_++;
  }
  node_list_.erase(iter);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
