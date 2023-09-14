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
#include <shared_mutex>
#include <ctime>
#include "common/exception.h"

namespace bustub {

LRUKNode::LRUKNode(size_t k, frame_id_t fid) : k_(k), fid_(fid) {}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
    young_list_ = node_list_.end();
}

// insert history
auto LRUKNode::PushHistory(size_t time_val) -> bool {
    if(history_.empty()){
        history_.push_back(time_val);
        history_size_++;
        return true;
    }
    size_t old_time = history_.front();
    if(old_time > time_val) {
        return false;
    }
    history_.push_back(time_val);
    history_size_++;
    if(history_size_ > k_) {
        history_.pop_front();
        history_size_--;
    }
    return true;
}

auto LRUKNode::GetBackwardK(double& bk, size_t timeval) -> bool {
    if(history_.empty() || history_.front() > timeval) {
        return false;
    }
    if(history_size_ < k_) {
        bk = INFINITY;
        return true;
    }
    bk = timeval - history_.front();
    return true;
}

//  std::list<LRUKNode> node_list_;
//  std::unordered_map<frame_id_t, std::list<LRUKNode>::iterator> node_store_;
//  std::list<LRUKNode>::iterator young_list_;
//  size_t current_timestamp_{0};
//  size_t curr_size_{0};
//  size_t replacer_size_;
//  size_t k_;
//  std::mutex latch_;
//  std::mutex young_latch_;

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool { 
    // init with big lock
    std::unique_lock<std::mutex> lock(latch_);
    if(0 == curr_size_) {
        return false;
    }
    // find in young list
    if(young_list_ != node_list_.end()) {
        for(auto nodeidx = young_list_; nodeidx != node_list_.end(); ++nodeidx) {
            if(nodeidx->GetIsEvictable()) {
                *frame_id = nodeidx->GetFrameId();
                // clean the record and change current size;
                curr_size_--;
                auto it_temp = nodeidx;
                if(young_list_ == nodeidx) {
                    young_list_++;
                }
                node_store_.erase(*frame_id);
                node_list_.erase(it_temp);
                
                return true;
            }
        }
    }
    //find in ori list
    for(auto nodeidx = node_list_.begin(); nodeidx != node_list_.end(); ++nodeidx) {
        if(nodeidx->GetIsEvictable()) {
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
    if(node_store_.end() == it) {
        //check if can add this entry
        bool expr = curr_size_ < replacer_size_;
        BUSTUB_ASSERT(expr, "replacer if full");

        node_list_.emplace_back(k_, frame_id);
        auto iter = std::prev(node_list_.end());
        if(node_list_.end() == young_list_) {
            young_list_ = iter;
        }
        node_store_[frame_id] = iter;
        curr_size_++;
        return;
    }

    // change node
    auto iter = it->second;
    time_t t = ::time(nullptr);
    tm *cur_time = localtime(&t);
    size_t cur_time_sz = cur_time->tm_gmtoff;
    iter->PushHistory(cur_time_sz);

    // change list in old list
    if(iter->GetHistorySize() == k_){
        if(iter == young_list_) {
            young_list_++;
            return;
        }
        // iter 插入young_list_之前（old最后）
        node_list_.splice(young_list_, node_list_, iter);
        return;
    }

    // change list in young list, iter must in young list
    // not only one in young list, put iter 
    if(young_list_ != std::prev(node_list_.end())) {
        if(young_list_ == iter) {
            young_list_++;
        }
        node_list_.splice(node_list_.end(), node_list_, iter);
    }
    // only one in young list, return

}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {

}

void LRUKReplacer::Remove(frame_id_t frame_id) {

}

auto LRUKReplacer::Size() -> size_t { 
    return 0; 
}

}  // namespace bustub
