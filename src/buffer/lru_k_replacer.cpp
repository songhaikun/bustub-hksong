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
#include "common/exception.h"
#include <mach/policy.h>

namespace bustub {
LRUKNode::LRUKNode(size_t k, frame_id_t fid) : k_(k), fid_(fid) {}

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


LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
    // young_list_ = cache_list_.end();
}

auto LRUKReplacer::EvictInList(std::list<LRUKNode>& list, frame_id_t *frame_id) -> bool {
    // std::unique_lock<std::mutex> lock(latch_);
    for(auto nodeidx = list.begin(); nodeidx != list.end(); ++nodeidx) {
        if(nodeidx->GetIsEvictable()) {
            *frame_id = nodeidx->GetFrameId();
            // clean the record and change current size;
            // lock.lock();
            curr_size_--;
            node_store_.erase(*frame_id);
            // lock.unlock();
            list.erase(nodeidx);
            return true;
        }
    }
    return false;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    std::unique_lock<std::mutex> lock(latch_);
    // std::unique_lock<std::mutex> cache_lock(cache_latch_);
    // std::unique_lock<std::mutex> lock(latch_);
    if(0 == curr_size_) {
        return false;
    }
    // lock.unlock();
    // find in young list
    if(EvictInList(node_list_, frame_id)){
        return true;
    }

    // cache_lock.unlock();
    // find in old list
    return EvictInList(cache_list_, frame_id);
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
    std::unique_lock<std::mutex> lock(latch_);
    // std::unique_lock<std::mutex> cache_lock(cache_latch_);
    // std::unique_lock<std::mutex> lock(latch_);
    // time_t t = ::time(nullptr);
    // tm *cur_time = localtime(&t);
    bool expr = (frame_id <= static_cast<int>(replacer_size_));
    BUSTUB_ASSERT(expr, "replacer too big");
    auto it = node_store_.find(frame_id);
    current_timestamp_++;
    // make and insert node
    if(node_store_.end() == it) {
        node_list_.emplace_back(k_, frame_id);
        auto iter = std::prev(node_list_.end());
        iter->PushHistory(current_timestamp_);
        node_store_[frame_id] = {iter, InWhichList::CLIST};
        return;
    }

    auto iter = it->second.first;
    size_t sz = iter->GetHistorySize();
    iter->PushHistory(current_timestamp_);
    //change and insert in node list
    if(sz < k_ - 1){
        node_list_.splice(node_list_.end(), node_list_, iter);
        return;
    }

    // change and insert in cache_list_
    // lock.unlock();
    // insert it in cache_list_
    double bw_k = 0.0;
    double bw_k_t = 0.0;
    BUSTUB_ASSERT(iter->GetBackwardK(bw_k, current_timestamp_), "get bk error");
    std::list<LRUKNode>::iterator it_insert = cache_list_.begin();
    for(auto it_ori_r = cache_list_.rbegin(); it_ori_r != cache_list_.rend(); it_ori_r++) {
        BUSTUB_ASSERT(it_ori_r->GetBackwardK(bw_k_t, current_timestamp_), "get bk error");
        if(bw_k <= bw_k_t) {
            it_insert = it_ori_r.base();
            break;
        }
    }
    double dt = 0;
    std::list<LRUKNode>& list = (sz == k_ - 1) ? node_list_ : cache_list_;
    cache_list_.splice(it_insert, list, iter);
    for(auto i : cache_list_) { 
        i.GetBackwardK(dt ,current_timestamp_);
    }
    if(sz == k_ - 1) {
        node_store_[frame_id] = {iter, InWhichList::NDLIST};
    }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    // init with a big lock    
    std::unique_lock<std::mutex> lock(latch_);
    auto it = node_store_.find(frame_id);
    BUSTUB_ASSERT(it != node_store_.end(), "frame id doesn't exist");

    auto iter = it->second.first;
    if(set_evictable == iter->GetIsEvictable()) {
        return;
    }
    iter->SetIsEvictable(set_evictable);
    curr_size_ += (set_evictable ? 1 : -1);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    // init with a big lock    
    std::unique_lock<std::mutex> lock(latch_);
    auto it = node_store_.find(frame_id);
    if(node_store_.end() == it) {
        return;
    }
    auto iter = it->second.first;
    if(!iter->GetIsEvictable()) {
        throw("error remove: remove the non-evictable frame");
        return;
    }
    std::list<LRUKNode>& list = (it->second.second == InWhichList::NDLIST) ? cache_list_ : node_list_;
    list.erase(iter);
}

auto LRUKReplacer::Size() -> size_t {
    std::unique_lock<std::mutex> lock(latch_);
    return curr_size_; 
}

}  // namespace bustub
