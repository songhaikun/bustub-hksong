//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>
INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  // you may define your own constructor based on your member variables
  IndexIterator(int idx, int max_idx_per_page,  long long uuid, BufferPoolManager *bpm, LeafPage *start_page);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return (itr.uuid_ == this->uuid_) && ((this->is_end_page_ && itr.is_end_page_) || (itr.cur_page_ == this->cur_page_ && itr.idx_ == this->idx_));
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    if (itr.uuid_ != this->uuid_) {
      return true;
    }
    if (itr.is_end_page_!=this->is_end_page_) {
      return true;
    }
    if (itr.is_end_page_) {
      return false;
    }
    return itr.cur_page_ != this->cur_page_ || itr.idx_ != this->idx_;
  }

 private:
  // add your own private member variables here
  // pointer 
  int idx_;
  int max_idx_per_page_;
  long long uuid_;
  BufferPoolManager *bpm_;
  LeafPage* cur_page_;
  bool is_end_page_{false};
   
};

}  // namespace bustub
