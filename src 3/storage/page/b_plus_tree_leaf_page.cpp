//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set
 * max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  if (max_size > 255 || max_size < 0) {
    LOG_DEBUG("page size should less than 256");
    BUSTUB_ASSERT(1, "error occured");
  }
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  next_page_id_ = INVALID_PAGE_ID;
  SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  array_[index].second = value;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertKeyAndValueAt(int index, const KeyType &key, const ValueType &value) {
  if (index >= 0 && index <= GetSize() && GetSize() < GetMaxSize()) {
    std::memmove(&array_[index + 1], &array_[index], (GetSize() - index) * sizeof(array_[0]));
    IncreaseSize(1);
  } else if(index >= 0 && index < GetSize() && GetSize() == GetMaxSize()) {
    std::memmove(&array_[index + 1], &array_[index], (GetSize() - index - 1) * sizeof(array_[0]));
  }
  array_[index].first = key;
  array_[index].second = value;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::DeleteKeyAndValueAt(int index){
  if (index >= 0 && index < GetSize()) {
    if (GetSize() != LEAF_PAGE_SIZE) {
      std::memmove(&array_[index], &array_[index + 1], (GetSize() - index - 1) * sizeof(array_[0]));
    } else {
      std::memmove(&array_[index], &array_[index + 1], (GetSize() - index - 2) * sizeof(array_[0]));
    }
    IncreaseSize(-1);
  }
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
