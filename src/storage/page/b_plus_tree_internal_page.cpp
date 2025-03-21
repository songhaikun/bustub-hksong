//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  if (max_size > 255 || max_size < 0) {
    LOG_DEBUG("page size should less than 256");
    BUSTUB_ASSERT(1, "error occured");
  }
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(1);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertKeyAndValueAt(int index, const KeyType &key, const ValueType &value) {
  if (index >= 1 && index <= GetSize() && GetSize() < GetMaxSize()) {
    std::memmove(reinterpret_cast<char *>(&array_[index + 1]), reinterpret_cast<char *>(&array_[index]),
                 (GetSize() - index) * sizeof(array_[0]));
    IncreaseSize(1);
  } else if (index >= 1 && index < GetSize() && GetSize() == GetMaxSize()) {
    std::memmove(reinterpret_cast<char *>(&array_[index + 1]), reinterpret_cast<char *>(&array_[index]),
                 (GetSize() - index - 1) * sizeof(array_[0]));
  }
  array_[index].first = key;
  array_[index].second = value;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteKeyAndValueAt(int index) {
  if (index >= 0 && index < GetSize()) {
    if (GetSize() != INTERNAL_PAGE_SIZE) {
      std::memmove(reinterpret_cast<char *>(&array_[index]), reinterpret_cast<char *>(&array_[index + 1]),
                   (GetSize() - index - 1) * sizeof(array_[0]));
    } else {
      std::memmove(reinterpret_cast<char *>(&array_[index]), reinterpret_cast<char *>(&array_[index + 1]),
                   (GetSize() - index - 2) * sizeof(array_[0]));
    }
    IncreaseSize(-1);
  }
}
/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  return array_[index].second;  // TODO(hksong): may need be changed to index + 1
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
