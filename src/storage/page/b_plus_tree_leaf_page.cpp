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
  try {
    if (index >= 255 || index < 0) {
      std::cout << "leaf page: key at error" << std::endl;
    }
    return array_[index].first;
  } catch(std::exception &e) {
    std::cout << e.what() << "leaf page: key at error" << std::endl;
    throw e;
  }
  return KeyType();
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  try {
    if (index >= 255 || index < 0) {
      std::cout << "leaf page: value at error" << std::endl;
    }
    return array_[index].second;
  } catch(std::exception &e) {
    std::cout << e.what() << "leaf page: value at error" << std::endl;
    throw e;
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  try {
    if (index >= 255 || index < 0) {
      array_[index].first = key;
  }
  } catch(std::exception &e) {
    std::cout << e.what() << "leaf page: set key at error" << std::endl;
    throw e;
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  try {
    if (index >= 255 || index < 0) {
      array_[index].second = value;
    }
  } catch(std::exception &e) {
    std::cout << e.what() << "leaf page: set value at error" << std::endl;
    throw e;
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertKeyAndValueAt(int index, const KeyType &key, const ValueType &value) {
  try{
    if (index >= 0 && index <= GetSize() && GetSize() < GetMaxSize()) {
      for (int i = GetSize() - 1; i >= index; --i) {
        array_[i + 1] = array_[i];
      }
      array_[index].first = key;
      array_[index].second = value;
      IncreaseSize(1);
    } else if(index >= 0 && index < GetSize() && GetSize() == GetMaxSize()) {
      for (int i = GetSize() - 2; i >= index; --i) {
        array_[i + 1] = array_[i];
      }
      array_[index].first = key;
      array_[index].second = value;
    }
  } catch(std::exception &e) {
    std::cout << e.what() << "leaf page: insert key and value at error" << std::endl;
    throw e;
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::DeleteKeyAndValueAt(int index){
  try {
  if (index >= 0 && index < GetSize()) {
    if  (GetSize() != LEAF_PAGE_SIZE) {
      for (int i = index; i < GetSize(); ++i) {
        array_[i] = array_[i + 1];
      }
    } else {
      for (int i = index; i < GetSize() - 1; ++i) {
        array_[i] = array_[i + 1];
      }
    }
    IncreaseSize(-1);
  }
  } catch(std::exception &e) {
    std::cout << e.what() << "leaf page: delete key and value at error" << std::endl;
    throw e;
  }
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
