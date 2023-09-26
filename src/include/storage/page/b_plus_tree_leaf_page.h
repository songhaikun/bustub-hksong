//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_leaf_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <string>
#include <utility>
#include <vector>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>
#define LEAF_PAGE_HEADER_SIZE 16
#define LEAF_PAGE_SIZE ((BUSTUB_PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / sizeof(MappingType))

/**
 * Store indexed key and record id(record id = page id combined with slot id,
 * see include/common/rid.h for detailed implementation) together within leaf
 * page. Only support unique key.
 *
 * Leaf page format (keys are stored in order):
 *  ----------------------------------------------------------------------
 * | HEADER | KEY(1) + RID(1) | KEY(2) + RID(2) | ... | KEY(n) + RID(n)
 *  ----------------------------------------------------------------------
 *
 *  Header format (size in byte, 16 bytes in total):
 *  ---------------------------------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) |
 *  ---------------------------------------------------------------------
 *  -----------------------------------------------
 * |  NextPageId (4)
 *  -----------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeLeafPage : public BPlusTreePage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  BPlusTreeLeafPage() = delete;
  BPlusTreeLeafPage(const BPlusTreeLeafPage &other) = delete;

  /**
   * After creating a new leaf page from buffer pool, must call initialize
   * method to set default values
   * @param max_size Max size of the leaf node
   */
  void Init(int max_size = LEAF_PAGE_SIZE);

  // helper methods
  auto GetNextPageId() const -> page_id_t;
  void SetNextPageId(page_id_t next_page_id);
  auto KeyAt(int index) const -> KeyType;
  auto ValueAt(int index) const -> ValueType;
  void SetKeyAt(int index, const KeyType &key);
  void SetValueAt(int index, const ValueType &value);
  void InsertKeyAndValueAt(int index, const KeyType &key, const ValueType &value);
  void DeleteKeyAndValueAt(int index);
  /**
   * @brief for test only return a string representing all keys in
   * this leaf page formatted as "(key1,key2,key3,...)"
   *
   * @return std::string
   */
  auto ToString() const -> std::string {
    std::string kstr = "(";
    bool first = true;

    for (int i = 0; i < GetSize(); i++) {
      KeyType key = KeyAt(i);
      if (first) {
        first = false;
      } else {
        kstr.append(",");
      }

      kstr.append(std::to_string(key.ToString()));
    }
    kstr.append(")");

    return kstr;
  }

  auto GetIndexLargerThanKey(int i, const KeyType key, KeyComparator comporator) const -> int {
    int start = i, end = GetSize() - 1;
    while (start <= end) {
      int mid_idx = start + (end - start) / 2;
      KeyType mid_key = array_[mid_idx].first;
      int cmp = comporator(mid_key, key);
      if (cmp < 0) {
        start = mid_idx + 1;
      } else if (cmp > 0) {
        end = mid_idx - 1;
      } else {
        return mid_idx + 1;
      }
    }
    return start;
  }

  auto GetIndexEqualToKey(int& i, const KeyType key, KeyComparator comporator) const -> bool {
    int start = i, end = GetSize() - 1;
    while (start <= end) {
      int mid_idx = start + (end - start) / 2;
      KeyType mid_key = array_[mid_idx].first;
      int cmp = comporator(mid_key, key);
      if (cmp < 0) {
        start = mid_idx + 1;
      } else if (cmp > 0) {
        end = mid_idx - 1;
      } else {
        i = mid_idx;
        return true;
      }
    }
    return false;
  }

  // auto GetIndexEqualToValue(int& i, const ValueType value) const -> bool {
  //   int start = i, end = GetSize() - 1;
  //   while (start <= end) {
  //     int mid_idx = start + (end - start) / 2;
  //     ValueType mid_value = array_[mid_idx].second;
  //     // int cmp = mid_key - value;
  //     if (mid_value < value) {
  //       start = mid_idx + 1;
  //     } else if (mid_value > value) {
  //       end = mid_idx - 1;
  //     } else {
  //       i = mid_idx;
  //       return true;
  //     }
  //   }
  //   return false;
  // }

 private:
  page_id_t next_page_id_;
  // Flexible array member for page data.
  MappingType array_[0];
};
}  // namespace bustub
