//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <string>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 12
#define INTERNAL_PAGE_SIZE ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(MappingType)))
/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  // Deleted to disallow initialization
  BPlusTreeInternalPage() = delete;
  BPlusTreeInternalPage(const BPlusTreeInternalPage &other) = delete;

  /**
   * Writes the necessary header information to a newly created page, must be
   * called after the creation of a new page to make a valid
   * BPlusTreeInternalPage
   * @param max_size Maximal size of the page
   */
  void Init(int max_size = INTERNAL_PAGE_SIZE);

  /**
   * @param index The index of the key to get. Index must be non-zero.
   * @return Key at index
   */
  auto KeyAt(int index) const -> KeyType;

  /**
   *
   * @param index The index of the key to set. Index must be non-zero.
   * @param key The new value for key
   */
  void SetKeyAt(int index, const KeyType &key);
  void SetValueAt(int index, const ValueType &value);
  void InsertKeyAndValueAt(int index, const KeyType &key, const ValueType &value);
  void DeleteKeyAndValueAt(int index);

  /**
   *
   * @param value the value to search for
   */
  auto ValueIndex(const ValueType &value) const -> int;

  /**
   *
   * @param index the index
   * @return the value at the index
   */
  auto ValueAt(int index) const -> ValueType;

  /**
   * @brief For test only, return a string representing all keys in
   * this internal page, formatted as "(key1,key2,key3,...)"
   *
   * @return std::string
   */
  auto ToString() const -> std::string {
    std::string kstr = "(";
    bool first = true;

    // first key of internal page is always invalid
    for (int i = 1; i < GetSize(); i++) {
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
    int start = i;
    int end = GetSize() - 1;
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

  auto GetIndexEqualToKey(int &i, const KeyType key, KeyComparator comporator) const -> bool {
    int start = i;
    int end = GetSize() - 1;
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

  auto GetIndexEqualToValue(int i, const page_id_t value) const -> int {
    // solution 1
    int end = GetSize();
    for (; i < end; ++i) {
      if (value == array_[i].second) {
        return i;
      }
    }
    return -1;
  }

  auto MemMove(B_PLUS_TREE_INTERNAL_PAGE_TYPE *src, int src_idx, int dest_idx, int len) {
    std::memmove(reinterpret_cast<char *>(&array_[dest_idx]), reinterpret_cast<char *>(&src->array_[src_idx]),
                 len * sizeof(array_[0]));
  }

 private:
  // Flexible array member for page data.
  MappingType array_[0];
};
}  // namespace bustub
