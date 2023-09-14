#include "primer/trie.h"
#include <iostream>
#include <memory>
#include <stack>
#include <string_view>
#include "common/exception.h"
namespace bustub {

// Get the value associated with the given key.
// 1. If the key is not in the trie, return nullptr.
// 2. If the key is in the trie but the type is mismatched, return nullptr.
// 3. Otherwise, return the value.
// You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
// nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
// dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
// Otherwise, return the value.
template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");
  if (nullptr == root_) {
    return nullptr;
  }
  std::shared_ptr<const TrieNode> rt = root_;
  int idx = 0;
  int len = key.size();
  for (; idx < len; ++idx) {
    if (rt->children_.find(key[idx]) == rt->children_.end()) {
      return nullptr;
    }
    rt = rt->children_.at(key[idx]);
  }
  if (!rt->is_value_node_) {
    return nullptr;
  }
  if (nullptr == dynamic_cast<const TrieNodeWithValue<T> *>(rt.get())) {
    return nullptr;
  }
  if (nullptr == dynamic_cast<const TrieNodeWithValue<T> *>(rt.get())->value_) {
    return nullptr;
  }
  const T *t = dynamic_cast<const TrieNodeWithValue<T> *>(rt.get())->value_.get();
  return t;
}

// Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
// throw NotImplementedException("Trie::Put is not implemented.");

// You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
// exists, you should create a new `TrieNodeWithValue`.

/// @brief 根据key构造树，利用原有树的结构，复制原有树的children，仅修改key上的路径
template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  int idx = 0;
  int len = key.size();
  char c = 0;
  // init trie
  std::shared_ptr<Trie> trie_p = std::make_shared<Trie>();
  std::shared_ptr<TrieNode> rt;
  if (0 == len) {
    std::map<char, std::shared_ptr<const TrieNode>> children;
    if (nullptr != root_) {
      children = root_->children_;
    }
    rt = std::make_shared<TrieNodeWithValue<T>>(children, std::make_shared<T>(std::move(value)));
    trie_p->root_ = rt;
    return *trie_p;
  } 
  if (nullptr != root_) {
    rt = std::shared_ptr<TrieNode>(root_->Clone());
  } else {
    rt = std::make_shared<TrieNode>();
  }
  trie_p->root_ = rt;
  // loop and make new trie nodes
  for (; idx < len - 1; ++idx) {
    c = key[idx];
    if (rt->children_.find(c) == rt->children_.end()) {
      rt->children_[c] = std::make_shared<TrieNode>();
    } else {
      rt->children_[c] = std::shared_ptr<TrieNode>(rt->children_[c]->Clone());
    }
    rt = std::const_pointer_cast<TrieNode>(rt->children_[c]);
  }

  // make new trie with value node
  c = key[idx];
  if (rt->children_.find(c) == rt->children_.end()) {
    rt->children_[c] = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
  } else {
    std::map<char, std::shared_ptr<const TrieNode>> children = rt->children_[key[idx]]->children_;
    rt->children_[c] = std::make_shared<TrieNodeWithValue<T>>(children, std::make_shared<T>(std::move(value)));
  }
  return *trie_p;
}

// You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
// you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.

/** @brief 需要判断key是否对应一个value，若不对应，直接退出返回
 *         若对应，判断该节点是否有后续节点
 *         若无后续节点
 *         删除需要删除掉上一个有值节点到代删节点间的节点，即找到上一个有值节点p，erase掉节点对应的hash索引
 *         否则将该节点转为非node节点
 *         上述过程需要在新树中进行，修改也是修改新树节点
 *         实现思路：首先调用get查找，若查无此key，返回一个新树。否则记录last_node，每次进行更新
 */

auto Trie::Remove(std::string_view key) const -> Trie {
  // init data
  std::shared_ptr<Trie> trie_p = std::make_shared<Trie>();
  if (nullptr == root_) {
    return *trie_p;
  }
  int idx = 0;
  int len = key.size();
  std::shared_ptr<TrieNode> rt(root_->Clone());
  trie_p->root_ = rt;
  std::shared_ptr<TrieNode> last_node = rt;  // 记录带删节点之前的一个有值节点
  char last_node_c = key[0];                 // 该节点对应的路径（key[idx]）
  char c = 0;
  // start delete
  for (idx = 0; idx < len - 1; ++idx) {
    c = key[idx];
    if (rt->children_.find(c) == rt->children_.end()) {  // key是错的
      return *trie_p;
    }
    rt->children_[c] = std::shared_ptr<TrieNode>(rt->children_[c]->Clone());  // 节点复制
    rt = std::const_pointer_cast<TrieNode>(rt->children_[c]);                 // 迭代子节点
    if (rt->is_value_node_ || rt->children_.size() > 1) {
      last_node = rt;
      last_node_c = key[idx + 1];
    }
  }
  c = key[idx];
  if (rt->children_.find(c) == rt->children_.end()) {
    return *trie_p;
  }
  if (rt->children_[c]->children_.empty()) {
    last_node->children_.erase(last_node_c);
    return *trie_p;
  }
  rt->children_[c] = std::make_shared<TrieNode>(rt->children_[c]->children_);
  rt = std::const_pointer_cast<TrieNode>(rt->children_[c]);
  rt = std::static_pointer_cast<TrieNode>(rt);
  rt->is_value_node_ = false;
  return *trie_p;
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
