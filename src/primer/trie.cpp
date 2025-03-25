#include "primer/trie.h"
#include "common/exception.h"
#include <memory>
#include <stack>
#include <string_view>
#include <utility>

namespace bustub {

template <class T> auto Trie::Get(std::string_view key) const -> const T * {
  if (nullptr == root_) {
    return nullptr;
  }
  auto newRoot(root_);
  int len = key.size();
  for (int i = 0; i < len; ++i) {
    auto iter = newRoot->children_.find(key[i]);
    if (iter == newRoot->children_.end()) {
      return nullptr;
    }
    newRoot = iter->second;
  }
  if (!newRoot->is_value_node_ || nullptr == dynamic_cast<const TrieNodeWithValue<T> *>(newRoot.get())
      || nullptr == dynamic_cast<const TrieNodeWithValue<T> *>(newRoot.get())->value_) {
    return nullptr;
  }
  const T *t = dynamic_cast<const TrieNodeWithValue<T> *>(newRoot.get())->value_.get();
  return t;
}

template <class T> auto Trie::Put(std::string_view key, T value) const -> Trie {
  TriePtr t = std::make_shared<Trie>();
  TrieNodePtr root;
  int len = key.size();
  if (0 == len) {
    ChildrenMap children;
    if (nullptr != root_) {
      children = root_->children_;
    }
    root = std::make_shared<TrieNodeWithValue<T>>(children, std::make_shared<T>(std::move(value)));
    t->root_ = root;
    return *t;
  }
  if (nullptr != root_) {
    root = std::shared_ptr<TrieNode>(root_->Clone());
  } else {
    root = std::make_shared<TrieNode>();
  }
  t->root_ = root;
  for (int i = 0; i < len - 1; ++i) {
    auto iter = root->children_.find(key[i]);
    if (iter == root->children_.end()) {
      TrieNodePtr p = std::make_shared<TrieNode>();
      root->children_.emplace(key[i], p);
      root = std::const_pointer_cast<TrieNode>(p);
    } else {
      iter->second = TrieNodePtr(iter->second->Clone());
      root = std::const_pointer_cast<TrieNode>(iter->second);
    }
  }
  auto iter = root->children_.find(key[len - 1]);
  if (iter == root->children_.end()) {
    root->children_.emplace(key[len - 1], std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value))));
  } else {
    ChildrenMap children = iter->second->children_;
    iter->second = std::make_shared<TrieNodeWithValue<T>>(children, std::make_shared<T>(std::move(value)));
  }
  return *t;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  TriePtr t = std::make_shared<Trie>();
  if (nullptr != root_) {
    TrieNodePtr root(root_->Clone());
    t->root_ = root;
    TrieNodePtr lastPtr = root;
    char lastC = key[0];
    int len = key.size();
    for (int i = 0; i < len - 1; ++i) {
      auto iter = root->children_.find(key[i]);
      if (iter == root->children_.end()) {
        return *t;
      }
      iter->second = TrieNodePtr(iter->second->Clone());
      root = std::const_pointer_cast<TrieNode>(iter->second);
      if (root->is_value_node_ || root->children_.size() > 1) {
        lastPtr = root;
        lastC = key[i + 1];
      }
    }
    auto iter = root->children_.find(key[len - 1]);
    if (iter == root->children_.end()) {
      return *t;
    }
    if (iter->second->children_.empty()) {
      lastPtr->children_.erase(lastC);
      return *t;
    }
    iter->second = std::make_shared<TrieNode>(iter->second->children_);
    root = std::const_pointer_cast<TrieNode>(iter->second);
    root->is_value_node_ = false;
  }
  return *t;
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and
// functions in the header file. However, we separate the implementation into a
// .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate
// them here, so that they can be picked up by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below
// lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

} // namespace bustub
