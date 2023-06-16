#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"
#include <stack>
namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  int index = 0, len = key.size();
  std::shared_ptr<const TrieNode> ptr = root_;

  if(nullptr == ptr || 0 >= len) {
    cout << "error assignment in Get!" << endl; 
    return nullptr;
  }

  while(index < len) {
    if(!(ptr->children_.count(key[index]))) return nullptr;
    ptr = ptr->children_[key[index]];
    ++index;
  }

  return ptr->is_value_node_ ? dynamic_cast<TrieNodeWithValue *>(ptr)->value_.get() : nullptr;

  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  //插入
  int index = 0, len = key.size();
  std::shared_ptr<const TrieNode> c_ptr = std::move(root_->Clone());
  std::shared_ptr<const TrieNode> root = c_ptr;
  //插入非终端节点
  while(index < len - 1) {
    std::shared_ptr<const TrieNode> t_ptr;
    if(c_ptr->children_.count(key[index])){ //有路径
      t_ptr = std::move((c_ptr->children_[key[index]])->Clone());
    } else { //没有路径
      t_ptr = std::shared_ptr<const TrieNode>(new TrieNode());
    }
    c_ptr->children_[key[index]] = t_ptr;
    c_ptr = t_ptr;
  }
  //插入终端节点
  std::shared_ptr<const TrieNodeWithValue> tv_ptr;
  // std::shared_ptr<T> val(std::move(value));
  if(c_ptr->children_.count(key[index])) {
    tv_ptr = std::shared_ptr<const TrieNodeWithValue>(new TrieNodeWithValue(c_ptr->children_, std::move(value)));
  } else {
    tv_ptr = std::shared_ptr<const TrieNodeWithValue>(new TrieNodeWithValue(std::move(value)));
  }
  c_ptr->children_[key[index]] = tv_ptr;
  // root_ = root;
  return root;
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

}

auto Trie::Remove(std::string_view key) const -> Trie {
  //删除节点与路径
  std::stack<std::shared_ptr<const TrieNode>> path;
  int index = 0, len = key.size();
  std::shared_ptr<const TrieNode> c_ptr = root_;
  
  while(index < len - 1) {
    if(!(c_ptr->children_.count(key[index]))) return *this;
    path.push(c_ptr);
    c_ptr = c_ptr->children_[key[index]];
    ++index;
  }
  if(!(c_ptr->children_.count(key[index]))) return *this;
  std::shared_ptr<const TrieNode> t_ptr = c_ptr->children_[key[index]];
  if(0 == t_ptr->children_.size()) {
    (c_ptr->children_).erase(key[index]);
  } else {
    t_ptr = std::shared_ptr<const TrieNode>(new TrieNode(t_ptr->children_));
    c_ptr->children_[key[index]] = t_ptr;
    return *this;
  }
  while(!path.empty()) {
    t_ptr = path.top();
    path.pop();
    if(1 != t_ptr->children_.size()) break;
    t_ptr->children_.erase(t_ptr->children_.begin(), t_ptr->children_.end());
  }
  return *this;

  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
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
