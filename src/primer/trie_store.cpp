#include "primer/trie_store.h"
#include "common/exception.h"
#include "primer/trie.h"
#include <memory>
#include <mutex>
#include <optional>

namespace bustub {

template <class T>
auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<T>> {
  std::shared_ptr<Trie> t;
  {
    std::lock_guard<std::mutex> lock(root_lock_);
    t = std::make_shared<Trie>(root_);
  }
  const T* res = t->Get<T>(key);
  if (nullptr == res) {
    return std::nullopt;
  }
  return std::make_optional<ValueGuard<T>>(*t, *res);
}

template <class T> void TrieStore::Put(std::string_view key, T value) {
  std::shared_ptr<Trie> t;
  std::lock_guard<std::mutex> wlock(write_lock_);
  {
    std::lock_guard<std::mutex> lock(root_lock_);
    t = std::make_shared<Trie>(root_);
  }
  auto ans = t->Put(key, std::move(value));
  {
    std::lock_guard<std::mutex> lock(root_lock_);
    root_ = ans;
  }
}

void TrieStore::Remove(std::string_view key) {
  std::shared_ptr<Trie> t;
  std::lock_guard<std::mutex> wlock(write_lock_);
  {
    std::lock_guard<std::mutex> lock(root_lock_);
    t = std::make_shared<Trie>(root_);
  }
  auto ans = t->Remove(key);
  {
    std::lock_guard<std::mutex> lock(root_lock_);
    root_ = ans;
  }
}

// Below are explicit instantiation of template functions.

template auto TrieStore::Get(std::string_view key)
    -> std::optional<ValueGuard<uint32_t>>;
template void TrieStore::Put(std::string_view key, uint32_t value);

template auto TrieStore::Get(std::string_view key)
    -> std::optional<ValueGuard<std::string>>;
template void TrieStore::Put(std::string_view key, std::string value);

// If your solution cannot compile for non-copy tests, you can remove the below
// lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto TrieStore::Get(std::string_view key)
    -> std::optional<ValueGuard<Integer>>;
template void TrieStore::Put(std::string_view key, Integer value);

template auto TrieStore::Get(std::string_view key)
    -> std::optional<ValueGuard<MoveBlocked>>;
template void TrieStore::Put(std::string_view key, MoveBlocked value);

} // namespace bustub
