#include "primer/trie_store.h"
#include <optional>
#include "common/exception.h"

namespace bustub {

template <class T>
auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<T>> {
  // Pseudo-code:
  // (1) Take the root lock, get the root, and release the root lock. Don't lookup the value in the
  //     trie while holding the root lock.
  // (2) Lookup the value in the trie.
  // (3) If the value is found, return a ValueGuard object that holds a reference to the value and the
  //     root. Otherwise, return std::nullopt.

  // get root
  std::unique_lock<std::mutex> rlock(root_lock_);
  std::shared_ptr<Trie> trie_p = std::make_shared<Trie>(root_);
  rlock.unlock();
  // get the value
  if (nullptr == trie_p->Get<T>(key)) {
    return std::nullopt;
  }
  const T *res = trie_p->Get<T>(key);
  std::optional<ValueGuard<T>> opt = std::make_optional<ValueGuard<T>>(*trie_p, *res);
  return opt;
}

template <class T>
void TrieStore::Put(std::string_view key, T value) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.

  std::unique_lock<std::mutex> wlock(write_lock_);
  std::unique_lock<std::mutex> rlock(root_lock_);
  std::shared_ptr<Trie> trie_p = std::make_shared<Trie>(root_);
  rlock.unlock();

  auto ans = trie_p->Put<T>(key, std::move(value));

  rlock.lock();
  root_ = ans;
  rlock.unlock();
  wlock.unlock();
}

void TrieStore::Remove(std::string_view key) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
  std::unique_lock<std::mutex> wlock(write_lock_);
  std::unique_lock<std::mutex> rlock(root_lock_);
  std::shared_ptr<Trie> trie_p = std::make_shared<Trie>(root_);
  rlock.unlock();

  auto ans = trie_p->Remove(key);

  rlock.lock();
  root_ = ans;
  rlock.unlock();
  wlock.unlock();
}

// Below are explicit instantiation of template functions.

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<uint32_t>>;
template void TrieStore::Put(std::string_view key, uint32_t value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<std::string>>;
template void TrieStore::Put(std::string_view key, std::string value);

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<Integer>>;
template void TrieStore::Put(std::string_view key, Integer value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<MoveBlocked>>;
template void TrieStore::Put(std::string_view key, MoveBlocked value);

}  // namespace bustub
