#include <sstream>
#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/**
 * Helper function to decide whether current b+tree is empty
 * @brief : read from header page and check root_page_id
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  const auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/**
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  pthread_t thread_id = pthread_self();
  std::stringstream ss;
  ss << thread_id << " getvalue:" << key << "max internal size" << internal_max_size_ << "leaf max size"
     << leaf_max_size_ << std::endl;
  LOG_DEBUG("%s", ss.str().c_str());
  try {
    // get root page
    if (header_page_id_ == INVALID_PAGE_ID) {
      return false;
    }
    ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
    const auto *header_page = header_guard.As<BPlusTreeHeaderPage>();
    page_id_t root_page_id = header_page->root_page_id_;
    if (root_page_id == INVALID_PAGE_ID) {
      return false;
    }
    ReadPageGuard pg_guard = bpm_->FetchPageRead(root_page_id);
    header_guard.Drop();
    const auto *b_page = pg_guard.As<class BPlusTreePage>();
    // loop and found value
    while (!b_page->IsLeafPage()) {
      const auto *bi_page = reinterpret_cast<const InternalPage *>(b_page);
      int i = bi_page->GetIndexLargerThanKey(1, key, comparator_);
      auto n_pid = static_cast<page_id_t>(bi_page->ValueAt(i - 1));
      if (n_pid == INVALID_PAGE_ID) {
        return false;
      }
      pg_guard = bpm_->FetchPageRead(n_pid);
      b_page = pg_guard.As<class BPlusTreePage>();
    }
    // get leaf node
    const auto *bl_page = reinterpret_cast<const LeafPage *>(b_page);
    int i = 0;
    if (bl_page->GetIndexEqualToKey(i, key, comparator_)) {
      result->emplace_back(bl_page->ValueAt(i));
      return true;
    }
  } catch (const std::exception &e) {
    std::cout << e.what() << " in GetValue" << std::endl;
    throw e;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // when we insert a tuple, we should remember parent context and do cursive insertion
  pthread_t thread_id = pthread_self();
  std::stringstream ss;
  ss << thread_id << " insert:" << key << ", " << value  << "max internal size" << internal_max_size_ << "leaf max size"
     << leaf_max_size_<< std::endl;
  LOG_DEBUG("%s", ss.str().c_str());
  try {
    if (header_page_id_ == INVALID_PAGE_ID) {
      return false;
    }
    WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
    auto *header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
    page_id_t root_page_id = header_page->root_page_id_;
    // new page
    if (INVALID_PAGE_ID == root_page_id) {
      page_id_t pid = -1;
      auto guard = bpm_->NewPageGuarded(&pid);
      if (-1 == pid) {
        LOG_DEBUG("b_plus_tree: error in insert");
        return false;
      }
      // change header page
      header_page->root_page_id_ = pid;
      // get addr and init the page
      auto *bl_page = guard.AsMut<LeafPage>();
      bl_page->Init(leaf_max_size_);
      // size++, set next page id(no need), change array_
      bl_page->InsertKeyAndValueAt(0, key, value);
      BUSTUB_ASSERT(bl_page->GetSize() == 1, "make new root page error");
      return true;
    }
    // insert page
    bool need_split_root = true;
    Context ctx;
    ctx.header_page_ = std::move(header_guard);
    ctx.root_page_id_ = root_page_id;
    WritePageGuard guard = bpm_->FetchPageWrite(root_page_id);
    auto *b_page = guard.AsMut<class BPlusTreePage>();
    ctx.write_set_.push_back(std::move(guard));

    while (!b_page->IsLeafPage()) {
      auto *bi_page = reinterpret_cast<InternalPage *>(b_page);
      int i = bi_page->GetIndexLargerThanKey(1, key, comparator_);
      auto n_pid = static_cast<page_id_t>(bi_page->ValueAt(i - 1));
      BUSTUB_ASSERT(n_pid != INVALID_PAGE_ID, "get invalid page id");
      if (0 == n_pid) {
        return false;
      }
      // record sth
      guard = bpm_->FetchPageWrite(n_pid);
      b_page = guard.AsMut<class BPlusTreePage>();
      // can safely insert
      if (b_page->GetSize() < b_page->GetMaxSize()) {
        ctx.write_set_.clear();
        ctx.header_page_ = std::nullopt;
        need_split_root = false;
      }
      ctx.write_set_.push_back(std::move(guard));
    }
    MappingType kv_map(key, value);
    return InsertLeafPage(ctx, kv_map, need_split_root, txn);
  } catch (const std::exception &e) {
    std::cout << "Exception caught: " << e.what() << " in file " << __FILE__ << " at line " << __LINE__ << std::endl;
    throw e;
  }
  return false;
}

/**
 * @brief insert / split the leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertLeafPage(Context &ctx, MappingType &mapping, bool &need_split_root, Transaction *txn)
    -> bool {
  try {
    BUSTUB_ASSERT(!ctx.write_set_.empty(), "write set shouldn't empty");
    WritePageGuard guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    auto *b_page = guard.AsMut<LeafPage>();
    BUSTUB_ASSERT(b_page->IsLeafPage(), "error insert LeafPage");
    int i = b_page->GetIndexLargerThanKey(0, mapping.first, comparator_);
    if ((i > 0 && comparator_(b_page->KeyAt(i - 1), mapping.first) == 0)) {
      return false;
    }
    // can insert directly
    if (b_page->GetSize() < b_page->GetMaxSize()) {
      b_page->InsertKeyAndValueAt(i, mapping.first, mapping.second);  // {key, rid}
      return true;
    }
    // need split
    page_id_t new_page_id = -1;
    BasicPageGuard new_page_guard = bpm_->NewPageGuarded(&new_page_id);
    BUSTUB_ASSERT(new_page_id != -1, "new page error in insert leaf page");
    auto *new_page = new_page_guard.AsMut<LeafPage>();
    new_page->Init(leaf_max_size_);

    if (b_page->GetMaxSize() % 2) {
      int mid_idx = b_page->GetMaxSize() / 2 + 1;
      KeyType mid_key = b_page->KeyAt(mid_idx);
      if (i <= mid_idx) {
        KeyType record_key = b_page->KeyAt(b_page->GetMaxSize() - 1);
        ValueType record_value = b_page->ValueAt(b_page->GetMaxSize() - 1);
        // b_page equal max_size

        b_page->InsertKeyAndValueAt(i, mapping.first, mapping.second);
        mid_key = b_page->KeyAt(mid_idx);
        int mv_len = b_page->GetMaxSize() - mid_idx;
        new_page->MemMove(b_page, mid_idx, 0, mv_len);
        new_page->IncreaseSize(mv_len);

        new_page->InsertKeyAndValueAt(b_page->GetMaxSize() - mid_idx, record_key, record_value);
      } else {
        int mv_len = b_page->GetMaxSize() - mid_idx;
        new_page->MemMove(b_page, mid_idx, 0, mv_len);
        new_page->IncreaseSize(mv_len);

        new_page->InsertKeyAndValueAt(i - mid_idx, mapping.first, mapping.second);
      }
      b_page->IncreaseSize(mid_idx - b_page->GetMaxSize());
      BUSTUB_ASSERT(new_page->GetSize() == b_page->GetSize(), "new page size should equal ori page size");
      ctx.last_page_id_ = guard.PageId();
      new_page->SetNextPageId(b_page->GetNextPageId());
      b_page->SetNextPageId(new_page_guard.PageId());
      ctx.last_insert_page_ = std::move(guard);
      return InsertInternalPage(ctx, mid_key, new_page_guard.PageId(), need_split_root, txn);
    }
    // b_page->GetMaxSize() % 2 == 0
    int mid_idx = b_page->GetMaxSize() / 2;
    KeyType mid_key = b_page->KeyAt(mid_idx);
    if (i <= mid_idx) {
      KeyType record_key = b_page->KeyAt(b_page->GetMaxSize() - 1);
      ValueType record_value = b_page->ValueAt(b_page->GetMaxSize() - 1);
      b_page->InsertKeyAndValueAt(i, mapping.first, mapping.second);
      mid_key = b_page->KeyAt(mid_idx);
      int mv_len = b_page->GetMaxSize() - mid_idx;
      new_page->MemMove(b_page, mid_idx, 0, mv_len);
      new_page->IncreaseSize(mv_len);
      new_page->InsertKeyAndValueAt(b_page->GetMaxSize() - mid_idx, record_key, record_value);
    } else {
      int mv_len = b_page->GetMaxSize() - mid_idx;
      new_page->MemMove(b_page, mid_idx, 0, mv_len);
      new_page->IncreaseSize(mv_len);
      new_page->InsertKeyAndValueAt(i - mid_idx, mapping.first, mapping.second);
    }
    b_page->IncreaseSize(mid_idx - b_page->GetMaxSize());
    BUSTUB_ASSERT(new_page->GetSize() == b_page->GetSize() + 1, "new page size minus 1 should equal ori page size");
    ctx.last_page_id_ = guard.PageId();
    new_page->SetNextPageId(b_page->GetNextPageId());
    b_page->SetNextPageId(new_page_guard.PageId());
    ctx.last_insert_page_ = std::move(guard);
    return InsertInternalPage(ctx, mid_key, new_page_guard.PageId(), need_split_root, txn);
  } catch (const std::exception &e) {
    std::cout << e.what() << "in insert leaf page" << std::endl;
    throw e;
  }
  return false;
}

/**
 * @brief split the internal page
 */

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertInternalPage(Context &ctx, const KeyType &key, page_id_t value, bool &need_split_root,
                                        Transaction *txn) -> bool {
  try {
    if (ctx.write_set_.empty()) {
      if (need_split_root) {
        page_id_t new_root_page_id = -1;
        BasicPageGuard root_guard = bpm_->NewPageGuarded(&new_root_page_id);
        BUSTUB_ASSERT(new_root_page_id != -1, "new page error in insert leaf page");
        auto *root_page = root_guard.AsMut<InternalPage>();
        root_page->Init(internal_max_size_);
        BUSTUB_ASSERT(root_page->GetSize() == 1, "init root size must equal 2");
        root_page->SetValueAt(0, ctx.last_page_id_);
        root_page->InsertKeyAndValueAt(1, key, value);
        BUSTUB_ASSERT(root_page->GetSize() == 2, "new root size must equal 2");
        // update header page
        std::unique_lock<std::mutex> little_lock(little_latch_);
        ctx.last_insert_page_->Drop();
        ctx.UpdateRootPage(new_root_page_id, bpm_);
        little_lock.unlock();
      }
      return true;
    }
    WritePageGuard guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    auto *b_page = guard.AsMut<InternalPage>();
    BUSTUB_ASSERT(b_page->IsInternalPage(), "error insert internal page");
    int i = b_page->GetIndexLargerThanKey(1, key, comparator_);
    if ((i > 1 && comparator_(b_page->KeyAt(i - 1), key) == 0)) {
      return false;
    }
    // can insert directly
    if (b_page->GetSize() < b_page->GetMaxSize()) {
      b_page->InsertKeyAndValueAt(i, key, value);  // {key, rid}
      need_split_root = false;
      return true;
    }
    // need split
    page_id_t new_page_id = -1;
    BasicPageGuard new_internal_page_guard = bpm_->NewPageGuarded(&new_page_id);
    BUSTUB_ASSERT(new_page_id != -1, "new page error in insert leaf page");
    auto *new_page = new_internal_page_guard.AsMut<InternalPage>();
    new_page->Init(internal_max_size_);

    // tricky: i and mid_idx who is larger
    if (b_page->GetMaxSize() % 2) {
      int mid_idx = b_page->GetMaxSize() / 2 + 1;
      KeyType mid_key = b_page->KeyAt(mid_idx);
      page_id_t mid_value = b_page->ValueAt(mid_idx);
      if (i <= mid_idx) {
        KeyType record_key = b_page->KeyAt(b_page->GetMaxSize() - 1);
        page_id_t record_value = b_page->ValueAt(b_page->GetMaxSize() - 1);
        b_page->InsertKeyAndValueAt(i, key, value);
        mid_key = b_page->KeyAt(mid_idx);
        mid_value = b_page->ValueAt(mid_idx);
        int mv_len = b_page->GetMaxSize() - mid_idx - 1;
        new_page->MemMove(b_page, mid_idx + 1, 1, mv_len);
        new_page->IncreaseSize(mv_len);

        new_page->InsertKeyAndValueAt(b_page->GetMaxSize() - mid_idx, record_key, record_value);
      } else {
        int mv_len = b_page->GetMaxSize() - mid_idx - 1;
        new_page->MemMove(b_page, mid_idx + 1, 1, mv_len);
        new_page->IncreaseSize(mv_len);

        new_page->InsertKeyAndValueAt(i - mid_idx, key, value);
      }
      b_page->IncreaseSize(mid_idx - b_page->GetMaxSize());
      new_page->SetValueAt(0, mid_value);
      BUSTUB_ASSERT(new_page->GetSize() == b_page->GetSize(), "new page size should equal ori page size");
      ctx.last_page_id_ = guard.PageId();
      ctx.last_insert_page_ = std::move(guard);
      return InsertInternalPage(ctx, mid_key, new_internal_page_guard.PageId(), need_split_root, txn);
    }
    // b_page->GetMaxSize() % 2 == 0
    BUSTUB_ASSERT(b_page->GetMaxSize() % 2 == 0, "maxsize % 2 should euqal 0");
    int mid_idx = b_page->GetMaxSize() / 2;
    KeyType mid_key = b_page->KeyAt(mid_idx);
    page_id_t mid_value = b_page->ValueAt(mid_idx);
    if (i <= mid_idx) {
      KeyType record_key = b_page->KeyAt(b_page->GetMaxSize() - 1);
      page_id_t record_value = b_page->ValueAt(b_page->GetMaxSize() - 1);
      b_page->InsertKeyAndValueAt(i, key, value);
      mid_key = b_page->KeyAt(mid_idx);
      mid_value = b_page->ValueAt(mid_idx);
      int mv_len = b_page->GetMaxSize() - mid_idx - 1;
      new_page->MemMove(b_page, mid_idx + 1, 1, mv_len);
      new_page->IncreaseSize(mv_len);

      new_page->InsertKeyAndValueAt(b_page->GetMaxSize() - mid_idx, record_key, record_value);
    } else {
      int mv_len = b_page->GetMaxSize() - mid_idx - 1;
      new_page->MemMove(b_page, mid_idx + 1, 1, mv_len);
      new_page->IncreaseSize(mv_len);

      new_page->InsertKeyAndValueAt(i - mid_idx, key, value);
    }
    b_page->IncreaseSize(mid_idx - b_page->GetMaxSize());
    new_page->SetValueAt(0, mid_value);
    BUSTUB_ASSERT(new_page->GetSize() == b_page->GetSize() + 1, "new page size plus 1 should equal ori page size");
    ctx.last_page_id_ = guard.PageId();
    ctx.last_insert_page_ = std::move(guard);
    return InsertInternalPage(ctx, mid_key, new_internal_page_guard.PageId(), need_split_root, txn);
  } catch (std::exception &e) {
    std::cout << e.what() << "in insert internal page" << std::endl;
    throw e;
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  pthread_t thread_id = pthread_self();
  std::stringstream ss;
  ss << thread_id<< " remove:" << key  << "max internal size" << internal_max_size_ << "leaf max size" << leaf_max_size_
     << std::endl;
  LOG_DEBUG("%s", ss.str().c_str());
  try {
    if (header_page_id_ == INVALID_PAGE_ID) {
      return;
    }
    WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
    auto *header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
    page_id_t root_page_id = header_page->root_page_id_;
    if (INVALID_PAGE_ID == root_page_id) {
      return;
    }
    Context ctx;
    // bool need_merge_root = true;
    ctx.header_page_ = std::move(header_guard);
    ctx.root_page_id_ = root_page_id;
    WritePageGuard guard = bpm_->FetchPageWrite(root_page_id);
    auto *b_page = guard.AsMut<class BPlusTreePage>();
    ctx.write_set_.push_back(std::move(guard));
    // find to the leaf page
    while (!b_page->IsLeafPage()) {
      auto *bi_page = reinterpret_cast<InternalPage *>(b_page);
      int i = bi_page->GetIndexLargerThanKey(1, key, comparator_);
      auto n_pid = static_cast<page_id_t>(bi_page->ValueAt(i - 1));
      BUSTUB_ASSERT(n_pid != INVALID_PAGE_ID, "get invalid page id");
      if (0 == n_pid) {
        return;
      }
      // record sth
      guard = bpm_->FetchPageWrite(n_pid);
      b_page = guard.AsMut<class BPlusTreePage>();
      // can safely delete
      if (b_page->GetSize() > b_page->GetMinSize()) {  // delete can't cause root change
        ctx.path_.clear();
        ctx.write_set_.clear();
        ctx.header_page_ = std::nullopt;  // release header page guard
      }
      ctx.path_.emplace_back(i - 1);
      ctx.write_set_.push_back(std::move(guard));
    }
    DeleteLeafPage(ctx, key, txn);
  } catch (const std::exception &e) {
    std::cout << e.what() << "in remove" << std::endl;
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteLeafPage(Context &ctx, const KeyType &key, Transaction *txn) {
  BUSTUB_ASSERT(!ctx.write_set_.empty(), "write set shouldn't empty");
  WritePageGuard guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto *b_page = guard.AsMut<LeafPage>();
  BUSTUB_ASSERT(b_page->IsLeafPage(), "error delete LeafPage");
  // find idx to insert
  int i = 0;
  if (!b_page->GetIndexEqualToKey(i, key, comparator_)) {
    return;
  }
  // can delete directly
  page_id_t guard_pid = guard.PageId();
  if (ctx.IsRootPage(guard_pid) || b_page->GetSize() > b_page->GetMinSize()) {
    b_page->DeleteKeyAndValueAt(i);
    if (b_page->GetSize() == 0) {
      // Drop guard
      std::unique_lock<std::mutex> little_lock(little_latch_);
      guard.Drop();
      if (!bpm_->DeletePage(guard_pid)) {  // FIXED: add real delete
        LOG_DEBUG("delete page failed");
      }
      ctx.UpdateRootPage(INVALID_PAGE_ID, bpm_);
      little_lock.unlock();
    }
    return;
  }
  ctx.last_page_id_ = guard.PageId();
  ctx.last_index_ = i;
  // need merge or distribute, change node to parent node
  BUSTUB_ASSERT(!ctx.write_set_.empty(), "ctx should have parent node");
  ctx.can_directly_delete_ = false;
  guard.Drop();
  return DeleteInternalPage(ctx, txn);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteInternalPage(Context &ctx, Transaction *txn) {
  if (ctx.write_set_.empty()) {
    return;
  }
  WritePageGuard guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto *b_page = guard.AsMut<InternalPage>();

  BUSTUB_ASSERT(b_page->IsInternalPage(), "error delete InternalPage");
  // we should find child in parent value
  int i = ctx.path_.back();
  ctx.path_.pop_back();
  if (-1 == i || b_page->ValueAt(i) != ctx.last_page_id_) {
    LOG_DEBUG("ctx.path_ error");
    return;
  }
  BUSTUB_ASSERT(i != b_page->GetSize(), "should found this child");
  // find left child and get its size
  if (i > 0) {
    WritePageGuard l_guard = bpm_->FetchPageWrite(b_page->ValueAt(i - 1));
    WritePageGuard ori_guard = bpm_->FetchPageWrite(ctx.last_page_id_);
    auto *ori_page = ori_guard.AsMut<BPlusTreePage>();
    auto *l_page = l_guard.AsMut<BPlusTreePage>();
    if (l_page->GetSize() > l_page->GetMinSize()) {
      if (l_page->IsLeafPage()) {
        auto *l_leaf = reinterpret_cast<LeafPage *>(l_page);
        // get key-value from l_child
        KeyType record_key = l_leaf->KeyAt(l_leaf->GetSize() - 1);
        ValueType record_value = l_leaf->ValueAt(l_leaf->GetSize() - 1);
        l_leaf->IncreaseSize(-1);
        b_page->SetKeyAt(i, std::move(record_key));
        auto *ori_leaf_page = ori_guard.AsMut<LeafPage>();
        BUSTUB_ASSERT(ori_page->IsLeafPage(), "ori_page should be a leaf page");
        BUSTUB_ASSERT(-1 != ctx.last_index_, "last_index_ is need delete index");
        ori_leaf_page->DeleteKeyAndValueAt(ctx.last_index_);
        ori_leaf_page->InsertKeyAndValueAt(0, KeyType(), record_value);
      } else {
        BUSTUB_ASSERT(l_page->IsInternalPage(), "l_page must be internal page");
        auto *l_internal = reinterpret_cast<InternalPage *>(l_page);
        // record the mid key of ori page and left guy (from their parent node)
        KeyType mid_key = b_page->KeyAt(i);
        KeyType record_key_l = l_internal->KeyAt(l_internal->GetSize() - 1);
        page_id_t record_value_l = l_internal->ValueAt(l_internal->GetSize() - 1);
        l_internal->IncreaseSize(-1);
        b_page->SetKeyAt(i, std::move(record_key_l));
        auto *ori_internal_page = ori_guard.AsMut<InternalPage>();
        BUSTUB_ASSERT(ori_page->IsInternalPage(), "ori_page should be a leaf page");
        BUSTUB_ASSERT(-1 != ctx.last_index_, "last_index_ is need delete index");
        // remove ori key
        ori_internal_page->DeleteKeyAndValueAt(ctx.last_index_);
        page_id_t record_value_ori = ori_internal_page->ValueAt(0);
        ori_internal_page->InsertKeyAndValueAt(1, mid_key, record_value_ori);
        ori_internal_page->SetValueAt(0, record_value_l);
      }  // else
      BUSTUB_ASSERT(ori_page->GetSize() == ori_page->GetMinSize(), "ori_page size should equal to minsize");
      return;
    }  // l_page size > minsize
  }    // i > 0
  // get from right guy
  if (i < b_page->GetSize() - 1) {  // i = 0
    WritePageGuard r_guard = bpm_->FetchPageWrite(b_page->ValueAt(i + 1));
    WritePageGuard ori_guard = bpm_->FetchPageWrite(ctx.last_page_id_);
    auto *ori_page = ori_guard.AsMut<BPlusTreePage>();
    auto *r_page = r_guard.AsMut<BPlusTreePage>();
    if (r_page->GetSize() > r_page->GetMinSize()) {
      if (r_page->IsLeafPage()) {
        auto *r_leaf = reinterpret_cast<LeafPage *>(r_page);
        // get key-value from r_child
        KeyType record_key = r_leaf->KeyAt(0);
        KeyType record_key_1 = r_leaf->KeyAt(1);
        ValueType record_value = r_leaf->ValueAt(0);
        r_leaf->DeleteKeyAndValueAt(0);
        b_page->SetKeyAt(i + 1, std::move(record_key_1));
        auto *ori_leaf_page = ori_guard.AsMut<LeafPage>();
        BUSTUB_ASSERT(ori_page->IsLeafPage(), "ori_page should be a leaf page");
        BUSTUB_ASSERT(-1 != ctx.last_index_, "last_index_ is need delete index");
        ori_leaf_page->DeleteKeyAndValueAt(ctx.last_index_);
        ori_leaf_page->InsertKeyAndValueAt(ori_page->GetSize(), record_key, record_value);
      } else {
        BUSTUB_ASSERT(r_page->IsInternalPage(), "r_page must be internal page");
        auto *r_internal = reinterpret_cast<InternalPage *>(r_page);
        KeyType record_key = r_internal->KeyAt(1);
        KeyType mid_key = b_page->KeyAt(i + 1);
        page_id_t record_value = r_internal->ValueAt(0);
        // page_id_t record_value_1 = r_internal->ValueAt(1);
        r_internal->DeleteKeyAndValueAt(0);
        // r_internal->SetValueAt(0, record_value_1);
        b_page->SetKeyAt(i + 1, std::move(record_key));
        auto *ori_internal_page = ori_guard.AsMut<InternalPage>();
        BUSTUB_ASSERT(ori_page->IsInternalPage(), "ori_page should be a leaf page");
        BUSTUB_ASSERT(-1 != ctx.last_index_, "last_index_ is need delete index");
        ori_internal_page->DeleteKeyAndValueAt(ctx.last_index_);
        ori_internal_page->InsertKeyAndValueAt(ori_page->GetSize(), mid_key, record_value);
      }  // else
      BUSTUB_ASSERT(ori_page->GetSize() == ori_page->GetMinSize(), "ori_page size should equal to minsize");
      return;
    }  // r_page size > minsize
  }
  // need merge
  WritePageGuard ori_guard = bpm_->FetchPageWrite(ctx.last_page_id_);
  auto *ori_page = ori_guard.AsMut<BPlusTreePage>();
  WritePageGuard neighbour_guard;
  if (i > 0) {
    neighbour_guard = bpm_->FetchPageWrite(b_page->ValueAt(i - 1));
  } else {
    neighbour_guard = bpm_->FetchPageWrite(b_page->ValueAt(i + 1));
  }
  auto *neighbour_page = neighbour_guard.AsMut<BPlusTreePage>();
  // merge leaf page
  if (ori_page->IsLeafPage()) {
    // move ori to left neighbour
    auto *ori_leaf_page = reinterpret_cast<LeafPage *>(ori_page);
    auto *neighbour_leaf_page = reinterpret_cast<LeafPage *>(neighbour_page);
    if (i > 0) {
      int start_idx = ori_leaf_page->GetSize();
      for (int i = 0; i < ori_leaf_page->GetSize(); ++i) {
        neighbour_leaf_page->InsertKeyAndValueAt(i + start_idx, ori_leaf_page->KeyAt(i), ori_leaf_page->ValueAt(i));
      }
      neighbour_leaf_page->DeleteKeyAndValueAt(ctx.last_index_ + start_idx);
      neighbour_leaf_page->SetNextPageId(ori_leaf_page->GetNextPageId());
      ctx.merged_page_id_ = neighbour_guard.PageId();
      ctx.deleted_page_id_ = ori_guard.PageId();  // FIXED: add real delete
      ori_guard.Drop();
      if (!bpm_->DeletePage(ctx.deleted_page_id_)) {  // FIXED: add real delete
        LOG_DEBUG("Deleting page failed");
      }
    } else {  // i = 0
      int start_idx = neighbour_leaf_page->GetSize();
      for (int i = 0; i < neighbour_leaf_page->GetSize(); ++i) {
        ori_leaf_page->InsertKeyAndValueAt(i + start_idx, neighbour_leaf_page->KeyAt(i),
                                           neighbour_leaf_page->ValueAt(i));
      }
      ori_leaf_page->DeleteKeyAndValueAt(ctx.last_index_);
      ori_leaf_page->SetNextPageId(neighbour_leaf_page->GetNextPageId());
      ctx.merged_page_id_ = ori_guard.PageId();
      ctx.deleted_page_id_ = neighbour_guard.PageId();  // FIXED: add real delete
      neighbour_guard.Drop();
      if (!bpm_->DeletePage(ctx.deleted_page_id_)) {  // FIXED: add real delete
        LOG_DEBUG("Deleting page failed");
      }
    }
  } else {
    auto *ori_internal_page = reinterpret_cast<InternalPage *>(ori_page);
    auto *neighbour_internal_page = reinterpret_cast<InternalPage *>(neighbour_page);
    ori_internal_page->DeleteKeyAndValueAt(ctx.last_index_);
    KeyType mid_key = b_page->KeyAt(i);
    if (i > 0) {
      int start_idx = neighbour_internal_page->GetSize();
      neighbour_internal_page->InsertKeyAndValueAt(start_idx, mid_key, ori_internal_page->ValueAt(0));
      for (int i = 1; i < ori_internal_page->GetSize(); ++i) {
        neighbour_internal_page->InsertKeyAndValueAt(start_idx + i, ori_internal_page->KeyAt(i),
                                                     ori_internal_page->ValueAt(i));
      }
      ctx.merged_page_id_ = neighbour_guard.PageId();
      ctx.deleted_page_id_ = ori_guard.PageId();  // FIXED: add real delete
      ori_guard.Drop();
      if (!bpm_->DeletePage(ctx.deleted_page_id_)) {  // FIXED: add real delete
        LOG_DEBUG("Deleting page failed");
      }
    } else {  // i = 0
      mid_key = b_page->KeyAt(1);
      int start_idx = ori_internal_page->GetSize();
      ori_internal_page->InsertKeyAndValueAt(start_idx, mid_key, neighbour_internal_page->ValueAt(0));
      for (int i = 1; i < neighbour_internal_page->GetSize(); ++i) {
        ori_internal_page->InsertKeyAndValueAt(start_idx + i, neighbour_internal_page->KeyAt(i),
                                               neighbour_internal_page->ValueAt(i));
      }
      ctx.merged_page_id_ = ori_guard.PageId();
      ctx.deleted_page_id_ = neighbour_guard.PageId();  // FIXED: add real delete
      neighbour_guard.Drop();
      if (!bpm_->DeletePage(ctx.deleted_page_id_)) {  // FIXED: add real delete
        LOG_DEBUG("Deleting page failed");
      }
    }
  }
  ctx.last_page_id_ = guard.PageId();
  ctx.last_index_ = (i == 0 ? 1 : i);
  if (b_page->GetSize() > b_page->GetMinSize()) {
    b_page->DeleteKeyAndValueAt(ctx.last_index_);
    return;
  }
  // deal with two level
  if (ctx.IsRootPage(guard.PageId())) {
    b_page->DeleteKeyAndValueAt(ctx.last_index_);
    if (b_page->GetSize() == 1) {  // only merge can go to here
      if (INVALID_TXN_ID != ctx.merged_page_id_) {
        std::unique_lock<std::mutex> little_lock(little_latch_);
        guard.Drop();
        ctx.UpdateRootPage(ctx.merged_page_id_, bpm_);
        little_lock.unlock();
      }
    }
    return;
  }
  guard.Drop();
  ori_guard.Drop();
  neighbour_guard.Drop();
  return DeleteInternalPage(ctx, txn);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  pthread_t thread_id = pthread_self();
  std::stringstream ss;
  ss << thread_id << " begin:"
     << "max internal size" << internal_max_size_ << "leaf max size" << leaf_max_size_ << std::endl;
  LOG_DEBUG("%s", ss.str().c_str());
  try {
    if (header_page_id_ == INVALID_PAGE_ID) {
      return INDEXITERATOR_TYPE(0, 0, 0, nullptr, nullptr);
    }
    ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
    const auto *header_page = header_guard.As<BPlusTreeHeaderPage>();
    page_id_t root_page_id = header_page->root_page_id_;
    if (INVALID_PAGE_ID == root_page_id) {
      return INDEXITERATOR_TYPE(0, 0, 0, nullptr, nullptr);
    }
    // get root page
    WritePageGuard pg_guard = bpm_->FetchPageWrite(root_page_id);
    header_guard.Drop();
    auto *b_page = pg_guard.AsMut<class BPlusTreePage>();
    // loop and found value
    while (!b_page->IsLeafPage()) {
      const auto *bi_page = reinterpret_cast<const InternalPage *>(b_page);
      auto n_pid = static_cast<page_id_t>(bi_page->ValueAt(0));
      BUSTUB_ASSERT(INVALID_PAGE_ID != n_pid, "n_pid should be valid");
      if (0 == n_pid) {
        return INDEXITERATOR_TYPE(0, 0, 0, nullptr, nullptr);
      }
      pg_guard = bpm_->FetchPageWrite(n_pid);
      b_page = pg_guard.AsMut<class BPlusTreePage>();
    }
    // get leaf node
    auto *bl_page = reinterpret_cast<LeafPage *>(b_page);
    pg_guard.Drop();
    return INDEXITERATOR_TYPE(0, std::move(bl_page->GetSize()), reinterpret_cast<uint64_t>(this), bpm_, bl_page);
  } catch (const std::exception &e) {
    std::cout << e.what() << "in begin" << std::endl;
    throw e;
  }
  return INDEXITERATOR_TYPE(0, 0, 0, nullptr, nullptr);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  pthread_t thread_id = pthread_self();
  std::stringstream ss;
  ss << thread_id << " begin(key): key " << key << ", max internal size" << internal_max_size_ << "leaf max size"
     << leaf_max_size_ << std::endl;
  LOG_DEBUG("%s", ss.str().c_str());
  try {
    if (header_page_id_ == INVALID_PAGE_ID) {
      return INDEXITERATOR_TYPE(0, 0, 0, nullptr, nullptr);
    }
    ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
    const auto *header_page = header_guard.As<BPlusTreeHeaderPage>();
    page_id_t root_page_id = header_page->root_page_id_;
    if (INVALID_PAGE_ID == root_page_id) {
      return INDEXITERATOR_TYPE(0, 0, 0, nullptr, nullptr);
    }
    // get root page
    WritePageGuard pg_guard = bpm_->FetchPageWrite(root_page_id);
    header_guard.Drop();
    auto *b_page = pg_guard.AsMut<class BPlusTreePage>();
    // loop and found value
    while (!b_page->IsLeafPage()) {
      auto *bi_page = reinterpret_cast<InternalPage *>(b_page);
      int i = bi_page->GetIndexLargerThanKey(1, key, comparator_);
      auto n_pid = static_cast<page_id_t>(bi_page->ValueAt(i - 1));
      if (n_pid == INVALID_PAGE_ID || n_pid == 0) {
        return INDEXITERATOR_TYPE(0, 0, 0, nullptr, nullptr);
      }
      if (0 == n_pid) {
        return INDEXITERATOR_TYPE(0, 0, 0, nullptr, nullptr);
      }
      pg_guard = bpm_->FetchPageWrite(n_pid);
      b_page = pg_guard.AsMut<class BPlusTreePage>();
    }
    // get leaf node
    auto *bl_page = reinterpret_cast<LeafPage *>(b_page);
    // TODO(hksong): change it to binary search
    int i = 0;
    if (bl_page->GetIndexEqualToKey(i, key, comparator_)) {
      return INDEXITERATOR_TYPE(i, std::move(bl_page->GetSize()), reinterpret_cast<uint64_t>(this), bpm_, bl_page);
    }
  } catch (const std::exception &e) {
    std::cout << e.what() << "in begin(key)" << std::endl;
    throw e;
  }
  return INDEXITERATOR_TYPE(0, 0, 0, nullptr, nullptr);
}
// IndexIterator(int max_idx_per_page,  long long uuid, BufferPoolManager *bpm, LeafPage *start_page);
/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE(0, 0, reinterpret_cast<uint64_t>(this), bpm_, nullptr);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  if (header_page_id_ == INVALID_PAGE_ID) {
    return INVALID_PAGE_ID;
  }
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  const auto *root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" "
           "CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" "
           "CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
