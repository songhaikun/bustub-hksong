#include <sstream>
#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_page.h"

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
  // get root page
  page_id_t root_page_id = GetRootPageId();
  if (root_page_id == INVALID_PAGE_ID) {
    return false;
  }
  BasicPageGuard basic_pg_guard = bpm_->FetchPageBasic(root_page_id);
  const auto *b_page = basic_pg_guard.As<class BPlusTreePage>();

  // loop and found value
  while (!b_page->IsLeafPage()) {
    const auto *bi_page = reinterpret_cast<const InternalPage *>(b_page);
    int i = 1;
    // TODO(hksong): change it to binary search
    for (; i < bi_page->GetSize(); ++i) {
      if (comparator_(bi_page->KeyAt(i), key) > 0) {
        break;
      }
    }
    auto n_pid = static_cast<page_id_t>(bi_page->ValueAt(i - 1));
    if (n_pid == INVALID_PAGE_ID) {
      return false;
    }
    basic_pg_guard = bpm_->FetchPageBasic(n_pid);
    b_page = basic_pg_guard.As<class BPlusTreePage>();
  }
  // get leaf node
  const auto *bl_page = reinterpret_cast<const LeafPage *>(b_page);
  // TODO(hksong): change it to binary search
  for (int i = 0; i < bl_page->GetSize(); ++i) {
    if (comparator_(bl_page->KeyAt(i), key) == 0) {
      result->emplace_back(std::move(bl_page->ValueAt(i)));
      return true;
    }
  }
  return false;
}

/**
 * @brief insert / split the leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertLeafPage(Context &ctx, MappingType &mapping, bool &need_split_root, Transaction *txn)
    -> bool {
  BUSTUB_ASSERT(!ctx.write_set_.empty(), "write set shouldn't empty");
  WritePageGuard guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto *b_page = guard.AsMut<LeafPage>();
  BUSTUB_ASSERT(b_page->IsLeafPage(), "error insert LeafPage");
  // find idx to insert
  int i = 0;
  // TODO(hksong): change it to binary search
  // FindIdxToInsert(int &idx, pageid, leaf or internal, key) -> bool;
  for (; i < b_page->GetSize(); ++i) {
    if (comparator_(b_page->KeyAt(i), mapping.first) == 0) {
      return false;
    }
    if (comparator_(b_page->KeyAt(i), mapping.first) > 0) {
      break;
    }
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
      b_page->InsertKeyAndValueAt(i, mapping.first, mapping.second);
      mid_key = b_page->KeyAt(mid_idx);
      for (int i = mid_idx; i < b_page->GetMaxSize(); ++i) {
        new_page->InsertKeyAndValueAt(i - mid_idx, b_page->KeyAt(i), b_page->ValueAt(i));
      }
      std::cout << new_page->GetSize() << std::endl;
      new_page->InsertKeyAndValueAt(b_page->GetMaxSize() - mid_idx, record_key, record_value);
    } else {
      for (int i = mid_idx; i < b_page->GetMaxSize(); ++i) {
        new_page->InsertKeyAndValueAt(i - mid_idx, b_page->KeyAt(i), b_page->ValueAt(i));
      }
      new_page->InsertKeyAndValueAt(i - mid_idx, mapping.first, mapping.second);
    }
    b_page->IncreaseSize(mid_idx - b_page->GetMaxSize());
    // std::cout << new_page->GetSize() << ',' << b_page->GetSize() << std::endl;
    BUSTUB_ASSERT(new_page->GetSize() == b_page->GetSize(), "new page size should equal ori page size");
    ctx.last_page_id_ = guard.PageId();
    b_page->SetNextPageId(new_page_guard.PageId());
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
    // 1,2,3,4 insert:0 -> 0,1,2 + 3,4
    for (int i = mid_idx; i < b_page->GetMaxSize(); ++i) {
      new_page->InsertKeyAndValueAt(i - mid_idx, b_page->KeyAt(i), b_page->ValueAt(i));
    }
    new_page->InsertKeyAndValueAt(b_page->GetMaxSize() - mid_idx, record_key, record_value);
  } else {
    for (int i = mid_idx; i < b_page->GetMaxSize(); ++i) {
      new_page->InsertKeyAndValueAt(i - mid_idx, b_page->KeyAt(i), b_page->ValueAt(i));
    }
    new_page->InsertKeyAndValueAt(i - mid_idx, mapping.first, mapping.second);
  }
  b_page->IncreaseSize(mid_idx - b_page->GetMaxSize());
  BUSTUB_ASSERT(new_page->GetSize() == b_page->GetSize() + 1, "new page size minus 1 should equal ori page size");
  ctx.last_page_id_ = guard.PageId();
  b_page->SetNextPageId(new_page_guard.PageId());
  return InsertInternalPage(ctx, mid_key, new_page_guard.PageId(), need_split_root, txn);
}

/**
 * @brief split the internal page
 */

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertInternalPage(Context &ctx, KeyType &key, page_id_t value, bool &need_split_root,
                                        Transaction *txn) -> bool {
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
      ctx.UpdateRootPage(new_root_page_id);
    }
    return true;
  }
  WritePageGuard guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto *b_page = guard.AsMut<InternalPage>();
  BUSTUB_ASSERT(b_page->IsInternalPage(), "error insert internal page");
  int i = 1;
  // TODO(hksong): change it to binary search
  for (; i < b_page->GetSize(); ++i) {
    if (comparator_(b_page->KeyAt(i), key) == 0) {
      return false;
    }
    if (comparator_(b_page->KeyAt(i), key) > 0) {
      break;
    }
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
      for (int i = mid_idx + 1; i < b_page->GetMaxSize(); ++i) {
        new_page->InsertKeyAndValueAt(i - mid_idx, b_page->KeyAt(i), b_page->ValueAt(i));
      }
      new_page->InsertKeyAndValueAt(b_page->GetMaxSize() - mid_idx, record_key, record_value);
    } else {
      for (int i = mid_idx + 1; i < b_page->GetMaxSize(); ++i) {
        new_page->InsertKeyAndValueAt(i - mid_idx, b_page->KeyAt(i), b_page->ValueAt(i));
      }
      new_page->InsertKeyAndValueAt(i - mid_idx, key, value);
    }
    b_page->IncreaseSize(mid_idx - b_page->GetMaxSize());
    new_page->SetValueAt(0, mid_value);
    BUSTUB_ASSERT(new_page->GetSize() == b_page->GetSize(), "new page size should equal ori page size");
    ctx.last_page_id_ = guard.PageId();
    return InsertInternalPage(ctx, mid_key, new_internal_page_guard.PageId(), need_split_root, txn);
  }
  // b_page->GetMaxSize() % 2 == 0
  int mid_idx = b_page->GetMaxSize() / 2;
  KeyType mid_key = b_page->KeyAt(mid_idx);
  page_id_t mid_value = b_page->ValueAt(mid_idx);
  if (i <= mid_idx) {
    KeyType record_key = b_page->KeyAt(b_page->GetMaxSize() - 1);
    page_id_t record_value = b_page->ValueAt(b_page->GetMaxSize() - 1);
    b_page->InsertKeyAndValueAt(i, key, value);
    mid_key = b_page->KeyAt(mid_idx);
    mid_value = b_page->ValueAt(mid_idx);
    for (int i = mid_idx + 1; i < b_page->GetMaxSize(); ++i) {
      new_page->InsertKeyAndValueAt(i - mid_idx, b_page->KeyAt(i), b_page->ValueAt(i));
    }
    new_page->InsertKeyAndValueAt(b_page->GetMaxSize() - mid_idx + 1, record_key, record_value);
  } else {
    for (int i = mid_idx + 1; i < b_page->GetMaxSize(); ++i) {
      new_page->InsertKeyAndValueAt(i - mid_idx, b_page->KeyAt(i), b_page->ValueAt(i));
    }
    new_page->InsertKeyAndValueAt(i - mid_idx, key, value);
  }
  b_page->IncreaseSize(mid_idx - b_page->GetMaxSize());
  new_page->SetValueAt(0, mid_value);
  std::cout << new_page->GetSize() << ',' << b_page->GetSize() << std::endl;
  BUSTUB_ASSERT(new_page->GetSize() == b_page->GetSize() + 1, "new page size plus 1 should equal ori page size");
  ctx.last_page_id_ = guard.PageId();
  return InsertInternalPage(ctx, mid_key, new_internal_page_guard.PageId(), need_split_root, txn);
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
  page_id_t root_page_id = GetRootPageId();
  // new page
  if (INVALID_PAGE_ID == root_page_id) {
    page_id_t pid = -1;
    auto guard = bpm_->NewPageGuarded(&pid);
    if (-1 == pid) {
      LOG_DEBUG("b_plus_tree: error in insert");
      return false;
    }
    // change header page
    WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
    auto *hpg = header_guard.AsMut<BPlusTreeHeaderPage>();
    hpg->root_page_id_ = pid;
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
  ctx.header_page_ = bpm_->FetchPageWrite(header_page_id_);
  ctx.root_page_id_ = root_page_id;

  WritePageGuard guard = bpm_->FetchPageWrite(root_page_id);
  auto *b_page = guard.AsMut<class BPlusTreePage>();
  ctx.write_set_.push_back(std::move(guard));

  while (!b_page->IsLeafPage()) {
    auto *bi_page = reinterpret_cast<InternalPage *>(b_page);
    int i = 1;
    // TODO(hksong): change it to binary search
    for (; i < bi_page->GetSize(); ++i) {
      if (comparator_(bi_page->KeyAt(i), key) > 0) {
        break;
      }
    }
    auto n_pid = static_cast<page_id_t>(bi_page->ValueAt(i - 1));
    BUSTUB_ASSERT(n_pid != INVALID_PAGE_ID, "get invalid page id");
    // record sth
    guard = bpm_->FetchPageWrite(n_pid);
    b_page = guard.AsMut<class BPlusTreePage>();
    // safe insert
    if (b_page->GetSize() < b_page->GetMaxSize()) {
      ctx.write_set_.clear();
      ctx.header_page_ = std::nullopt;
      need_split_root = false;
    }
    ctx.write_set_.push_back(std::move(guard));
  }
  MappingType kv_map(key, value);
  return InsertLeafPage(ctx, kv_map, need_split_root, txn);
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
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
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
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

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
