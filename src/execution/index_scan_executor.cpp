//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx,
                                     const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get());
  iter_ = std::make_unique<IndexIterator<bustub::GenericKey<8>, bustub::RID, bustub::GenericComparator<8>>>(tree_->GetBeginIterator());

}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // LOG_DEBUG("IndexScan");
  if (iter_->IsEnd()) {
    return false;
  }
  *rid = (*(*iter_)).second;
  // std::cout << rid->ToString() << std::endl;
  auto page_guard = exec_ctx_->GetBufferPoolManager()->FetchPageRead(rid->GetPageId());
  auto page = page_guard.As<TablePage>();
  *tuple = page->GetTuple(*rid).second;
  iter_->operator++();
  // std::cout << "?" << std::endl;
  return true;
}
}  // namespace bustub
