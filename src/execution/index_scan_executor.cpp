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
  if (!iter_->IsEnd()) {
    *rid = (*(*iter_)).second;
    iter_->operator++();
    return true;
  }
  return false;
}
}  // namespace bustub
