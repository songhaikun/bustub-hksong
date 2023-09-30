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
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
  tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get());
  iter_ = std::make_unique<IndexIterator<bustub::GenericKey<8>, bustub::RID, bustub::GenericComparator<8>>>(tree_->GetBeginIterator());

}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // LOG_DEBUG("IndexScan");
  if (iter_->IsEnd()) {
    return false;
  }
  while (!iter_->IsEnd()) {
    *rid = (*(*iter_)).second;
    auto [meta, tuple_temp] = table_info_->table_->GetTuple(*rid);
    if (meta.is_deleted_) {
      iter_->operator++();
      continue;
    }
    *tuple = tuple_temp;
    iter_->operator++();
    return true;
  }
  return false;
}
}  // namespace bustub
