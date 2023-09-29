//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "catalog/catalog.h"
#include <unistd.h>

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx,
                                 const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  iter_ = std::make_unique<TableIterator>(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_->IsEnd()) {
    return false;
  }
  std::pair<TupleMeta, Tuple> tuple_pair;
  while (!iter_->IsEnd() && (tuple_pair = iter_->GetTuple()).first.is_deleted_) {
    iter_->operator++();
  }
  
  if (!iter_->IsEnd()) {
    *tuple = std::move(tuple_pair.second);
    *rid = iter_->GetRID();
    iter_->operator++();
    return true;
  }
  return false;
}

}  // namespace bustub
