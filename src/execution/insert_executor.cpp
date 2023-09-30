//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"

namespace bustub {

InsertExecutor::InsertExecutor(
    ExecutorContext *exec_ctx, const InsertPlanNode *plan,
    std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_infos_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (has_inserted_) {
    return false;
  }
  int cnt = 0;
  while (child_executor_->Next(tuple, rid)) {
    struct TupleMeta tuple_meta{INVALID_PAGE_ID, INVALID_PAGE_ID, false};
    auto rid1 = table_info_->table_->InsertTuple(tuple_meta, *tuple, exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(), plan_->TableOid());
    if (!rid1) {
      continue;
    }
    // update the index
    for (auto index_info : index_infos_) {
      auto key = tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key, rid1.value(), exec_ctx_->GetTransaction());
    }
    cnt++;
  }

  std::vector<Value> values{};
  values.emplace_back(TypeId::INTEGER, cnt);
  *tuple = {values, &plan_->OutputSchema()};
  has_inserted_ = true;
  return true;
}

} // namespace bustub
