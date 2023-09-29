//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(
    ExecutorContext *exec_ctx, const DeletePlanNode *plan,
    std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_infos_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (has_deleted_) {
    return false;
  }
  uint64_t cnt = 0;
  while (child_executor_->Next(tuple, rid)) {
    struct TupleMeta tuple_meta{INVALID_PAGE_ID, INVALID_PAGE_ID, true};
    // change ori state
    table_info_->table_->UpdateTupleMeta(tuple_meta, *rid);
    for (auto index_info : index_infos_) {
      index_info->index_->DeleteEntry(*tuple, *rid, exec_ctx_->GetTransaction());
    }
    cnt++;
  }
  Value value(BIGINT, cnt);
  auto return_schema = Schema(std::vector<Column>{{"success_delete_count", TypeId::BIGINT}});
  std::vector<Value> values{};
  values.push_back(value);
  *tuple = {values, &return_schema};
  has_deleted_ = true;
  return true;
}

} // namespace bustub
