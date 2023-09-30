//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(
    ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
    std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_infos_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (has_updated_) {
    return false;
  }
  uint64_t cnt = 0;
  auto schema = child_executor_->GetOutputSchema();
  while (child_executor_->Next(tuple, rid)) {
    struct TupleMeta delete_tuple_meta{INVALID_PAGE_ID, INVALID_PAGE_ID, true};
    struct TupleMeta insert_tuple_meta{INVALID_PAGE_ID, INVALID_PAGE_ID, false};
    // change ori state (delete)
    table_info_->table_->UpdateTupleMeta(delete_tuple_meta, *rid);
    for (auto index_info : index_infos_) {
      auto key = tuple->KeyFromTuple(schema, *index_info->index_->GetKeySchema(), index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
    }
    // insert new tuple
    std::vector<Value> values;
    values.reserve(GetOutputSchema().GetColumnCount());
    for (auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    auto insert_tuple = Tuple(values, &child_executor_->GetOutputSchema());
    auto rid1 = table_info_->table_->InsertTuple(insert_tuple_meta, insert_tuple, exec_ctx_->GetLockManager(),
                                     exec_ctx_->GetTransaction(), plan_->TableOid());
    // update the index 
    for (auto index_info : index_infos_) {
      auto key = insert_tuple.KeyFromTuple(schema, *index_info->index_->GetKeySchema(), index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key, rid1.value(), exec_ctx_->GetTransaction());
    }
    cnt++;
  }
  Value value(BIGINT, cnt);
  std::vector<Value> values{};
  values.push_back(value);
  auto return_schema = Schema(std::vector<Column>{{"success_update_count", TypeId::BIGINT}});
  *tuple = {values, &return_schema};
  has_updated_ = true;
  return true;
}

} // namespace bustub
