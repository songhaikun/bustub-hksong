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
#include "type/type_id.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_infos_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (has_updated_ || nullptr == child_executor_) {
    return false;
  }
  int cnt = 0;
  // auto schema = child_executor_->GetOutputSchema();
  while (child_executor_->Next(tuple, rid)) {
    struct TupleMeta delete_tuple_meta {
      INVALID_PAGE_ID, INVALID_PAGE_ID, true
    };
    struct TupleMeta insert_tuple_meta {
      INVALID_PAGE_ID, INVALID_PAGE_ID, false
    };
    // change ori state (delete)
    table_info_->table_->UpdateTupleMeta(delete_tuple_meta, *rid);

    // insert new tuple
    std::vector<Value> values;
    values.reserve(GetOutputSchema().GetColumnCount());
    for (auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    auto insert_tuple = Tuple(values, &child_executor_->GetOutputSchema());

    auto rid1 = table_info_->table_->InsertTuple(insert_tuple_meta, insert_tuple, exec_ctx_->GetLockManager(),
                                                 exec_ctx_->GetTransaction(), plan_->TableOid());
    if (std::nullopt == rid1) {
      table_info_->table_->UpdateTupleMeta(insert_tuple_meta, *rid);
      LOG_DEBUG("error occurred while inserting tuple");
      continue;
    }
    // update the index
    for (auto index_info : index_infos_) {
      auto key = tuple->KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
                                     index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
    }
    for (auto index_info : index_infos_) {
      auto key = insert_tuple.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
                                           index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key, rid1.value(), exec_ctx_->GetTransaction());
    }
    cnt++;
  }
  std::vector<Value> values{};
  values.emplace_back(TypeId::INTEGER, cnt);
  *tuple = {values, &plan_->OutputSchema()};
  has_updated_ = true;
  return true;
}

}  // namespace bustub
