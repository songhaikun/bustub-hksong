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
  uint64_t cnt = 0;
  // auto schema = child_executor_->GetOutputSchema();
  while (child_executor_->Next(tuple, rid)) {
    struct TupleMeta tuple_meta{INVALID_PAGE_ID, INVALID_PAGE_ID, false};
    if (table_info_->table_->InsertTuple(tuple_meta, *tuple, exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(), plan_->TableOid())) {
      cnt++;
    } else {
      LOG_DEBUG("insert tuple error");
    }
    // update the index
    for (auto index_info : index_infos_) {
      // auto key = tuple->KeyFromTuple(schema, *index_info->index_->GetKeySchema(), index_info->index_->GetKeyAttrs());
      if (!index_info->index_->InsertEntry(*tuple, *rid, exec_ctx_->GetTransaction())) {
        LOG_DEBUG("insert index error");
      }
    }
  }
  Value value(BIGINT, cnt);
  auto return_schema = Schema(std::vector<Column>{{"success_insert_count", TypeId::BIGINT}});
  std::vector<Value> values{};
  values.push_back(value);
  *tuple = {values, &return_schema};
  // first return true, then return false as has no child
  has_inserted_ = true;
  return true;
}

} // namespace bustub
