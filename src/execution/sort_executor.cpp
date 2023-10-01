#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)),
      obs_(plan_->GetOrderBy()) {}

void SortExecutor::Init() {
  child_executor_->Init();
  // build sorted vector of keys
  Tuple child_tuple;
  RID child_rid;
  tuple_vec_.clear();
  // get all child tuples
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    tuple_vec_.push_back(child_tuple);
  }
  std::sort(tuple_vec_.begin(), tuple_vec_.end(), [&](Tuple &a, Tuple &b) -> bool {
    // use order_bys
    for (auto [otype, expr] : obs_) {
      if (otype == OrderByType::DEFAULT || otype == OrderByType::ASC) {
        if (expr->Evaluate(&a, child_executor_->GetOutputSchema()).CompareLessThan(
            expr->Evaluate(&b, child_executor_->GetOutputSchema())) == CmpBool::CmpTrue) {
          return true;
        } else if (expr->Evaluate(&a, child_executor_->GetOutputSchema()).CompareGreaterThan(
            expr->Evaluate(&b, child_executor_->GetOutputSchema())) == CmpBool::CmpTrue) {
          return false;
        } else {
          continue;
        }
      } else if (otype == OrderByType::DESC) {
        if (expr->Evaluate(&a, child_executor_->GetOutputSchema()).CompareLessThan(
            expr->Evaluate(&b, child_executor_->GetOutputSchema())) == CmpBool::CmpTrue) {
          return false;
        } else if (expr->Evaluate(&a, child_executor_->GetOutputSchema()).CompareGreaterThan(
            expr->Evaluate(&b, child_executor_->GetOutputSchema())) == CmpBool::CmpTrue) {
          return true;
        } else {
          continue;
        }
      } else {
        return false;
      }
    }
    return false;
  });
  iterator_ = tuple_vec_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iterator_ != tuple_vec_.end()) {
    *tuple = *iterator_;
    *rid = iterator_->GetRid();
    ++iterator_;
    return true;
  }
  return false;
}
} // namespace bustub
