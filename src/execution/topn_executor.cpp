#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  // use priority queue to record data
  auto topn_comparator = [&](const Tuple &a, const Tuple &b) -> bool {
    for (auto [otype, expr] : plan_->GetOrderBy()) {
      if (expr->Evaluate(&a, child_executor_->GetOutputSchema())
              .CompareEquals(expr->Evaluate(&b, child_executor_->GetOutputSchema())) == CmpBool::CmpTrue) {
        continue;
      }
      if (otype == OrderByType::DEFAULT || otype == OrderByType::ASC) {
        return (expr->Evaluate(&a, child_executor_->GetOutputSchema())
                    .CompareLessThan(expr->Evaluate(&b, child_executor_->GetOutputSchema())) == CmpBool::CmpTrue);
      }
      if (otype == OrderByType::DESC) {
        return (expr->Evaluate(&a, child_executor_->GetOutputSchema())
                    .CompareGreaterThan(expr->Evaluate(&b, child_executor_->GetOutputSchema())) == CmpBool::CmpTrue);
      }
      return false;
    }
    return false;
  };
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(topn_comparator)> tuple_queue(topn_comparator);
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    tuple_queue.push(child_tuple);
    if (tuple_queue.size() > plan_->GetN()) {
      tuple_queue.pop();
    }
  }
  while (!tuple_queue.empty()) {
    child_tuples_.push(tuple_queue.top());
    tuple_queue.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!child_tuples_.empty()) {
    *tuple = child_tuples_.top();
    *rid = tuple->GetRid();
    child_tuples_.pop();
    return true;
  }
  return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return child_tuples_.size(); };

}  // namespace bustub
