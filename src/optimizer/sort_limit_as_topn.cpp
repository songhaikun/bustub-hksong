#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan)
    -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->children_) {
    children.push_back(OptimizeSortLimitAsTopN(child));
  }
  auto sl_to_topn_plan = plan->CloneWithChildren(children);
  if (sl_to_topn_plan->GetType() == PlanType::Limit) {
    const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*sl_to_topn_plan);
    const auto &limit = limit_plan.GetLimit();
    BUSTUB_ENSURE(limit_plan.children_.size() == 1, "Limit Plan should has exactly one child.");
    if (sl_to_topn_plan->GetChildAt(0)->GetType() == PlanType::Sort) {
      const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*sl_to_topn_plan->GetChildAt(0));
      const auto &order_bys = sort_plan.GetOrderBy();
      BUSTUB_ENSURE(sort_plan.children_.size() == 1, "Sort Plan should has exactly one child.");
      return std::make_shared<TopNPlanNode>(limit_plan.output_schema_, sort_plan.GetChildAt(0), order_bys, limit);
    }
  }
  return sl_to_topn_plan;
}
}  // namespace bustub
