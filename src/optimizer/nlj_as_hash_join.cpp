#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"
#include <algorithm>
#include <memory>

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan)
    -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>

  std::vector<AbstractPlanNodeRef> children;

  for (const auto &it : plan->children_) {
    children.push_back(OptimizeSortLimitAsTopN(it));
  }
  
  auto res_plan = plan->CloneWithChildren(children);
  const std::shared_ptr<const NestedLoopJoinPlanNode> &ptr = std::dynamic_pointer_cast<const NestedLoopJoinPlanNode>(plan);
  if (nullptr == ptr) {
    return res_plan;
  }
  AbstractPlanNodeRef left_plan = OptimizeNLJAsHashJoin(ptr->GetLeftPlan());
  AbstractPlanNodeRef right_plan = OptimizeNLJAsHashJoin(ptr->GetRightPlan());

  std::vector<AbstractExpressionRef> left_key_expressions;
  std::vector<AbstractExpressionRef> right_key_expressions;


  std::vector<AbstractExpressionRef> vector = ptr->Predicate()->GetChildren();  // 得到谓词的孩子节点

  for (auto &it : vector) {
    const std::shared_ptr<ColumnValueExpression> col_expr = std::dynamic_pointer_cast<ColumnValueExpression>(it);
    if (col_expr) {
      if (col_expr->GetTupleIdx() == 0) {
        left_key_expressions.push_back(it);
      } else {
        right_key_expressions.push_back(it);
      }
    } else {
      const std::shared_ptr<ComparisonExpression> com_expr = std::dynamic_pointer_cast<ComparisonExpression>(it);
      if (com_expr) {
        for (uint8_t j = 0; j < 2; ++j) {
          const std::shared_ptr<ColumnValueExpression> col_expr_tmp
              = std::dynamic_pointer_cast<ColumnValueExpression>(com_expr->GetChildAt(j));
          if (col_expr_tmp->GetTupleIdx() == 0) {
            left_key_expressions.push_back(col_expr_tmp);
          } else {
            right_key_expressions.push_back(col_expr_tmp);
          }
        }
      }
    }
  }
  return std::make_shared<HashJoinPlanNode>(ptr->output_schema_,left_plan, right_plan, left_key_expressions,
                                            right_key_expressions, ptr->GetJoinType());
}

} // namespace bustub
