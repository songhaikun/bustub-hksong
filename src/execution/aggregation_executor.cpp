//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "common/config.h"
#include "common/rid.h"
#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  // build hash table
  while (child_->Next(&tuple, &rid)) {
    auto agg_key = MakeAggregateKey(&tuple);
    auto agg_value = MakeAggregateValue(&tuple);
    aht_.InsertCombine(agg_key, agg_value);
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // probe hash table
  Schema schema(plan_->OutputSchema());
  if (aht_iterator_ != aht_.End()) {
    auto values = aht_iterator_.Key().group_bys_;
    for (auto &vec : aht_iterator_.Val().aggregates_) {
      values.push_back(vec);
    }
    *tuple = {values, &schema};
    ++aht_iterator_;
    has_agg_ = true;
    return true;
  }
  // empty table, return 0 + null, null...
  if (!has_agg_ && plan_->group_bys_.empty()) {
    std::vector<Value> values;
    for (auto agg_type : plan_->agg_types_) {
      switch (agg_type) {
        case AggregationType::CountStarAggregate:
          values.push_back(ValueFactory::GetIntegerValue(0));
          break;
        case AggregationType::CountAggregate:
        case AggregationType::SumAggregate:
        case AggregationType::MinAggregate:
        case AggregationType::MaxAggregate:
          values.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
          break;
      }
    }
    *tuple = {values, &schema};
    has_agg_ = true;
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
