#ifndef VALUES_H
#define VALUES_H

#include "db.h"

extern const Value VAL_NULL;
extern const Value VAL_ERROR;

bool is_null(const Value* val);
const char* repr(const Value* val);
bool eval_comparison(Value left, Value right, OperatorType op);
void update_aggregate_state(AggState* state, const Value* val, const IRAggregate* agg);
Value compute_aggregate(AggFuncType func_type, AggState* state, DataType return_type);

#endif