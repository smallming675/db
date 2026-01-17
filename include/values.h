#ifndef VALUES_H
#define VALUES_H

#include "db.h"

extern const Value VAL_NULL;
extern const Value VAL_ERROR;

bool is_null(const Value* val);
const char* repr(const Value* val);
bool eval_comparison(Value left, Value right, OperatorType op);
Value compute_aggregate(AggFuncType func_type, AggState* state, DataType return_type);
Value convert_value(const Value* val, DataType target_type);
bool try_convert_value(const Value* val, DataType target_type, Value* out_result);

#endif