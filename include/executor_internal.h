#ifndef EXECUTOR_INTERNAL_H
#define EXECUTOR_INTERNAL_H

#include "db.h"

Value get_column_value(const Row* row, const TableDef* schema, const char* column_name);
Value get_column_value_from_join(const Row* row, const TableDef* left_schema, 
                                  const TableDef* right_schema, int left_col_count,
                                  const char* column_name);
bool eval_expression(const Expr* expr, const Row* row, const TableDef* schema);
bool eval_expression_for_join(const Expr* expr, const Row* row, 
                              const TableDef* left_schema, 
                              const TableDef* right_schema,
                              int left_col_count);
Value eval_select_expression(Expr* expr, const Row* row, const TableDef* schema);
Value eval_scalar_function(const Expr* expr, const Row* row, const TableDef* schema);
void print_column_headers(SelectNode* select, Table* table, bool is_select_star, int col_count);
void print_result_row(const Value* values, int col_count);
void print_pretty_result(QueryResult* result);

#endif
