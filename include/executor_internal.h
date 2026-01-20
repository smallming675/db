#ifndef EXECUTOR_INTERNAL_H
#define EXECUTOR_INTERNAL_H

#include "arraylist.h"
#include "db.h"
#include "table.h"
#include "values.h"

void free_row_contents(void* ptr);
Value copy_string_value(const Value* src);

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
bool try_index_filter(Table* table, const Expr* where_expr, ArrayList* result);
void print_pretty_result(QueryResult* result);
QueryResult* get_last_query_result(void);
void set_last_query_result(QueryResult* result);

void exec_create_table_ast(ASTNode* ast);
void exec_drop_table_ast(ASTNode* ast);
void exec_create_index_ast(ASTNode* ast);
void exec_drop_index_ast(ASTNode* ast);

void exec_insert_row_ast(ASTNode* ast);
void exec_update_row_ast(ASTNode* ast);
void exec_delete_row_ast(ASTNode* ast);

uint16_t exec_join_ast(ASTNode* ast);
void exec_filter_ast(ASTNode* ast, uint8_t table_id);
void exec_aggregate_ast(ASTNode* ast, uint8_t table_id);
void exec_project_ast(ASTNode* ast, uint8_t table_id);

extern ArrayList g_agg_res;
extern bool g_in_agg_context;
extern QueryResult* g_last_result;

#endif
