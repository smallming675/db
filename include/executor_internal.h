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

Value compute_count_aggregate(Table* table, ArrayList* filtered_indices, int loop_count,
                             SelectNode* select, Expr* operand, bool count_all);
Value compute_sum_aggregate(Table* table, ArrayList* filtered_indices, int loop_count,
                           SelectNode* select, Expr* operand);
Value compute_avg_aggregate(Table* table, ArrayList* filtered_indices, int loop_count,
                           SelectNode* select, Expr* operand);
Value compute_min_aggregate(Table* table, ArrayList* filtered_indices, int loop_count,
                           SelectNode* select, Expr* operand);
Value compute_max_aggregate(Table* table, ArrayList* filtered_indices, int loop_count,
                           SelectNode* select, Expr* operand);

void setup_query_result(QueryResult** result, Table* table, SelectNode* select,
                        bool is_select_star, int col_count);
void process_aggregate_result_rows(QueryResult* result, ArrayList* g_aggregate_results,
                                  SelectNode* select, int col_count);
void process_regular_result_rows(QueryResult* result, Table* table, SelectNode* select,
                                bool is_select_star, int col_count);
void print_query_output(QueryResult* result, SelectNode* select, Table* table,
                       bool is_select_star, int col_count);

extern ArrayList g_aggregate_results;
extern bool g_in_aggregate_context;
extern QueryResult* g_last_result;

#endif
