#ifndef TABLE_H
#define TABLE_H

#include "db.h"
#include "arraylist.h"
#include "db.h"

extern ArrayList tables;

Table* get_table_by_id(uint8_t table_id);
Table* find_table(const char* table_name);
void init_tables(void);
void free_table_internal(void* ptr);
void copy_row(Row* dst, const Row* src, int dst_value_offset);
void print_table_header(const TableDef* schema);
void print_table_separator(int column_count);
void print_row_data(const Row* row, const TableDef* schema);
void copy_row(Row* dst, const Row* src, int column_count);
Row* create_row(int initial_capacity);
void free_row(Row* row);
int resize_row(Row* row, int new_capacity);
Table* create_table(const char* name, int initial_row_capacity);
void free_table(Table* table);
int resize_table(Table* table, int new_capacity);
void init_tables(void);
void free_table_internal(void* ptr);
void index_table_column(const char* table_name, const char* column_name, const char* index_name);
void drop_index_by_name(const char* index_name);
Value copy_value(const Value* src);
void free_value(void* ptr);
bool check_not_null_constraint(Table* table, int col_idx, Value* val);
bool check_unique_constraint(Table* table, int col_idx, Value* val, int exclude_row_idx);
bool check_foreign_key_constraint(Table* table, int col_idx, Value* val);

#endif
