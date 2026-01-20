#ifndef TABLE_H
#define TABLE_H

#include "db.h"
#include "arraylist.h"
#include "db.h"

extern ArrayList tables;

Table* get_table_by_id(uint8_t table_id);
Table* find_table(const char* table_name);
uint8_t find_table_id_by_name(const char* table_name);
void init_tables(void);
void free_table_internal(void* ptr);
void copy_row(Row* dst, const Row* src, int dst_value_offset);
Row* create_row(int initial_capacity);
void free_row(Row* row);
Table* create_table(const char* name, int initial_row_capacity);
void free_table(Table* table);
void index_table_column(const char* table_name, const char* column_name, const char* index_name);
void drop_index_by_name(const char* index_name);
Index* find_index_by_table_column(const char* table_name, const char* column_name);
void lookup_index_values(const Index* index, const Value* key, ArrayList* result);
Value copy_value(const Value* src);
void free_value(void* ptr);
bool check_not_null_constraint(Table* table, int col_idx, Value* val);
bool check_unique_constraint(Table* table, int col_idx, Value* val, int exclude_row_idx);
bool check_foreign_key_constraint(Table* table, int col_idx, Value* val);

#endif
