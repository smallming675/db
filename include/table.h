#ifndef TABLE_H
#define TABLE_H

#include "db.h"

extern Table tables[MAX_TABLES];
extern int table_count;

Table* get_table(const char* name);
void exec_create_table(const IRNode* current);
void exec_insert_row(const IRNode* current);
void exec_scan_table(const IRNode* current);
void exec_drop_table(const IRNode* current);
void print_table_header(const TableDef* schema);
void print_table_separator(int column_count);
void print_row_data(const Row* row, const TableDef* schema);

#endif