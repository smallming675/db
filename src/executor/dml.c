#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "arraylist.h"
#include "db.h"
#include "executor.h"
#include "executor_internal.h"
#include "logger.h"
#include "table.h"
#include "values.h"

static bool insert_row_with_columns(Table* table, ArrayList* value_row, int schema_col_count,
                                    int value_count, ArrayList* columns) {
    Row* row = (Row*)alist_append(&table->rows);
    if (!row) return false;

    alist_init(row, sizeof(Value), free_value);

    for (int i = 0; i < schema_col_count; i++) {
        Value* val = (Value*)alist_append(row);
        val->type = TYPE_NULL;
    }

    for (int i = 0; i < value_count; i++) {
        int* col_idx_ptr = (int*)alist_get(columns, i);
        int col_idx = col_idx_ptr ? *col_idx_ptr : i;
        if (col_idx < 0 || col_idx >= schema_col_count) continue;

        ColumnValue* cv = (ColumnValue*)alist_get(value_row, i);
        if (!cv) continue;

        Value* val = (Value*)alist_get(row, col_idx);
        if (!val) continue;

        Value new_val = copy_string_value(&cv->value);
        *val = new_val;

        if (!check_foreign_key_constraint(table, col_idx, val)) {
            log_msg(LOG_ERROR, "INSERT aborted due to foreign key constraint violation");
            alist_destroy(row);
            alist_remove(&table->rows, alist_length(&table->rows) - 1);
            return false;
        }
    }

    return true;
}

static bool insert_row_without_columns(Table* table, ArrayList* value_row, int value_count) {
    Row* row = (Row*)alist_append(&table->rows);
    if (!row) return false;

    alist_init(row, sizeof(Value), free_value);

    for (int i = 0; i < value_count; i++) {
        ColumnValue* cv = (ColumnValue*)alist_get(value_row, i);
        if (!cv) continue;

        Value* val = (Value*)alist_append(row);
        *val = copy_string_value(&cv->value);

        if (!check_foreign_key_constraint(table, i, val)) {
            log_msg(LOG_ERROR, "INSERT aborted due to foreign key constraint violation");
            alist_destroy(row);
            alist_remove(&table->rows, alist_length(&table->rows) - 1);
            return false;
        }
    }

    return true;
}

void exec_insert_row_ast(ASTNode* ast) {
    InsertNode* ins = &ast->insert;
    Table* table = get_table_by_id(ins->table_id);
    if (!table) {
        log_msg(LOG_ERROR, "Table with ID %d not found", ins->table_id);
        return;
    }

    int schema_col_count = alist_length(&table->schema.columns);
    int row_count = alist_length(&ins->value_rows);
    if (row_count == 0) return;

    int specified_col_count = alist_length(&ins->columns);
    bool has_columns = specified_col_count > 0;

    int inserted = 0;
    for (int r = 0; r < row_count; r++) {
        ArrayList* value_row = (ArrayList*)alist_get(&ins->value_rows, r);
        if (!value_row) continue;

        int value_count = alist_length(value_row);
        if (value_count == 0) continue;

        bool success;
        if (has_columns) {
            success = insert_row_with_columns(table, value_row, schema_col_count, value_count,
                                              &ins->columns);
        } else {
            success = insert_row_without_columns(table, value_row, value_count);
        }

        if (success) inserted++;
    }

    log_msg(LOG_INFO, "Inserted %d rows into table '%s'", inserted, table->name);
}

void exec_update_row_ast(ASTNode* ast) {
    UpdateNode* update = &ast->update;
    Table* table = get_table_by_id(update->table_id);
    if (!table) return;

    int updated = 0;
    int row_count = alist_length(&table->rows);
    for (int i = 0; i < row_count; i++) {
        Row* row = (Row*)alist_get(&table->rows, i);
        if (!row) continue;

        if (!eval_expression(update->where_clause, row, &table->schema)) continue;
        for (int j = 0; j < alist_length(&update->values); j++) {
            ColumnValue* cv = (ColumnValue*)alist_get(&update->values, j);
            if (!(cv && cv->column_idx >= 0)) continue;
            Value* row_val = (Value*)alist_get(row, cv->column_idx);
            Value new_val = copy_string_value(&cv->value);

            if (!check_foreign_key_constraint(table, cv->column_idx, &new_val)) {
                log_msg(LOG_ERROR, "UPDATE aborted due to foreign key constraint violation");
                free_value(&new_val);
                return;
            }

            free_value(row_val);
            *row_val = new_val;
        }
        updated++;
    }

    log_msg(LOG_INFO, "Updated %d rows in table '%s'", updated, table->name);
}

void exec_delete_row_ast(ASTNode* ast) {
    DeleteNode* del = &ast->delete;
    Table* table = get_table_by_id(del->table_id);
    if (!table) return;

    ArrayList kept_rows;
    alist_init(&kept_rows, sizeof(Row), free_row_contents);

    int deleted_rows = 0;
    int row_count = alist_length(&table->rows);
    for (int i = 0; i < row_count; i++) {
        Row* row = (Row*)alist_get(&table->rows, i);
        if (!row) continue;

        if (eval_expression(del->where_clause, row, &table->schema)) {
            deleted_rows++;
        } else {
            Row* kept = (Row*)alist_append(&kept_rows);
            if (!kept) continue;
            alist_init(kept, sizeof(Value), free_value);
            int src_len = alist_length(row);
            for (int k = 0; k < src_len; k++) {
                Value* src_val = (Value*)alist_get(row, k);
                Value* dst_val = (Value*)alist_append(kept);
                *dst_val = copy_string_value(src_val);
            }
        }
    }

    alist_destroy(&table->rows);
    table->rows = kept_rows;

    log_msg(LOG_INFO, "Deleted %d rows from table '%s'", deleted_rows, table->name);
}
