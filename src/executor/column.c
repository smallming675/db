#include <string.h>

#include "arraylist.h"
#include "db.h"
#include "logger.h"

static int find_column_index(const TableDef* schema, const char* column_name);

static Value get_column_value_by_index(const Row* row, const TableDef* schema, int col_idx) {
    (void)schema;
    Value val = {0};
    val.type = TYPE_NULL;

    if (col_idx >= 0 && col_idx < alist_length(row)) {
        Value* row_val = (Value*)alist_get(row, col_idx);
        if (row_val) {
            val = *row_val;
        }
    }
    return val;
}

int find_column_index(const TableDef* schema, const char* column_name) {
    int col_count = alist_length(&schema->columns);
    for (int i = 0; i < col_count; i++) {
        ColumnDef* col = (ColumnDef*)alist_get(&schema->columns, i);
        if (col && strcmp(col->name, column_name) == 0) {
            return i;
        }
    }
    return -1;
}

Value get_column_value(const Row* row, const TableDef* schema, const char* column_name) {
    int idx = find_column_index(schema, column_name);
    if (idx >= 0) {
        return get_column_value_by_index(row, schema, idx);
    }
    log_msg(LOG_WARN, "get_column_value: Column '%s' not found in schema", column_name);
    Value val = {0};
    val.type = TYPE_NULL;
    return val;
}

Value get_column_value_from_join(const Row* row, const TableDef* left_schema, 
                                  const TableDef* right_schema, int left_col_count,
                                  const char* column_name) {
    int left_idx = find_column_index(left_schema, column_name);
    if (left_idx >= 0 && left_idx < left_col_count) {
        return get_column_value_by_index(row, left_schema, left_idx);
    }

    int right_idx = find_column_index(right_schema, column_name);
    if (right_idx >= 0) {
        int actual_idx = left_col_count + right_idx;
        return get_column_value_by_index(row, NULL, actual_idx);
    }

    Value val = {0};
    val.type = TYPE_NULL;
    return val;
}
