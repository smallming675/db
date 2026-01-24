#include "db.h"
#include "values.h"
#include "table.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "logger.h"

ArrayList table_stats;

void init_stat(void) {
    alist_init(&table_stats, sizeof(TableStats), NULL);
}

void update_column_stats(TableStats *stats, int col_idx, const Value *value) {
    if (!stats || !value || col_idx < 0 || col_idx >= stats->column_count)
        return;

    ColumnStats *col_stats = &stats->column_stats[col_idx];
    col_stats->row_count++;

    if (!is_null(value)) {
        if (!col_stats->has_stats || compare_values(value, &col_stats->min_val) < 0) {
            col_stats->min_val = copy_value(value);
        }

        if (!col_stats->has_stats || compare_values(value, &col_stats->max_val) > 0) {
            col_stats->max_val = copy_value(value);
        }

        size_t value_size = 0;
        if (value->type == TYPE_STRING) {
            value_size = strlen(value->char_val);
        } else {
            value_size = sizeof(Value);
        }

        col_stats->avg_width =
            (col_stats->avg_width * (col_stats->row_count - 1) + value_size) / col_stats->row_count;
    }
}