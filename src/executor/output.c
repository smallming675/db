#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "arraylist.h"
#include "db.h"
#include "values.h"

typedef struct {
    int width;
    bool is_numeric;
} ColumnWidth;

static int get_str_width(const char* str) {
    int len = 0;
    for (int i = 0; str && str[i]; i++) {
        unsigned char c = (unsigned char)str[i];
        if (c < 0x80) {
            len++;
        } else if ((c & 0xE0) == 0xC0) {
            len++;
            i++;
        } else if ((c & 0xF0) == 0xE0) {
            len++;
            i += 2;
        } else if ((c & 0xF8) == 0xF0) {
            len++;
            i += 3;
        }
    }
    return len;
}

static void calculate_column_widths(QueryResult* result, ColumnWidth* col_widths, int col_count) {
    for (int j = 0; j < col_count; j++) {
        col_widths[j].width = get_str_width(result->column_names[j]);
        col_widths[j].is_numeric = true;
    }

    for (int i = 0; i < result->row_count; i++) {
        for (int j = 0; j < col_count; j++) {
            const Value* val = &result->values[i * col_count + j];
            const char* str = val->type == TYPE_NULL ? "NULL" : repr(val);
            int str_width = get_str_width(str);
            bool is_numeric = (val->type == TYPE_INT || val->type == TYPE_FLOAT);
            col_widths[j].is_numeric = col_widths[j].is_numeric && is_numeric;

            if (str_width > col_widths[j].width) {
                col_widths[j].width = str_width;
            }
        }
    }
}

void print_table_separator(ColumnWidth* col_widths, int col_count) {
    printf("├");
    for (int j = 0; j < col_count; j++) {
        for (int i = 0; i < col_widths[j].width + 2; i++) {
            printf("─");
        }
        if (j < col_count - 1) {
            printf("┼");
        }
    }
    printf("┤\n");
}

void print_table_header(QueryResult* result, ColumnWidth* col_widths, int col_count) {
    printf("│");
    for (int j = 0; j < col_count; j++) {
        const char* name = result->column_names[j];
        int name_width = get_str_width(name);
        int padding = col_widths[j].width - name_width;
        int left_pad = padding / 2;
        printf(" %*s%s%*s ", left_pad, "", name, padding - left_pad, "");
        printf("│");
    }
    printf("\n");
}

void print_row_data(QueryResult* result, ColumnWidth* col_widths, int col_count, int row_idx) {
    printf("│");
    for (int j = 0; j < col_count; j++) {
        const Value* val = &result->values[row_idx * col_count + j];
        const char* str = val->type == TYPE_NULL ? "NULL" : repr(val);
        int str_width = get_str_width(str);
        int padding = col_widths[j].width - str_width;

        if (col_widths[j].is_numeric) {
            int left_pad = padding / 2;
            printf(" %*s%s%*s ", left_pad, "", str, padding - left_pad, "");
        } else {
            printf(" %s%*s ", str, padding, "");
        }
        printf("│");
    }
    printf("\n");
}

void print_result_row(const Value* values, int col_count) {
    for (int j = 0; j < col_count; j++) {
        const Value* val = &values[j];
        printf("%-20s", val->type == TYPE_NULL ? "NULL" : repr(val));
    }
    printf("\n");
}

void print_column_headers(SelectNode* select, Table* table, bool is_select_star, int col_count) {
    if (is_select_star) {
        for (int j = 0; j < col_count; j++) {
            ColumnDef* col = (ColumnDef*)alist_get(&table->schema.columns, j);
            printf("%-20s", col ? col->name : "unknown");
        }
    } else {
        for (int i = 0; i < col_count; i++) {
            Expr** expr = (Expr**)alist_get(&select->expressions, i);
            const char* alias = expr[0]->alias;
            if (alias[0]) {
                printf("%-20s", alias);
            } else if (expr[0]->type == EXPR_COLUMN) {
                printf("%-20s", expr[0]->column_name);
            } else {
                printf("%-20s", "expr");
            }
        }
    }
    printf("\n");
}

void print_pretty_result(QueryResult* result) {
    if (!result || result->row_count == 0) {
        return;
    }

    int col_count = result->col_count;
    ColumnWidth* col_widths = malloc(col_count * sizeof(ColumnWidth));
    if (!col_widths) return;

    calculate_column_widths(result, col_widths, col_count);

    printf("\n");
    printf("┌");
    for (int j = 0; j < col_count; j++) {
        for (int i = 0; i < col_widths[j].width + 2; i++) {
            printf("─");
        }
        if (j < col_count - 1) {
            printf("┬");
        }
    }
    printf("┐\n");

    print_table_header(result, col_widths, col_count);
    print_table_separator(col_widths, col_count);

    for (int i = 0; i < result->row_count; i++) {
        print_row_data(result, col_widths, col_count, i);
    }

    printf("└");
    for (int j = 0; j < col_count; j++) {
        for (int i = 0; i < col_widths[j].width + 2; i++) {
            printf("─");
        }
        if (j < col_count - 1) {
            printf("┴");
        }
    }
    printf("┘\n");

    free(col_widths);
}
