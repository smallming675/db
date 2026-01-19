#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "arraylist.h"
#include "db.h"
#include "logger.h"
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
        char* name = *(char**)alist_get(&result->column_names, j);
        col_widths[j].width = get_str_width(name);
        col_widths[j].is_numeric = true;
    }

    int row_count = alist_length(&result->rows);
    for (int i = 0; i < row_count; i++) {
        for (int j = 0; j < col_count; j++) {
            Value* val = (Value*)alist_get(&result->values, i * col_count + j);
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

void print_table_separator(ColumnWidth* col_widths, int col_count, int style) {
    if (style == 0) {
        printf("+");
        for (int j = 0; j < col_count; j++) {
            for (int i = 0; i < col_widths[j].width + 2; i++) {
                printf("-");
            }
            if (j < col_count - 1) {
                printf("+");
            }
        }
        printf("+\n");
    } else if (style == 1) {
        printf("\u250C");
        for (int j = 0; j < col_count; j++) {
            for (int i = 0; i < col_widths[j].width + 2; i++) {
                printf("\u2500");
            }
            if (j < col_count - 1) {
                printf("\u252C");
            }
        }
        printf("\u2510\n");
    } else if (style == 2) {
        printf("\u251C");
        for (int j = 0; j < col_count; j++) {
            for (int i = 0; i < col_widths[j].width + 2; i++) {
                printf("\u2500");
            }
            if (j < col_count - 1) {
                printf("\u253C");
            }
        }
        printf("\u2524\n");
    } else {
        printf("\u2514");
        for (int j = 0; j < col_count; j++) {
            for (int i = 0; i < col_widths[j].width + 2; i++) {
                printf("\u2500");
            }
            if (j < col_count - 1) {
                printf("\u2534");
            }
        }
        printf("\u2518\n");
    }
}

void print_table_header(QueryResult* result, ColumnWidth* col_widths, int col_count) {
    printf("\u2502");
    for (int j = 0; j < col_count; j++) {
        char* name = *(char**)alist_get(&result->column_names, j);
        int name_width = get_str_width(name);
        int padding = col_widths[j].width - name_width;
        int left_pad = padding / 2;
        printf(" %*s%s%*s ", left_pad, "", name, padding - left_pad, "");
        printf("\u2502");
    }
    printf("\n");
}

void print_row_data(QueryResult* result, ColumnWidth* col_widths, int col_count, int row_idx) {
    printf("\u2502");
    for (int j = 0; j < col_count; j++) {
        Value* val = (Value*)alist_get(&result->values, row_idx * col_count + j);
        const char* str = val->type == TYPE_NULL ? "NULL" : repr(val);
        int str_width = get_str_width(str);
        int padding = col_widths[j].width - str_width;
        int left_pad = padding / 2;
        printf(" %*s%s%*s ", left_pad, "", str, padding - left_pad, "");
        printf("\u2502");
    }
    printf("\n");
}

void print_pretty_result(QueryResult* result) {
    if (!result || alist_length(&result->rows) == 0) {
        return;
    }

    int col_count = result->col_count;
    ColumnWidth* col_widths = malloc(col_count * sizeof(ColumnWidth));
    if (!col_widths) return;

    calculate_column_widths(result, col_widths, col_count);

    printf("\n");
    print_table_separator(col_widths, col_count, 1);

    print_table_header(result, col_widths, col_count);
    print_table_separator(col_widths, col_count, 2);

    int row_count = alist_length(&result->rows);
    for (int i = 0; i < row_count; i++) {
        print_row_data(result, col_widths, col_count, i);
    }

    print_table_separator(col_widths, col_count, 3);

    free(col_widths);
}
