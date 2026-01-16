#include "table.h"

#include "db.h"
#include "logger.h"
#include "values.h"

ArrayList tables;

static Table* find_table(const char* name);

Row* create_row(int initial_capacity) {
    Row* row = malloc(sizeof(Row));
    if (!row) return NULL;

    row->value_capacity = initial_capacity > 0 ? initial_capacity : 4;
    row->values = malloc(row->value_capacity * sizeof(Value));
    row->is_null = malloc(row->value_capacity * sizeof(bool));
    row->value_count = 0;

    if (!row->values || !row->is_null) {
        /* Clean up on failure */
        free(row->values);
        free(row->is_null);
        free(row);
        return NULL;
    }

    return row;
}

void free_row(Row* row) {
    if (!row) return;

    /* Free string values */
    for (int i = 0; i < row->value_count; i++) {
        if (row->values[i].type == TYPE_STRING && row->values[i].char_val) {
            free(row->values[i].char_val);
        }
    }

    free(row->values);
    free(row->is_null);
    free(row);
}

int resize_row(Row* row, int new_capacity) {
    if (new_capacity <= 0) new_capacity = 4;

    Value* new_values = realloc(row->values, new_capacity * sizeof(Value));
    bool* new_is_null = realloc(row->is_null, new_capacity * sizeof(bool));
    if (!new_values || !new_is_null) return 0;

    row->values = new_values;
    row->is_null = new_is_null;
    row->value_capacity = new_capacity;

    return 1;
}

/* Callback function for alist_destroy() to free Table objects */
void free_table_internal(void* ptr) {
    Table* table = (Table*)ptr;
    if (!table) return;

    for (int i = 0; i < table->row_count; i++) {
        Row* row = &table->rows[i];
        /* Free any allocated string values */
        for (int j = 0; j < row->value_count; j++) {
            if (row->values[j].type == TYPE_STRING && row->values[j].char_val) {
                free(row->values[j].char_val);
            }
        }
        free(row->values);
        free(row->is_null);
    }
    free(table->rows);
}

Table* create_table(const char* name, int initial_row_capacity) {
    if (alist_length(&tables) >= MAX_TABLES) {
        log_msg(LOG_ERROR, "create_table: Maximum table limit (%d) reached", MAX_TABLES);
        return NULL;
    }

    /* alist_append() returns a pointer to the newly appended element */
    Table* table = (Table*)alist_append(&tables);
    strncpy(table->name, name, MAX_TABLE_NAME_LEN - 1);
    table->name[MAX_TABLE_NAME_LEN - 1] = '\0';

    table->row_capacity = initial_row_capacity > 0 ? initial_row_capacity : 4;
    table->rows = malloc(table->row_capacity * sizeof(Row));
    table->row_count = 0;

    if (!table->rows) {
        return NULL;
    }

    for (int i = 0; i < table->row_capacity; i++) {
        table->rows[i].values = NULL;
        table->rows[i].is_null = NULL;
        table->rows[i].value_count = 0;
        table->rows[i].value_capacity = 0;
    }

    return table;
}

/* Frees all resources associated with a table. 
 * used when manually removing a table (not via alist_remove). */
void free_table(Table* table) {
    if (!table) return;
    for (int i = 0; i < table->row_count; i++) {
        free_row(&table->rows[i]);
    }
    free(table->rows);
}

int resize_table(Table* table, int new_capacity) {
    if (new_capacity <= 0) new_capacity = 4;

    Row* new_rows = realloc(table->rows, new_capacity * sizeof(Row));
    if (!new_rows) return 0;

    table->rows = new_rows;
    table->row_capacity = new_capacity;

    for (int i = table->row_count; i < table->row_capacity; i++) {
        table->rows[i].values = NULL;
        table->rows[i].is_null = NULL;
        table->rows[i].value_count = 0;
        table->rows[i].value_capacity = 0;
    }

    return 1;
}

static Value copy_value(const Value* src) {
    Value dst = *src;
    if (src->type == TYPE_STRING && src->char_val != NULL) {
        dst.char_val = malloc(strlen(src->char_val) + 1);
        if (dst.char_val) {
            strcpy(dst.char_val, src->char_val);
        }
    }
    return dst;
}

/* Copies all data from src row to dst row.
 * If dst's capacity is insufficient, realloc's its arrays. */
void copy_row(Row* dst, const Row* src, int column_count) {
    (void)column_count;
    if (!dst || !src) return;
    if (dst == src) return;

    /* Ensure dst has enough capacity for src's values */
    if (dst->value_capacity < src->value_count) {
        Value* new_values = realloc(dst->values, src->value_count * sizeof(Value));
        bool* new_is_null = realloc(dst->is_null, src->value_count * sizeof(bool));
        if (!new_values || !new_is_null) return;
        dst->values = new_values;
        dst->is_null = new_is_null;
        dst->value_capacity = src->value_count;
    }

    for (int i = 0; i < src->value_count; i++) {
        dst->values[i] = copy_value(&src->values[i]);
        dst->is_null[i] = src->is_null[i];
    }
    dst->value_count = src->value_count;
}

static Table* find_table(const char* name) {
    for (int i = 0; i < alist_length(&tables); i++) {
        Table* table = (Table*)alist_get(&tables, i);
        if (table && strcmp(table->name, name) == 0) {
            return table;
        }
    }
    return NULL;
}

Table* get_table(const char* name) { return find_table(name); }

void exec_create_table(const IRNode* current) {
    log_msg(LOG_DEBUG, "exec_create_table: Creating table: %s",
            current->create_table.table_def.name);

    if (find_table(current->create_table.table_def.name)) {
        log_msg(LOG_ERROR, "exec_create_table: Table '%s' already exists",
                current->create_table.table_def.name);
        return;
    }

    Table* table = create_table(current->create_table.table_def.name, 4);
    if (!table) {
        log_msg(LOG_ERROR, "exec_create_table: Failed to allocate table");
        return;
    }

    table->schema = current->create_table.table_def;

    log_msg(LOG_INFO, "exec_create_table: Table '%s' created with %d columns", table->name,
            table->schema.column_count);
}

void exec_insert_row(const IRNode* current) {
    log_msg(LOG_DEBUG, "exec_insert_row: Inserting row into table: %s",
            current->insert_row.table_name);

    Table* table = find_table(current->insert_row.table_name);
    if (!table) {
        char suggestion[256];
        int table_count = alist_length(&tables);
        const char* table_names[table_count > 0 ? table_count : 1];
        for (int i = 0; i < table_count; i++) {
            Table* t = (Table*)alist_get(&tables, i);
            table_names[i] = t ? t->name : "";
        }
        suggest_similar(current->insert_row.table_name, table_names, table_count, suggestion,
                        sizeof(suggestion));
        show_prominent_error("Table " COLOR_YELLOW "%s" COLOR_RESET " does not exist!",
                             current->insert_row.table_name);
        if (strlen(suggestion) > 0) {
            printf("  %s\n", suggestion);
        }
        return;
    }

    if (current->insert_row.value_count != table->schema.column_count) {
        log_msg(LOG_ERROR, "exec_insert_row: Column count mismatch (expected %d, got %d)",
                table->schema.column_count, current->insert_row.value_count);
        return;
    }

    /* Grow the table's row array */
    if (table->row_count >= table->row_capacity) {
        if (!resize_table(table, table->row_capacity * 2)) {
            log_msg(LOG_ERROR, "exec_insert_row: Failed to resize table");
            return;
        }
    }

    Row* row = &table->rows[table->row_count];

    if (row->value_capacity < current->insert_row.value_count) {
        if (!resize_row(row, current->insert_row.value_count)) {
            log_msg(LOG_ERROR, "exec_insert_row: Failed to resize row");
            return;
        }
    }

    for (int i = 0; i < current->insert_row.value_count; i++) {
        row->values[i] = copy_value(&current->insert_row.values[i]);
        row->is_null[i] = current->insert_row.values[i].type == TYPE_NULL;
    }
    row->value_count = current->insert_row.value_count;

    log_msg(LOG_INFO, "exec_insert_row: Row inserted into table '%s' (row %d)",
            current->insert_row.table_name, table->row_count + 1);
    table->row_count++;
}

void exec_scan_table(const IRNode* current) {
    log_msg(LOG_DEBUG, "exec_scan_table: Scanning table: %s", current->scan_table.table_name);

    Table* table = find_table(current->scan_table.table_name);
    if (!table) {
        char suggestion[256];
        int table_count = alist_length(&tables);
        const char* table_names[table_count > 0 ? table_count : 1];
        for (int i = 0; i < table_count; i++) {
            Table* t = (Table*)alist_get(&tables, i);
            table_names[i] = t ? t->name : "";
        }
        suggest_similar(current->scan_table.table_name, table_names, table_count, suggestion,
                        sizeof(suggestion));
        show_prominent_error("Table " COLOR_YELLOW "%s" COLOR_RESET " does not exist",
                             current->scan_table.table_name);
        if (strlen(suggestion) > 0) {
            printf("  %s\n", suggestion);
        }
        return;
    }
}

void exec_drop_table(const IRNode* current) {
    log_msg(LOG_DEBUG, "exec_drop_table: Dropping table: %s", current->drop_table.table_name);

    int table_index = -1;
    int table_count = alist_length(&tables);
    for (int i = 0; i < table_count; i++) {
        Table* t = (Table*)alist_get(&tables, i);
        if (t && strcmp(t->name, current->drop_table.table_name) == 0) {
            table_index = i;
            break;
        }
    }

    if (table_index < 0) {
        char suggestion[256];
        const char* table_names[table_count > 0 ? table_count : 1];
        for (int i = 0; i < table_count; i++) {
            Table* t = (Table*)alist_get(&tables, i);
            table_names[i] = t ? t->name : "";
        }
        suggest_similar(current->drop_table.table_name, table_names, table_count, suggestion,
                        sizeof(suggestion));
        show_prominent_error("Table " COLOR_YELLOW "%s" COLOR_RESET " does not exist",
                             current->drop_table.table_name);
        if (strlen(suggestion) > 0) {
            printf("  %s\n", suggestion);
        }
        return;
    }

    alist_remove(&tables, table_index);

    log_msg(LOG_INFO, "exec_drop_table: Table '%s' dropped", current->drop_table.table_name);
}

void print_table_header(const TableDef* schema) {
    printf("┌");
    for (int i = 0; i < schema->column_count; i++) {
        for (int j = 0; j < 20; j++) {
            printf("─");
        }
        if (i < schema->column_count - 1) {
            printf("┬");
        }
    }
    printf("┐\n");

    printf("│");
    for (int i = 0; i < schema->column_count; i++) {
        int len = strlen(schema->columns[i].name);
        int padding = 20 - len;
        int left_pad = padding / 2;
        printf("%*s%s%*s", left_pad, "", schema->columns[i].name, 20 - left_pad - len, "");
        printf("│");
    }
    printf("\n");

    printf("├");
    for (int i = 0; i < schema->column_count; i++) {
        for (int j = 0; j < 20; j++) {
            printf("─");
        }
        if (i < schema->column_count - 1) {
            printf("┼");
        }
    }
    printf("┤\n");
}

void print_table_separator(int column_count) {
    printf("├");
    for (int i = 0; i < column_count; i++) {
        for (int j = 0; j < 20; j++) {
            printf("─");
        }
        if (i < column_count - 1) {
            printf("┼");
        }
    }
    printf("┤\n");
}

void print_row_data(const Row* row, const TableDef* schema) {
    printf("│");
    for (int col_idx = 0; col_idx < schema->column_count; col_idx++) {
        if (row->is_null[col_idx]) {
            printf("%9s%10s", "", "NULL");
        } else {
            const char* val_str = repr(&row->values[col_idx]);
            int len = strlen(val_str);
            int padding = 20 - len;
            int left_pad = padding / 2;
            printf("%*s%s%*s", left_pad, "", val_str, 20 - left_pad - len, "");
        }
        printf("│");
    }
    printf("\n");
}

void init_tables(void) {
    if (tables.data == NULL) {
        alist_init(&tables, sizeof(Table), free_table_internal);
    }
}
