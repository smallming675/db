#include "db.h"
#include "logger.h"
#include "values.h"
#include "table.h"

Table tables[MAX_TABLES];
int table_count = 0;

static Table* find_table(const char* name);

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

/* avoid dangling pointers */
void copy_row(Row* dst, const Row* src, int column_count) {
    for (int i = 0; i < column_count; i++) {
        dst->values[i] = copy_value(&src->values[i]);
        dst->is_null[i] = src->is_null[i];
    }
}

static Table* find_table(const char* name) {
    for (int i = 0; i < table_count; i++) {
        if (strcmp(tables[i].name, name) == 0) {
            return &tables[i];
        }
    }
    return NULL;
}

Table* get_table(const char* name) {
    return find_table(name);
}

void exec_create_table(const IRNode* current) {
    log_msg(LOG_DEBUG, "exec_create_table: Creating table: %s",
            current->create_table.table_def.name);

    if (table_count >= MAX_TABLES) {
        log_msg(LOG_ERROR, "exec_create_table: Maximum number of tables reached");
        log_msg(LOG_ERROR, "exec_create_table: Cannot create table '%s': maximum tables reached",
                current->create_table.table_def.name);
        return;
    }

    if (find_table(current->create_table.table_def.name)) {
        log_msg(LOG_ERROR, "exec_create_table: Table '%s' already exists",
                current->create_table.table_def.name);
        log_msg(LOG_ERROR, "exec_create_table: Cannot create table '%s': table already exists",
                current->create_table.table_def.name);
        return;
    }

    Table* table = &tables[table_count];
    strcpy(table->name, current->create_table.table_def.name);
    table->schema = current->create_table.table_def;
    table->row_count = 0;

    log_msg(LOG_INFO, "exec_create_table: Table '%s' created with %d columns", table->name,
            table->schema.column_count);
    table_count++;
}

void exec_insert_row(const IRNode* current) {
    log_msg(LOG_DEBUG, "exec_insert_row: Inserting row into table: %s",
            current->insert_row.table_name);

    Table* table = find_table(current->insert_row.table_name);
    if (!table) {
        char suggestion[256];
        const char *table_names[MAX_TABLES];
        for (int i = 0; i < table_count; i++) {
            table_names[i] = tables[i].name;
        }
        suggest_similar(current->insert_row.table_name, table_names, table_count, suggestion, sizeof(suggestion));
        show_prominent_error("Table '%s' does not exist", current->insert_row.table_name);
        if (strlen(suggestion) > 0) {
            printf("  %s\n", suggestion);
        }
        return;
    }

    if (table->row_count >= MAX_ROWS) {
        log_msg(LOG_ERROR, "exec_insert_row: Table is full");
        log_msg(LOG_ERROR, "exec_insert_row: Cannot insert into table '%s': table is full",
                current->insert_row.table_name);
        return;
    }

    if (current->insert_row.value_count != table->schema.column_count) {
        log_msg(LOG_ERROR, "exec_insert_row: Column count mismatch (expected %d, got %d)",
                table->schema.column_count, current->insert_row.value_count);
        log_msg(LOG_ERROR,
                "exec_insert_row: Cannot insert into table '%s': column count mismatch "
                "(expected %d, got %d)",
                current->insert_row.table_name, table->schema.column_count,
                current->insert_row.value_count);
        return;
    }

    Row* row = &table->rows[table->row_count];
    for (int i = 0; i < current->insert_row.value_count; i++) {
        row->values[i] = copy_value(&current->insert_row.values[i]);
        row->is_null[i] = is_null(&current->insert_row.values[i]);
    }

    log_msg(LOG_INFO, "exec_insert_row: Row inserted into table '%s' (row %d)",
            current->insert_row.table_name, table->row_count + 1);
    table->row_count++;
}

/* Validate table exists */
void exec_scan_table(const IRNode* current) {
    log_msg(LOG_DEBUG, "exec_scan_table: Scanning table: %s", current->scan_table.table_name);

    Table* table = find_table(current->scan_table.table_name);
    if (!table) {
        char suggestion[256];
        const char *table_names[MAX_TABLES];
        for (int i = 0; i < table_count; i++) {
            table_names[i] = tables[i].name;
        }
        suggest_similar(current->scan_table.table_name, table_names, table_count, suggestion, sizeof(suggestion));
        show_prominent_error("Table '%s' does not exist", current->scan_table.table_name);
        if (strlen(suggestion) > 0) {
            printf("  %s\n", suggestion);
        }
        return;
    }
}

void exec_drop_table(const IRNode* current) {
    log_msg(LOG_DEBUG, "exec_drop_table: Dropping table: %s", current->drop_table.table_name);

    Table* table = find_table(current->drop_table.table_name);
    if (!table) {
        char suggestion[256];
        const char *table_names[MAX_TABLES];
        for (int i = 0; i < table_count; i++) {
            table_names[i] = tables[i].name;
        }
        suggest_similar(current->drop_table.table_name, table_names, table_count, suggestion, sizeof(suggestion));
        show_prominent_error("Table '%s' does not exist", current->drop_table.table_name);
        if (strlen(suggestion) > 0) {
            printf("  %s\n", suggestion);
        }
        return;
    }

    int table_index = table - tables;
    for (int i = table_index; i < table_count - 1; i++) {
        tables[i] = tables[i + 1];
    }
    table_count--;

    log_msg(LOG_INFO, "exec_drop_table: Table '%s' dropped", current->drop_table.table_name);
    log_msg(LOG_INFO, "exec_drop_table: Table '%s' dropped successfully",
            current->drop_table.table_name);
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
