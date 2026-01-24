#include <stdlib.h>
#include <string.h>

#include "arraylist.h"
#include "db.h"
#include "executor.h"
#include "logger.h"
#include "utils.h"
#include "table.h"

void free_row_contents(void *ptr) {
    ArrayList *row = (ArrayList *)ptr;
    if (!row)
        return;
    alist_destroy(row);
}

Value copy_string_value(const Value *src) {
    Value copy = {0};
    if (!src)
        return copy;
    copy = *src;
    if (src->type == TYPE_STRING && src->char_val) {
        copy.char_val = malloc(strlen(src->char_val) + 1);
        if (copy.char_val) {
            string_copy(copy.char_val, strlen(src->char_val) + 1, src->char_val);
        }
    }
    return copy;
}

void exec_create_table_ast(ASTNode *ast) {
    CreateTableNode *ct = &ast->create_table;

    if (find_table_id_by_name(ct->table_name) != 0) {
        log_msg(LOG_WARN, "Table '%s' already exists", ct->table_name);
        return;
    }

    if (alist_length(&tables) >= MAX_TABLES) {
        log_msg(LOG_ERROR, "create_table: Maximum table limit (%d) reached", MAX_TABLES);
        return;
    }

    Table *table = malloc(sizeof(Table));
    if (!table) {
        log_msg(LOG_ERROR, "Failed to allocate table");
        return;
    }

    string_copy(table->name, sizeof(table->name), ct->table_name);
    alist_init(&table->rows, sizeof(Row), free_row_contents);
    alist_init(&table->schema.columns, sizeof(ColumnDef), NULL);
    table->schema.strict = ct->strict;

    int col_count = alist_length(&ct->columns);
    for (int i = 0; i < col_count; i++) {
        ColumnDef *col = (ColumnDef *)alist_get(&ct->columns, i);
        if (col) {
            ColumnDef *new_col = (ColumnDef *)alist_append(&table->schema.columns);
            if (new_col)
                *new_col = *col;
        }
    }

    table->table_id = alist_length(&tables);
    Table *t = (Table *)alist_append(&tables);
    if (t)
        *t = *table;

    log_msg(LOG_INFO, "Created table '%s' with %d columns (STRICT=%s)", ct->table_name, col_count,
            ct->strict ? "true" : "false");

    free(table);
}

void exec_drop_table_ast(ASTNode *ast) {
    DropTableNode *drop = &ast->drop_table;
    Table *table = get_table_by_id(drop->table_id);
    if (!table)
        return;

    char table_name[MAX_TABLE_NAME_LEN];
    string_copy(table_name, sizeof(table_name), table->name);

    for (int i = 0; i < alist_length(&tables); i++) {
        Table *t = (Table *)alist_get(&tables, i);
        if (t && t->table_id == drop->table_id) {
            alist_remove(&tables, i);
            break;
        }
    }

    log_msg(LOG_INFO, "Dropped table '%s'", table_name);
}

void exec_create_index_ast(ASTNode *ast) {
    CreateIndexNode *ci = &ast->create_index;
    Table *table = get_table_by_id(ci->table_id);
    if (!table) {
        log_msg(LOG_ERROR, "Table not found for index creation");
        return;
    }

    const char *column_name = NULL;
    if (ci->column_idx >= 0 && ci->column_idx < alist_length(&table->schema.columns)) {
        ColumnDef *col = (ColumnDef *)alist_get(&table->schema.columns, ci->column_idx);
        if (col) {
            column_name = col->name;
        }
    }

    if (!column_name) {
        log_msg(LOG_ERROR, "Column not found for index creation");
        return;
    }

    index_table_column(table->name, column_name, ci->index_name);
}

void exec_drop_index_ast(ASTNode *ast) {
    DropIndexNode *di = &ast->drop_index;
    drop_index_by_name(di->index_name);
}
