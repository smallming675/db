#include "table.h"

#include <stdio.h>
#include <strings.h>

#include "arraylist.h"
#include "db.h"
#include "logger.h"
#include "values.h"

ArrayList tables;
ArrayList indexes;
static int next_table_id = 1;

static void free_index(void* ptr);

static void free_row_contents(void* ptr) {
    Row* row = (Row*)ptr;
    if (!row) return;

    for (int i = 0; i < row->value_count; i++) {
        if (row->values[i].type == TYPE_STRING && row->values[i].char_val) {
            free(row->values[i].char_val);
        }
    }

    free(row->values);
    row->values = NULL;
    row->value_count = 0;
    row->value_capacity = 0;
}

Row* create_row(int initial_capacity) {
    Row* row = malloc(sizeof(Row));
    if (!row) return NULL;

    row->value_capacity = initial_capacity > 0 ? initial_capacity : 4;
    row->values = malloc(row->value_capacity * sizeof(Value));
    row->value_count = 0;

    if (!row->values) {
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
    free(row);
}

int resize_row(Row* row, int new_capacity) {
    if (new_capacity <= 0) new_capacity = 4;

    Value* new_values = realloc(row->values, new_capacity * sizeof(Value));
    if (!new_values) return 0;

    row->values = new_values;
    row->value_capacity = new_capacity;

    return 1;
}

static void free_column_def_contents(void* ptr) {
    ColumnDef* col = (ColumnDef*)ptr;
    (void)col;
}

/* Callback function for alist_destroy() to free Table objects */
void free_table_internal(void* ptr) {
    Table* table = (Table*)ptr;
    if (!table) return;

    if (table->rows.data != NULL) {
        alist_destroy(&table->rows);
    }
    if (table->schema.columns.data != NULL) {
        alist_destroy(&table->schema.columns);
    }
}

Table* create_table(const char* name, int initial_row_capacity) {
    (void)initial_row_capacity;
    if (alist_length(&tables) >= MAX_TABLES) {
        log_msg(LOG_ERROR, "create_table: Maximum table limit (%d) reached", MAX_TABLES);
        return NULL;
    }

    /* alist_append() returns a pointer to the newly appended element */
    Table* table = (Table*)alist_append(&tables);
    strncpy(table->name, name, MAX_TABLE_NAME_LEN - 1);
    table->name[MAX_TABLE_NAME_LEN - 1] = '\0';

    table->table_id = next_table_id++;
    alist_init(&table->rows, sizeof(Row), free_row_contents);
    alist_init(&table->schema.columns, sizeof(ColumnDef), free_column_def_contents);

    return table;
}

/* Frees all resources associated with a table.
 * used when manually removing a table (not via alist_remove). */
void free_table(Table* table) {
    if (!table) return;
    if (table->rows.data != NULL) {
        alist_destroy(&table->rows);
    }
    if (table->schema.columns.data != NULL) {
        alist_destroy(&table->schema.columns);
    }
}

int resize_table(Table* table, int new_capacity) {
    (void)table;
    (void)new_capacity;
    return 1;
}

Value copy_value(const Value* src) {
    Value dst = *src;
    if (src->type == TYPE_STRING && src->char_val != NULL) {
        dst.char_val = malloc(strlen(src->char_val) + 1);
        if (dst.char_val) {
            strcpy(dst.char_val, src->char_val);
        }
    }
    return dst;
}

void free_value(void* ptr) {
    Value* val = (Value*)ptr;
    if (!val) return;
    if (val->type == TYPE_STRING && val->char_val) {
        free(val->char_val);
        val->char_val = NULL;
    }
    val->type = TYPE_NULL;
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
        if (!new_values) return;
        dst->values = new_values;
        dst->value_capacity = src->value_count;
    }

    for (int i = 0; i < src->value_count; i++) {
        dst->values[i] = copy_value(&src->values[i]);
    }
    dst->value_count = src->value_count;
}

Table* find_table(const char* name) {
    for (int i = 0; i < alist_length(&tables); i++) {
        Table* table = (Table*)alist_get(&tables, i);
        if (table && strcmp(table->name, name) == 0) {
            return table;
        }
    }
    return NULL;
}

Table* get_table(const char* name) { return find_table(name); }

Table* get_table_by_id(uint8_t table_id) {
    int count = alist_length(&tables);
    for (int i = 0; i < count; i++) {
        Table* table = (Table*)alist_get(&tables, i);
        if (table && table->table_id == table_id) {
            return table;
        }
    }
    return NULL;
}

bool value_equals(const Value* a, const Value* b) {
    if (a->type != b->type) return false;
    if (is_null(a) && is_null(b)) return true;
    if (is_null(a) || is_null(b)) return false;

    switch (a->type) {
        case TYPE_INT:
            return a->int_val == b->int_val;
        case TYPE_FLOAT:
            return a->float_val == b->float_val;
        case TYPE_STRING:
            return strcmp(a->char_val, b->char_val) == 0;
        default:
            return false;
    }
}

bool check_not_null_constraint(Table* table, int col_idx, Value* val) {
    ColumnDef* col = (ColumnDef*)alist_get(&table->schema.columns, col_idx);
    if (!col) return false;
    if (!(col->flags & COL_FLAG_NULLABLE) && is_null(val)) {
        log_msg(LOG_ERROR, "Constraint violation: NOT NULL on column '%s'", col->name);
        return false;
    }
    return true;
}

bool check_unique_constraint(Table* table, int col_idx, Value* val, int exclude_row_idx) {
    if (is_null(val)) return true;

    int row_count = alist_length(&table->rows);
    for (int i = 0; i < row_count; i++) {
        if (i == exclude_row_idx) continue;
        Row* row = (Row*)alist_get(&table->rows, i);
        ColumnDef* col = (ColumnDef*)alist_get(&table->schema.columns, col_idx);
        if (row && col && value_equals(&row->values[col_idx], val)) {
            log_msg(LOG_ERROR, "Constraint violation: UNIQUE on column '%s' (duplicate value '%s')",
                    col->name, repr(val));
            return false;
        }
    }
    return true;
}

bool check_foreign_key_constraint(Table* table, int col_idx, Value* val) {
    ColumnDef* col = (ColumnDef*)alist_get(&table->schema.columns, col_idx);
    if (!col) return false;
    if (!(col->flags & COL_FLAG_FOREIGN_KEY) || col->references_table[0] == '\0') return true;
    if (is_null(val)) return true;

    Table* ref_table = find_table(col->references_table);
    if (!ref_table) {
        log_msg(LOG_ERROR, "Constraint violation: FOREIGN KEY references non-existent table '%s'",
                col->references_table);
        return false;
    }

    int ref_col_idx = -1;
    if (col->references_column[0] != '\0') {
        int ref_col_count = alist_length(&ref_table->schema.columns);
        for (int i = 0; i < ref_col_count; i++) {
            ColumnDef* ref_col = (ColumnDef*)alist_get(&ref_table->schema.columns, i);
            if (ref_col && strcmp(ref_col->name, col->references_column) == 0) {
                ref_col_idx = i;
                break;
            }
        }
    } else {
        ref_col_idx = 0;
    }

    if (ref_col_idx < 0) {
        log_msg(
            LOG_ERROR,
            "Constraint violation: FOREIGN KEY references non-existent column '%s' in table '%s'",
            col->references_column, col->references_table);
        return false;
    }

    int ref_row_count = alist_length(&ref_table->rows);
    for (int i = 0; i < ref_row_count; i++) {
        Row* ref_row = (Row*)alist_get(&ref_table->rows, i);
        ColumnDef* ref_col = (ColumnDef*)alist_get(&ref_table->schema.columns, ref_col_idx);
        if (ref_row && ref_col && value_equals(&ref_row->values[ref_col_idx], val)) {
            return true;
        }
    }

    ColumnDef* ref_col = (ColumnDef*)alist_get(&ref_table->schema.columns, ref_col_idx);
    log_msg(LOG_ERROR,
            "Constraint violation: FOREIGN KEY on column '%s' (value '%s' not found in %s.%s)",
            col->name, repr(val), col->references_table, ref_col ? ref_col->name : "unknown");
    return false;
}

void init_tables(void) {
    if (tables.data == NULL) {
        alist_init(&tables, sizeof(Table), free_table_internal);
    }
    if (indexes.data == NULL) {
        alist_init(&indexes, sizeof(Index), free_index);
    }
}

/* Hash function for Values - used by index implementation.
 * Converts a Value to a hash code for bucket indexing.
 * Returns a value between 0 and bucket_count-1. */
int hash_value(const Value* value, int bucket_count) {
    if (!value || bucket_count <= 0) return 0;

    unsigned long hash = 0;
    switch (value->type) {
        case TYPE_INT:
            hash = (unsigned long)(value->int_val);
            break;
        case TYPE_FLOAT:
            hash = (unsigned long)(value->float_val * 1000.0);
            break;
        case TYPE_STRING:
            if (value->char_val) {
                const char* p = value->char_val;
                while (*p) {
                    hash = hash * 31 + *p++;
                }
            }
            break;
        case TYPE_TIME:
            hash = value->time_val.time_val;
            break;
        case TYPE_DATE:
            hash = value->date_val.date_val;
            break;
        default:
            hash = 0;
    }

    return (int)(hash % (unsigned long)bucket_count);
}

/* Free an index entry */
static void free_index_entry(IndexEntry* entry) {
    if (!entry) return;

    if (entry->key.type == TYPE_STRING && entry->key.char_val) {
        free(entry->key.char_val);
    }
    free_index_entry(entry->next);
    free(entry);
}

/* Free an index structure and all its entries */
static void free_index(void* ptr) {
    Index* index = (Index*)ptr;
    if (!index) return;

    if (index->buckets) {
        for (int i = 0; i < index->bucket_count; i++) {
            if (index->buckets[i]) {
                free_index_entry(index->buckets[i]);
            }
        }
        free(index->buckets);
    }
    log_msg(LOG_DEBUG, "free_index: Index '%s' freed", index->index_name);
}

/* Find an index by name */
Index* find_index(const char* index_name) {
    if (!index_name) return NULL;

    int index_count = alist_length(&indexes);
    for (int i = 0; i < index_count; i++) {
        Index* idx = (Index*)alist_get(&indexes, i);
        if (idx && strcmp(idx->index_name, index_name) == 0) {
            return idx;
        }
    }
    return NULL;
}

/* Create an index on a table column */
void index_table_column(const char* table_name, const char* column_name, const char* index_name) {
    if (!table_name || !column_name) return;

    Table* table = get_table(table_name);
    if (!table) {
        log_msg(LOG_ERROR, "index_table_column: Table '%s' not found", table_name);
        return;
    }

    int column_idx = -1;
    int column_count = alist_length(&table->schema.columns);
    for (int i = 0; i < column_count; i++) {
        ColumnDef* col = (ColumnDef*)alist_get(&table->schema.columns, i);
        if (col && strcasecmp(col->name, column_name) == 0) {
            column_idx = i;
            break;
        }
    }

    if (column_idx < 0) {
        log_msg(LOG_ERROR, "index_table_column: Column '%s' not found in table '%s'", column_name,
                table_name);
        return;
    }

    char idx_name[MAX_TABLE_NAME_LEN];
    if (index_name && index_name[0] != '\0') {
        snprintf(idx_name, sizeof(idx_name), "%s", index_name);
    } else {
        snprintf(idx_name, sizeof(idx_name), "idx_%s_%s", table_name, column_name);
    }

    Index* existing = find_index(idx_name);
    if (existing) {
        log_msg(LOG_INFO, "index_table_column: Index '%s' already exists, rebuilding", idx_name);
        int index_count = alist_length(&indexes);
        for (int i = 0; i < index_count; i++) {
            Index* idx = (Index*)alist_get(&indexes, i);
            if (idx && strcmp(idx->index_name, idx_name) == 0) {
                alist_remove(&indexes, i);
                break;
            }
        }
    }

    Index* index = (Index*)malloc(sizeof(Index));
    if (!index) {
        log_msg(LOG_ERROR, "index_table_column: Failed to allocate index");
        return;
    }

    memset(index, 0, sizeof(Index));
    snprintf(index->index_name, sizeof(index->index_name), "%s", idx_name);
    snprintf(index->table_name, sizeof(index->table_name), "%s", table_name);
    snprintf(index->column_name, sizeof(index->column_name), "%s", column_name);
    index->bucket_count = 64;
    index->buckets = calloc(index->bucket_count, sizeof(IndexEntry*));
    index->entry_count = 0;

    if (!index->buckets) {
        log_msg(LOG_ERROR, "index_table_column: Failed to allocate index buckets");
        free(index);
        return;
    }

    int row_count = alist_length(&table->rows);
    for (int i = 0; i < row_count; i++) {
        Row* row = (Row*)alist_get(&table->rows, i);
        if (!row || row->value_count <= column_idx) continue;

        IndexEntry* entry = malloc(sizeof(IndexEntry));
        if (!entry) continue;

        entry->key = copy_value(&row->values[column_idx]);
        entry->row_index = i;
        entry->next = NULL;

        int bucket = hash_value(&entry->key, index->bucket_count);
        entry->next = index->buckets[bucket];
        index->buckets[bucket] = entry;
        index->entry_count++;
    }

    Index* stored = (Index*)alist_append(&indexes);
    *stored = *index;

    log_msg(LOG_INFO, "index_table_column: Created index '%s' on '%s.%s' with %d entries",
            index_name, table_name, column_name, index->entry_count);
}

/* Drop an index by name */
void drop_index_by_name(const char* index_name) {
    if (!index_name) return;

    int index_count = alist_length(&indexes);
    for (int i = 0; i < index_count; i++) {
        Index* idx = (Index*)alist_get(&indexes, i);
        if (idx && strcmp(idx->index_name, index_name) == 0) {
            alist_remove(&indexes, i);
            log_msg(LOG_INFO, "drop_index_by_name: Index '%s' dropped", index_name);
            return;
        }
    }

    log_msg(LOG_ERROR, "drop_index_by_name: Index '%s' not found", index_name);
}
