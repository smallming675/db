#include "table.h"

#include <stdio.h>
#include <string.h>
#include <strings.h>

#include "arraylist.h"
#include "db.h"
#include "logger.h"
#include "utils.h"
#include "values.h"

ArrayList tables;
ArrayList indexes;
static void free_index(void *ptr);

void copy_row(Row *dst, const Row *src, int column_count) {
    if (!dst || !src)
        return;

    int src_count = alist_length(src);
    int count = column_count > 0 && column_count < src_count ? column_count : src_count;

    for (int i = 0; i < count; i++) {
        Value *src_val = (Value *)alist_get(src, i);
        Value *dst_val = (Value *)alist_append(dst);
        if (src_val && dst_val) {
            *dst_val = copy_value(src_val);
        }
    }
}

void free_table_internal(void *ptr) {
    Table *table = (Table *)ptr;
    if (!table)
        return;

    if (table->rows.data != NULL) {
        alist_destroy(&table->rows);
    }
    if (table->schema.columns.data != NULL) {
        alist_destroy(&table->schema.columns);
    }
}

void free_table(Table *table) {
    if (!table)
        return;
    if (table->rows.data != NULL) {
        alist_destroy(&table->rows);
    }
    if (table->schema.columns.data != NULL) {
        alist_destroy(&table->schema.columns);
    }
}

Value copy_value(const Value *src) {
    Value dst = *src;
    if (src->type == TYPE_STRING && src->char_val != NULL) {
        dst.char_val = malloc(strlen(src->char_val) + 1);
        if (dst.char_val) {
            strcopy(dst.char_val, strlen(src->char_val) + 1, src->char_val);
        }
    } else if (src->type == TYPE_BLOB && src->blob_val.data != NULL && src->blob_val.length > 0) {
        dst.blob_val.data = malloc(src->blob_val.length);
        if (dst.blob_val.data) {
            memcopy(dst.blob_val.data, src->blob_val.data, src->blob_val.length);
        }
    }
    return dst;
}

void free_value(void *ptr) {
    Value *val = (Value *)ptr;
    if (!val)
        return;
    if (val->type == TYPE_STRING && val->char_val) {
        free(val->char_val);
        val->char_val = NULL;
    } else if (val->type == TYPE_BLOB && val->blob_val.data) {
        free(val->blob_val.data);
        val->blob_val.data = NULL;
        val->blob_val.length = 0;
    }
    val->type = TYPE_NULL;
}

Table *find_table(const char *name) {
    ParseContext *ctx = parse_get_context();
    if (ctx && ctx->current_table && strcmp(ctx->current_table->name, name) == 0) {
        return ctx->current_table;
    }

    for (int i = 0; i < alist_length(&tables); i++) {
        Table *table = (Table *)alist_get(&tables, i);
        if (table && strcmp(table->name, name) == 0) {
            return table;
        }
    }
    return NULL;
}

uint8_t find_table_id_by_name(const char *name) {
    ParseContext *ctx = parse_get_context();
    if (ctx && ctx->current_table && strcmp(ctx->current_table->name, name) == 0) {
        return ctx->current_table->table_id;
    }

    for (int i = 0; i < alist_length(&tables); i++) {
        Table *table = (Table *)alist_get(&tables, i);
        if (table && strcmp(table->name, name) == 0) {
            return table->table_id;
        }
    }
    return 0;
}

Table *get_table(const char *name) {
    return find_table(name);
}

Table *get_table_by_id(uint8_t table_id) {
    int count = alist_length(&tables);
    for (int i = 0; i < count; i++) {
        Table *table = (Table *)alist_get(&tables, i);
        if (table && table->table_id == table_id) {
            return table;
        }
    }
    return NULL;
}

bool value_equals(const Value *a, const Value *b) {
    if (a->type != b->type)
        return false;
    if (is_null(a) && is_null(b))
        return true;
    if (is_null(a) || is_null(b))
        return false;

    switch (a->type) {
    case TYPE_INT:
        return a->int_val == b->int_val;
    case TYPE_FLOAT:
        return a->float_val == b->float_val;
    case TYPE_BOOLEAN:
        return a->bool_val == b->bool_val;
    case TYPE_DECIMAL:
        return a->decimal_val.value == b->decimal_val.value &&
               a->decimal_val.precision == b->decimal_val.precision &&
               a->decimal_val.scale == b->decimal_val.scale;
    case TYPE_BLOB:
        return a->blob_val.length == b->blob_val.length &&
               memcmp(a->blob_val.data, b->blob_val.data, a->blob_val.length) == 0;
    case TYPE_STRING:
        return strcmp(a->char_val, b->char_val) == 0;
    default:
        return false;
    }
}

bool check_not_null_constraint(Table *table, int col_idx, Value *val) {
    ColumnDef *col = (ColumnDef *)alist_get(&table->schema.columns, col_idx);
    if (!col)
        return false;
    if (!(col->flags & COL_FLAG_NULLABLE) && is_null(val)) {
        log_msg(LOG_ERROR, "Constraint violation: NOT NULL on column '%s'", col->name);
        return false;
    }
    return true;
}

bool check_unique_constraint(Table *table, int col_idx, Value *val, int exclude_row_idx) {
    if (is_null(val))
        return true;

    int row_count = alist_length(&table->rows);
    for (int i = 0; i < row_count; i++) {
        if (i == exclude_row_idx)
            continue;
        Row *row = (Row *)alist_get(&table->rows, i);
        ColumnDef *col = (ColumnDef *)alist_get(&table->schema.columns, col_idx);
        if (row && col) {
            Value *row_val = (Value *)alist_get(row, col_idx);
            if (row_val && value_equals(row_val, val)) {
                log_msg(LOG_ERROR,
                        "Constraint violation: UNIQUE on column '%s' (duplicate value '%s')",
                        col->name, repr(val));
                return false;
            }
        }
    }
    return true;
}

bool check_foreign_key_constraint(Table *table, int col_idx, Value *val) {
    ColumnDef *col = (ColumnDef *)alist_get(&table->schema.columns, col_idx);
    if (!col)
        return false;
    if (!(col->flags & COL_FLAG_FOREIGN_KEY))
        return true;
    if (is_null(val))
        return true;

    Table *ref_table = get_table_by_id(col->reference.table_id);
    if (!ref_table) {
        log_msg(LOG_ERROR, "Constraint violation: FOREIGN KEY references non-existent table_id=%d",
                col->reference.table_id);
        return false;
    }

    uint16_t ref_col_id = col->reference.column_id;

    int ref_row_count = alist_length(&ref_table->rows);
    for (int i = 0; i < ref_row_count; i++) {
        Row *ref_row = (Row *)alist_get(&ref_table->rows, i);
        if (ref_row) {
            Value *ref_val = (Value *)alist_get(ref_row, ref_col_id);
            if (ref_val && value_equals(ref_val, val)) {
                return true;
            }
        }
    }

    ColumnDef *ref_col = (ColumnDef *)alist_get(&ref_table->schema.columns, ref_col_id);
    log_msg(LOG_ERROR,
            "Constraint violation: FOREIGN KEY on column '%s' (value '%s' not found in table_id=%d col=%s)",
            col->name, repr(val), col->reference.table_id, ref_col ? ref_col->name : "unknown");
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
int hash_value(const Value *value, int bucket_count) {
    if (!value || bucket_count <= 0)
        return 0;

    unsigned long hash = 0;
    switch (value->type) {
    case TYPE_INT:
        hash = (unsigned long)(value->int_val);
        break;
    case TYPE_FLOAT:
        hash = (unsigned long)(value->float_val * 1000.0);
        break;
    case TYPE_BOOLEAN:
        hash = (unsigned long)(value->bool_val);
        break;
    case TYPE_STRING:
        if (value->char_val) {
            const char *p = value->char_val;
            while (*p) {
                hash = hash * 31 + *p++;
            }
        }
        break;
    case TYPE_TIME:
        hash = value->time_val;
        break;
    case TYPE_DATE:
        hash = value->date_val;
        break;
    default:
        hash = 0;
    }

    return (int)(hash % (unsigned long)bucket_count);
}

static void free_index_entry(IndexEntry *entry) {
    if (!entry)
        return;

    if (entry->key.type == TYPE_STRING && entry->key.char_val) {
        free(entry->key.char_val);
    }
    free_index_entry(entry->next);
    free(entry);
}

static void free_index(void *ptr) {
    Index *index = (Index *)ptr;
    if (!index)
        return;

    alist_destroy(&index->columns);

    if (index->buckets) {
        for (int i = 0; i < (int)index->bucket_count; i++) {
            if (index->buckets[i]) {
                free_index_entry(index->buckets[i]);
            }
        }
        free(index->buckets);
    }
    log_msg(LOG_DEBUG, "free_index: Index '%s' freed", index->index_name);
}

Index *find_index(const char *index_name) {
    if (!index_name)
        return NULL;

    int index_count = alist_length(&indexes);
    for (int i = 0; i < index_count; i++) {
        Index *idx = (Index *)alist_get(&indexes, i);
        if (idx && strcmp(idx->index_name, index_name) == 0) {
            return idx;
        }
    }
    return NULL;
}

static void remove_existing_index(const char *idx_name) {
    int index_count = alist_length(&indexes);
    for (int i = 0; i < index_count; i++) {
        Index *idx = (Index *)alist_get(&indexes, i);
        if (idx && strcmp(idx->index_name, idx_name) == 0) {
            alist_remove(&indexes, i);
            break;
        }
    }
}

static Index *create_and_init_index(const char *idx_name, uint8_t table_id,
                                    ArrayList *column_ids) {
    Index *index = (Index *)malloc(sizeof(Index));
    if (!index) {
        log_msg(LOG_ERROR, "index_table_column: Failed to allocate index");
        return NULL;
    }

    memclear(index, sizeof(Index));
    strcopy(index->index_name, sizeof(index->index_name), idx_name);
    index->table_id = table_id;
    alist_init(&index->columns, sizeof(uint16_t), NULL);

    int col_count = alist_length(column_ids);
    for (int i = 0; i < col_count; i++) {
        uint16_t *src_id = (uint16_t *)alist_get(column_ids, i);
        uint16_t *dst_id = (uint16_t *)alist_append(&index->columns);
        if (src_id && dst_id) {
            *dst_id = *src_id;
        }
    }

    index->bucket_count = 64;
    index->buckets = calloc(index->bucket_count, sizeof(IndexEntry *));
    index->entry_count = 0;

    if (!index->buckets) {
        log_msg(LOG_ERROR, "index_table_column: Failed to allocate index buckets");
        alist_destroy(&index->columns);
        free(index);
        return NULL;
    }

    return index;
}

static void populate_index_entries(Index *index, Table *table) {
    if (alist_length(&index->columns) == 0)
        return;

    uint16_t *col = (uint16_t *)alist_get(&index->columns, 0);
    if (!col)
        return;
    uint16_t col_id = *col;

    int row_count = alist_length(&table->rows);
    for (int i = 0; i < row_count; i++) {
        Row *row = (Row *)alist_get(&table->rows, i);
        if (!row || alist_length(row) <= col_id)
            continue;

        IndexEntry *entry = malloc(sizeof(IndexEntry));
        if (!entry)
            continue;

        Value *row_val = (Value *)alist_get(row, col_id);
        entry->key = copy_value(row_val);
        entry->row_index = i;
        entry->next = NULL;

        int bucket = hash_value(&entry->key, index->bucket_count);
        entry->next = index->buckets[bucket];
        index->buckets[bucket] = entry;
        index->entry_count++;
    }
}

void index_table(uint8_t table_id, ArrayList *column_ids, const char *index_name) {
    if (!column_ids || alist_length(column_ids) == 0)
        return;

    Table *table = get_table_by_id(table_id);
    if (!table) {
        log_msg(LOG_ERROR, "index_table: Table with ID %d not found", table_id);
        return;
    }

    char idx_name[MAX_TABLE_NAME_LEN];
    if (index_name && index_name[0] != '\0') {
        strcopy(idx_name, sizeof(idx_name), index_name);
    } else {
        uint16_t *first_col = (uint16_t *)alist_get(column_ids, 0);
        ColumnDef *col = (ColumnDef *)alist_get(&table->schema.columns, *first_col);
        string_format(idx_name, sizeof(idx_name), "idx_%s_%s", table->name,
                      col ? col->name : "unknown");
    }

    Index *existing = find_index(idx_name);
    if (existing) {
        log_msg(LOG_INFO, "index_table: Index '%s' already exists, rebuilding", idx_name);
        remove_existing_index(idx_name);
    }

    Index *index = create_and_init_index(idx_name, table_id, column_ids);
    if (!index)
        return;

    populate_index_entries(index, table);

    Index *stored = (Index *)alist_append(&indexes);
    stored->bucket_count = index->bucket_count;
    stored->buckets = index->buckets;
    stored->entry_count = index->entry_count;
    strcopy(stored->index_name, sizeof(stored->index_name), index->index_name);
    stored->table_id = index->table_id;
    alist_init(&stored->columns, sizeof(uint16_t), NULL);
    for (int i = 0; i < alist_length(&index->columns); i++) {
        uint16_t *src = (uint16_t *)alist_get(&index->columns, i);
        uint16_t *dst = (uint16_t *)alist_append(&stored->columns);
        if (src && dst)
            *dst = *src;
    }

    log_msg(LOG_INFO, "index_table: Created index '%s' on table_id=%d with %d entries", idx_name,
            table_id, index->entry_count);

    free(index);
}

/* Drop an index by name */
void drop_index_by_name(const char *index_name) {
    if (!index_name)
        return;

    int index_count = alist_length(&indexes);
    for (int i = 0; i < index_count; i++) {
        Index *idx = (Index *)alist_get(&indexes, i);
        if (idx && strcmp(idx->index_name, index_name) == 0) {
            alist_remove(&indexes, i);
            log_msg(LOG_INFO, "drop_index_by_name: Index '%s' dropped", index_name);
            return;
        }
    }

    log_msg(LOG_ERROR, "drop_index_by_name: Index '%s' not found", index_name);
}

Index *find_index_by_table_column(uint8_t table_id, uint16_t column_id) {
    int index_count = alist_length(&indexes);
    for (int i = 0; i < index_count; i++) {
        Index *idx = (Index *)alist_get(&indexes, i);
        if (idx && idx->table_id == table_id) {
            if (alist_length(&idx->columns) > 0) {
                uint16_t *first_col = (uint16_t *)alist_get(&idx->columns, 0);
                if (first_col && *first_col == column_id) {
                    return idx;
                }
            }
        }
    }
    return NULL;
}

void lookup_index_values(const Index *index, const Value *key, ArrayList *result) {
    if (!index || !key || !result)
        return;

    int bucket = hash_value(key, index->bucket_count);
    bucket = (bucket < 0) ? -bucket : bucket;
    bucket = bucket % index->bucket_count;

    for (IndexEntry *entry = index->buckets[bucket]; entry != NULL; entry = entry->next) {
        if (value_equals(&entry->key, key)) {
            int *row_idx = (int *)alist_append(result);
            if (row_idx)
                *row_idx = entry->row_index;
        }
    }
}
