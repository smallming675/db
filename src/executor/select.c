#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "arraylist.h"
#include "db.h"
#include "executor.h"
#include "executor_internal.h"
#include "logger.h"
#include "table.h"
#include "values.h"

static void copy_columns_to_result(Table* result, Table* source, int col_count);
static Row* create_joined_row(ArrayList* left, ArrayList* right, int left_cols, int right_cols);
static Row* create_left_join_null_row(ArrayList* left, int right_cols);
static void collect_filtered_indices(Table* table, SelectNode* select, int row_count,
                                     ArrayList* filtered_indices);
static void compute_aggregates(Table* table, ArrayList* filtered_indices, int loop_count,
                               SelectNode* select, int expr_count);
static Value compute_single_aggregate(Table* table, ArrayList* filtered_indices, int loop_count,
                                      SelectNode* select, AggFuncType func_type, Expr* operand,
                                      bool count_all);
static bool init_join_result_table(Table* result_table, SelectNode* select, Table* left_table,
                                   Table* right_table);
static void process_join_rows(Table* result_table, SelectNode* select, Table* left_table,
                              Table* right_table, int left_cols, int right_cols);
static void copy_result_to_global_tables(Table* result_table);
static Value compute_count_aggregate(Table* table, ArrayList* filtered_indices, int loop_count,
                                    SelectNode* select, Expr* operand, bool count_all);
static Value compute_sum_aggregate(Table* table, ArrayList* filtered_indices, int loop_count,
                                  SelectNode* select, Expr* operand);
static Value compute_avg_aggregate(Table* table, ArrayList* filtered_indices, int loop_count,
                                  SelectNode* select, Expr* operand);
static Value compute_min_aggregate(Table* table, ArrayList* filtered_indices, int loop_count,
                                  SelectNode* select, Expr* operand);
static Value compute_max_aggregate(Table* table, ArrayList* filtered_indices, int loop_count,
                                  SelectNode* select, Expr* operand);
static void setup_query_result(QueryResult** result, Table* table, SelectNode* select,
                               bool is_select_star, int col_count);
static void process_aggregate_result_rows(QueryResult* result, ArrayList* agg_results,
                                          SelectNode* select, int col_count);
static void process_regular_result_rows(QueryResult* result, Table* table, SelectNode* select,
                                        bool is_select_star, int col_count);
static void print_query_output(QueryResult* result);
static void free_string_ptr(void *ptr);

typedef struct {
    Value key;
    ArrayList rows;
} HashBucket;

typedef struct {
    HashBucket* buckets;
    int bucket_count;
    int total_rows;
} HashJoinTable;

static HashJoinTable* create_hash_table(int bucket_count) {
    HashJoinTable* ht = malloc(sizeof(HashJoinTable));
    if (!ht) return NULL;
    ht->bucket_count = bucket_count > 0 ? bucket_count : 64;
    ht->buckets = calloc(ht->bucket_count, sizeof(HashBucket));
    if (!ht->buckets) {
        free(ht);
        return NULL;
    }
    ht->total_rows = 0;
    return ht;
}

static void free_hash_table(HashJoinTable* ht) {
    if (!ht) return;
    if (ht->buckets) {
        for (int i = 0; i < ht->bucket_count; i++) {
            alist_destroy(&ht->buckets[i].rows);
        }
        free(ht->buckets);
    }
    free(ht);
}

static int get_bucket_index(const Value* key, int bucket_count) {
    int hash = hash_value(key, bucket_count);
    hash = (hash < 0) ? -hash : hash;
    return hash % bucket_count;
}

static void build_hash_table(HashJoinTable* ht, Table* table, const char* key_column,
                             const TableDef* schema) {
    int row_count = alist_length(&table->rows);
    int col_idx = -1;

    int col_count = alist_length(&schema->columns);
    for (int i = 0; i < col_count; i++) {
        ColumnDef* col = (ColumnDef*)alist_get(&schema->columns, i);
        if (col && strcasecmp(col->name, key_column) == 0) {
            col_idx = i;
            break;
        }
    }

    if (col_idx < 0) return;

    for (int i = 0; i < row_count; i++) {
        Row* row = (Row*)alist_get(&table->rows, i);
        if (!row) continue;

        Value* key_val = (Value*)alist_get(row, col_idx);
        if (!key_val) continue;

        int bucket_idx = get_bucket_index(key_val, ht->bucket_count);
        int* row_idx = (int*)alist_append(&ht->buckets[bucket_idx].rows);
        if (row_idx) *row_idx = i;
        ht->total_rows++;
    }
}

static void probe_hash_table(const HashJoinTable* ht, Table* probe_table, const char* probe_key_column,
                             const TableDef* probe_schema, Table* build_table,
                             Table* result_table, bool is_left_join, int left_cols, int right_cols) {
    int probe_col_idx = -1;
    int probe_col_count = alist_length(&probe_schema->columns);
    for (int i = 0; i < probe_col_count; i++) {
        ColumnDef* col = (ColumnDef*)alist_get(&probe_schema->columns, i);
        if (col && strcasecmp(col->name, probe_key_column) == 0) {
            probe_col_idx = i;
            break;
        }
    }

    if (probe_col_idx < 0) return;

    int probe_rows = alist_length(&probe_table->rows);

    for (int i = 0; i < probe_rows; i++) {
        Row* probe_row = (Row*)alist_get(&probe_table->rows, i);
        if (!probe_row) continue;

        Value* probe_key = (Value*)alist_get(probe_row, probe_col_idx);
        if (!probe_key) continue;

        int bucket_idx = get_bucket_index(probe_key, ht->bucket_count);
        ArrayList* bucket_rows = &ht->buckets[bucket_idx].rows;
        int bucket_row_count = alist_length(bucket_rows);

        if (bucket_row_count == 0) {
            if (is_left_join) {
                Row* new_row = create_left_join_null_row(probe_row, left_cols);
                if (new_row) {
                    Row* slot = (Row*)alist_append(&result_table->rows);
                    if (slot) {
                        memset(slot, 0, sizeof(Row));
                        alist_init(slot, sizeof(Value), free_value);
                        for (int k = 0; k < alist_length(new_row); k++) {
                            Value* src_val = (Value*)alist_get(new_row, k);
                            Value* dst_val = (Value*)alist_append(slot);
                            *dst_val = copy_string_value(src_val);
                        }
                    }
                    free(new_row);
                }
            }
            continue;
        }

        for (int j = 0; j < bucket_row_count; j++) {
            int* build_row_idx = (int*)alist_get(bucket_rows, j);
            if (!build_row_idx) continue;

            Row* build_row = (Row*)alist_get(&build_table->rows, *build_row_idx);
            if (!build_row) continue;

            Row* new_row = create_joined_row(probe_row, build_row, left_cols, right_cols);
            if (new_row) {
                Row* slot = (Row*)alist_append(&result_table->rows);
                if (slot) {
                    alist_init(slot, sizeof(Value), free_value);
                    for (int k = 0; k < alist_length(new_row); k++) {
                        Value* src_val = (Value*)alist_get(new_row, k);
                        Value* dst_val = (Value*)alist_append(slot);
                        *dst_val = copy_string_value(src_val);
                    }
                }
                free(new_row);
            }
        }
    }
}

static bool try_hash_join(Table* result_table, SelectNode* select, Table* left_table,
                          Table* right_table, int left_cols, int right_cols) {
    if (!select->join_condition) return false;

    const char* left_col = NULL;
    const char* right_col = NULL;

    Expr* left = select->join_condition->binary.left;
    Expr* right = select->join_condition->binary.right;

    if (left->type == EXPR_COLUMN && right->type == EXPR_COLUMN) {
        left_col = left->column_name;
        right_col = right->column_name;
    } else {
        return false;
    }

    int left_row_count = alist_length(&left_table->rows);
    int right_row_count = alist_length(&right_table->rows);

    bool is_left_join = (select->join_type == JOIN_LEFT);

    if (left_row_count == 0 || right_row_count == 0) {
        if (is_left_join && right_row_count == 0) {
            for (int i = 0; i < left_row_count; i++) {
                Row* left_row = (Row*)alist_get(&left_table->rows, i);
                if (!left_row) continue;
                Row* new_row = create_left_join_null_row(left_row, left_cols);
                if (new_row) {
                    Row* slot = (Row*)alist_append(&result_table->rows);
                    if (slot) {
                        memset(slot, 0, sizeof(Row));
                        alist_init(slot, sizeof(Value), free_value);
                        for (int k = 0; k < alist_length(new_row); k++) {
                            Value* src_val = (Value*)alist_get(new_row, k);
                            Value* dst_val = (Value*)alist_append(slot);
                            *dst_val = copy_string_value(src_val);
                        }
                    }
                    free(new_row);
                }
            }
        }
        return true;
    }

    HashJoinTable* ht = create_hash_table(64);
    if (!ht) return false;

    Table* build_table;
    Table* probe_table;
    const char* build_col;
    const char* probe_col;
    const TableDef* build_schema;
    const TableDef* probe_schema;

    if (left_row_count <= right_row_count) {
        build_table = left_table;
        probe_table = right_table;
        build_col = left_col;
        probe_col = right_col;
        build_schema = &left_table->schema;
        probe_schema = &right_table->schema;
    } else {
        build_table = right_table;
        probe_table = left_table;
        build_col = right_col;
        probe_col = left_col;
        build_schema = &right_table->schema;
        probe_schema = &left_table->schema;
    }

    build_hash_table(ht, build_table, build_col, build_schema);
    probe_hash_table(ht, probe_table, probe_col, probe_schema, build_table,
                     result_table, is_left_join, left_cols, right_cols);

    free_hash_table(ht);
    return true;
}

uint16_t exec_join_ast(ASTNode* ast) {
    SelectNode* select = &ast->select;
    Table* left_table = get_table_by_id(select->table_id);
    Table* right_table = get_table_by_id(select->join_table_id);

    if (!left_table || !right_table) {
        log_msg(LOG_ERROR, "JOIN failed: tables not found");
        return 0;
    }

    static uint16_t join_counter = 0;
    join_counter++;
    uint16_t result_id = join_counter;

    Table result_table;
    memset(&result_table, 0, sizeof(Table));

    if (!init_join_result_table(&result_table, select, left_table, right_table)) {
        return 0;
    }

    int left_cols = alist_length(&left_table->schema.columns);
    int right_cols = alist_length(&right_table->schema.columns);

    process_join_rows(&result_table, select, left_table, right_table, left_cols, right_cols);

    copy_result_to_global_tables(&result_table);

    log_msg(LOG_INFO, "Created join table '%s' with %d rows", result_table.name,
            alist_length(&result_table.rows));

    alist_destroy(&result_table.rows);
    alist_destroy(&result_table.schema.columns);
    return result_id;
}

static bool init_join_result_table(Table* result_table, SelectNode* select, Table* left_table,
                                   Table* right_table) {
    snprintf(result_table->name, MAX_TABLE_NAME_LEN, "_join_%d_%d", select->table_id,
             select->join_table_id);
    result_table->table_id = 0;
    alist_init(&result_table->rows, sizeof(Row), free_row_contents);
    alist_init(&result_table->schema.columns, sizeof(ColumnDef), NULL);
    result_table->schema.strict = false;

    int left_cols = alist_length(&left_table->schema.columns);
    int right_cols = alist_length(&right_table->schema.columns);

    copy_columns_to_result(result_table, left_table, left_cols);
    copy_columns_to_result(result_table, right_table, right_cols);
    return true;
}

static void process_join_rows(Table* result_table, SelectNode* select, Table* left_table,
                              Table* right_table, int left_cols, int right_cols) {
    int left_rows = alist_length(&left_table->rows);
    int right_rows = alist_length(&right_table->rows);

    if (left_rows > 0 && right_rows > 0) {
        if (try_hash_join(result_table, select, left_table, right_table, left_cols, right_cols)) {
            log_msg(LOG_DEBUG, "process_join_rows: Used hash join");
            return;
        }
    }

    for (int i = 0; i < left_rows; i++) {
        Row* left_row = (Row*)alist_get(&left_table->rows, i);
        if (!left_row) continue;

        bool had_match = false;
        for (int j = 0; j < right_rows; j++) {
            Row* right_row = (Row*)alist_get(&right_table->rows, j);
            if (!right_row) continue;

            bool match = true;
            if (select->join_condition) {
                match =
                    eval_expression_for_join(select->join_condition, left_row, &left_table->schema,
                                             &right_table->schema, left_cols);
            }

            if (match) {
                had_match = true;
                Row* new_row = create_joined_row(left_row, right_row, left_cols, right_cols);
                if (new_row) {
                    Row* slot = (Row*)alist_append(&result_table->rows);
                    if (slot) {
                        alist_init(slot, sizeof(Value), free_value);
                        int src_len = alist_length(new_row);
                        for (int k = 0; k < src_len; k++) {
                            Value* src_val = (Value*)alist_get(new_row, k);
                            Value* dst_val = (Value*)alist_append(slot);
                            *dst_val = copy_string_value(src_val);
                        }
                    }
                    free(new_row);
                }
            }
        }

        if (!had_match && select->join_type == JOIN_LEFT) {
            Row* new_row = create_left_join_null_row(left_row, right_cols);
            if (new_row) {
                Row* slot = (Row*)alist_append(&result_table->rows);
                if (slot) {
                    memset(slot, 0, sizeof(Row));
                    alist_init(slot, sizeof(Value), free_value);
                    int src_len = alist_length(new_row);
                    for (int k = 0; k < src_len; k++) {
                        Value* src_val = (Value*)alist_get(new_row, k);
                        Value* dst_val = (Value*)alist_append(slot);
                        *dst_val = copy_string_value(src_val);
                    }
                }
                free(new_row);
            }
        }
    }
}

static void copy_result_to_global_tables(Table* result_table) {
    Table* t = (Table*)alist_append(&tables);
    if (t) {
        memset(t, 0, sizeof(Table));
        snprintf(t->name, MAX_TABLE_NAME_LEN, "%s", result_table->name);
        t->table_id = result_table->table_id;
        t->schema.strict = result_table->schema.strict;
        alist_init(&t->rows, sizeof(Row), free_row_contents);
        alist_init(&t->schema.columns, sizeof(ColumnDef), NULL);
        copy_columns_to_result(t, result_table, alist_length(&result_table->schema.columns));

        int row_count = alist_length(&result_table->rows);
        for (int i = 0; i < row_count; i++) {
            Row* src_row = (Row*)alist_get(&result_table->rows, i);
            if (!src_row) continue;
            Row* dst_row = (Row*)alist_append(&t->rows);
            if (dst_row) {
                alist_init(dst_row, sizeof(Value), free_value);
                int val_count = alist_length(src_row);
                for (int j = 0; j < val_count; j++) {
                    Value* src_val = (Value*)alist_get(src_row, j);
                    Value* dst_val = (Value*)alist_append(dst_row);
                    *dst_val = copy_string_value(src_val);
                }
            }
        }
    }
}

static void copy_columns_to_result(Table* result, Table* source, int col_count) {
    for (int i = 0; i < col_count; i++) {
        ColumnDef* col = (ColumnDef*)alist_get(&source->schema.columns, i);
        if (col) {
            ColumnDef* new_col = (ColumnDef*)alist_append(&result->schema.columns);
            if (new_col) *new_col = *col;
        }
    }
}

static Row* create_joined_row(ArrayList* left, ArrayList* right, int left_cols, int right_cols) {
    (void)left_cols;
    (void)right_cols;
    Row* row = malloc(sizeof(Row));
    if (!row) return NULL;

    alist_init(row, sizeof(Value), free_value);

    int left_count = alist_length(left);
    for (int i = 0; i < left_count; i++) {
        Value* val = (Value*)alist_append(row);
        *val = *(Value*)alist_get(left, i);
    }

    int right_count = alist_length(right);
    for (int i = 0; i < right_count; i++) {
        Value* val = (Value*)alist_append(row);
        *val = *(Value*)alist_get(right, i);
    }

    return row;
}

static Row* create_left_join_null_row(ArrayList* left, int right_cols) {
    Row* row = malloc(sizeof(Row));
    if (!row) return NULL;

    alist_init(row, sizeof(Value), free_value);

    int left_count = alist_length(left);
    for (int i = 0; i < left_count; i++) {
        Value* val = (Value*)alist_append(row);
        *val = *(Value*)alist_get(left, i);
    }

    for (int k = 0; k < right_cols; k++) {
        Value* val = (Value*)alist_append(row);
        val->type = TYPE_NULL;
    }
    return row;
}

void exec_filter_ast(ASTNode* ast, uint8_t table_id) {
    SelectNode* select = &ast->select;
    Table* table = get_table_by_id(table_id);
    if (!table) return;

    int row_count = alist_length(&table->rows);
    int match_count = 0;

    ArrayList matching_rows;
    alist_init(&matching_rows, sizeof(int), NULL);

    bool used_index = try_index_filter(table, select->where_clause, &matching_rows);

    if (used_index) {
        match_count = alist_length(&matching_rows);
        if (match_count > 0 && match_count < row_count) {
            for (int i = row_count - 1; i >= 0; i--) {
                bool keep = false;
                for (int j = 0; j < match_count; j++) {
                    int* match_idx = (int*)alist_get(&matching_rows, j);
                    if (*match_idx == i) {
                        keep = true;
                        break;
                    }
                }
                if (!keep) {
                    alist_remove(&table->rows, i);
                }
            }
        }
        alist_destroy(&matching_rows);
    } else {
        for (int i = 0; i < row_count; i++) {
            Row* row = (Row*)alist_get(&table->rows, i);
            if (!row) continue;

            if (eval_expression(select->where_clause, row, &table->schema)) {
                match_count++;
            }
        }
    }
    
    alist_destroy(&matching_rows);

    log_msg(LOG_INFO, "Filtered table '%s' to %d rows", table->name, match_count);
}

void exec_aggregate_ast(ASTNode* ast, uint8_t table_id) {
    SelectNode* select = &ast->select;
    Table* table = get_table_by_id(table_id);
    if (!table) return;

    int row_count = alist_length(&table->rows);
    if (row_count == 0) return;

    int expr_count = alist_length(&select->expressions);
    if (expr_count == 0) return;

    g_in_agg_context = true;
    alist_clear(&g_agg_res);

    ArrayList filtered_indices;
    alist_init(&filtered_indices, sizeof(int), NULL);

    collect_filtered_indices(table, select, row_count, &filtered_indices);

    int filtered_count = alist_length(&filtered_indices);
    int loop_count = select->where_clause ? filtered_count : row_count;

    compute_aggregates(table, &filtered_indices, loop_count, select, expr_count);

    alist_destroy(&filtered_indices);
    log_msg(LOG_INFO, "Aggregated table '%s' to 1 row", table->name);
}

static void collect_filtered_indices(Table* table, SelectNode* select, int row_count,
                                     ArrayList* filtered_indices) {
    if (!select->where_clause) return;

    for (int i = 0; i < row_count; i++) {
        Row* row = (Row*)alist_get(&table->rows, i);
        if (row && eval_expression(select->where_clause, row, &table->schema)) {
            int* idx = (int*)alist_append(filtered_indices);
            *idx = i;
        }
    }
}

static void compute_aggregates(Table* table, ArrayList* filtered_indices, int loop_count,
                               SelectNode* select, int expr_count) {
    for (int expr_idx = 0; expr_idx < expr_count; expr_idx++) {
        Expr** expr = (Expr**)alist_get(&select->expressions, expr_idx);
        if (!expr || !*expr) continue;
        if ((*expr)->type != EXPR_AGGREGATE_FUNC) continue;

        AggFuncType func_type = (*expr)->aggregate.func_type;
        Expr* operand = (*expr)->aggregate.operand;
        bool count_all = (*expr)->aggregate.count_all;

        Value result = compute_single_aggregate(table, filtered_indices, loop_count, select,
                                                func_type, operand, count_all);

        Value result_copy = result;
        void* slot = alist_append(&g_agg_res);
        *(Value*)slot = result_copy;
    }
}

static Value compute_single_aggregate(Table* table, ArrayList* filtered_indices, int loop_count,
                                      SelectNode* select, AggFuncType func_type, Expr* operand,
                                      bool count_all) {
    Value result = {0};
    result.type = TYPE_FLOAT;
    result.char_val = NULL;

    switch (func_type) {
        case FUNC_COUNT:
            result = compute_count_aggregate(table, filtered_indices, loop_count, select, operand,
                                             count_all);
            break;
        case FUNC_SUM:
            result = compute_sum_aggregate(table, filtered_indices, loop_count, select, operand);
            break;
        case FUNC_AVG:
            result = compute_avg_aggregate(table, filtered_indices, loop_count, select, operand);
            break;
        case FUNC_MIN:
            result = compute_min_aggregate(table, filtered_indices, loop_count, select, operand);
            break;
        case FUNC_MAX:
            result = compute_max_aggregate(table, filtered_indices, loop_count, select, operand);
            break;
        default:
            result.float_val = 0;
    }
    return result;
}

static Value compute_count_aggregate(Table* table, ArrayList* filtered_indices, int loop_count,
                              SelectNode* select, Expr* operand, bool count_all) {
    Value result = {0};
    result.type = TYPE_INT;
    result.int_val = loop_count;
    if (!count_all && operand && operand->type == EXPR_COLUMN) {
        int null_count = 0;
        for (int idx = 0; idx < loop_count; idx++) {
            int i = select->where_clause ? *(int*)alist_get(filtered_indices, idx) : idx;
            Row* row = (Row*)alist_get(&table->rows, i);
            if (row) {
                Value val = get_column_value(row, &table->schema, operand->column_name);
                if (is_null(&val)) null_count++;
            }
        }
        result.int_val = loop_count - null_count;
    }
    return result;
}

static Value compute_sum_aggregate(Table* table, ArrayList* filtered_indices, int loop_count,
                            SelectNode* select, Expr* operand) {
    Value result = {0};
    result.type = TYPE_FLOAT;
    double sum = 0.0;
    if (operand && operand->type == EXPR_COLUMN) {
        for (int idx = 0; idx < loop_count; idx++) {
            int i = select->where_clause ? *(int*)alist_get(filtered_indices, idx) : idx;
            Row* row = (Row*)alist_get(&table->rows, i);
            if (row) {
                Value val = get_column_value(row, &table->schema, operand->column_name);
                if (val.type == TYPE_INT)
                    sum += val.int_val;
                else if (val.type == TYPE_FLOAT)
                    sum += val.float_val;
            }
        }
    }
    result.float_val = sum;
    return result;
}

static Value compute_avg_aggregate(Table* table, ArrayList* filtered_indices, int loop_count,
                          SelectNode* select, Expr* operand) {
    Value result = {0};
    result.type = TYPE_FLOAT;
    double sum = 0.0;
    int count = 0;
    if (operand && operand->type == EXPR_COLUMN) {
        for (int idx = 0; idx < loop_count; idx++) {
            int i = select->where_clause ? *(int*)alist_get(filtered_indices, idx) : idx;
            Row* row = (Row*)alist_get(&table->rows, i);
            if (row) {
                Value val = get_column_value(row, &table->schema, operand->column_name);
                if (val.type == TYPE_INT) {
                    sum += val.int_val;
                    count++;
                } else if (val.type == TYPE_FLOAT) {
                    sum += val.float_val;
                    count++;
                }
            }
        }
    }
    result.float_val = count > 0 ? sum / count : 0;
    return result;
}

static Value compute_min_aggregate(Table* table, ArrayList* filtered_indices, int loop_count,
                          SelectNode* select, Expr* operand) {
    Value result = {0};
    result.type = TYPE_FLOAT;
    double min_val = INFINITY;
    if (operand && operand->type == EXPR_COLUMN) {
        for (int idx = 0; idx < loop_count; idx++) {
            int i = select->where_clause ? *(int*)alist_get(filtered_indices, idx) : idx;
            Row* row = (Row*)alist_get(&table->rows, i);
            if (row) {
                Value val = get_column_value(row, &table->schema, operand->column_name);
                if (val.type == TYPE_INT && val.int_val < min_val)
                    min_val = val.int_val;
                else if (val.type == TYPE_FLOAT && val.float_val < min_val)
                    min_val = val.float_val;
            }
        }
    }
    result.float_val = min_val == INFINITY ? 0 : min_val;
    return result;
}

static Value compute_max_aggregate(Table* table, ArrayList* filtered_indices, int loop_count,
                          SelectNode* select, Expr* operand) {
    Value result = {0};
    result.type = TYPE_FLOAT;
    double max_val = -INFINITY;
    if (operand && operand->type == EXPR_COLUMN) {
        for (int idx = 0; idx < loop_count; idx++) {
            int i = select->where_clause ? *(int*)alist_get(filtered_indices, idx) : idx;
            Row* row = (Row*)alist_get(&table->rows, i);
            if (row) {
                Value val = get_column_value(row, &table->schema, operand->column_name);
                if (val.type == TYPE_INT && val.int_val > max_val)
                    max_val = val.int_val;
                else if (val.type == TYPE_FLOAT && val.float_val > max_val)
                    max_val = val.float_val;
            }
        }
    }
    result.float_val = max_val == -INFINITY ? 0 : max_val;
    return result;
}

static void free_string_ptr(void* ptr) {
    void** str_ptr = (void**)ptr;
    free(*str_ptr);
}

static void setup_query_result(QueryResult** result, Table* table, SelectNode* select, bool is_select_star,
                        int col_count) {
    *result = malloc(sizeof(QueryResult));
    if (!*result) return;

    memset(*result, 0, sizeof(QueryResult));
    (*result)->col_count = col_count;
    alist_init(&(*result)->column_names, sizeof(char*), free_string_ptr);
    alist_init(&(*result)->values, sizeof(Value), free_value);
    alist_init(&(*result)->rows, sizeof(int), NULL);

    for (int i = 0; i < col_count; i++) {
        const char* name;
        if (is_select_star) {
            ColumnDef* col = (ColumnDef*)alist_get(&table->schema.columns, i);
            name = col ? col->name : "unknown";
        } else {
            Expr** expr = (Expr**)alist_get(&select->expressions, i);
            const char* alias = expr[0]->alias;
            name =
                alias[0] ? alias : (expr[0]->type == EXPR_COLUMN ? expr[0]->column_name : "expr");
        }
        char* name_copy = malloc(strlen(name) + 1);
        if (!name_copy) {
            log_msg(LOG_ERROR, "setup_query_result: Failed to allocate memory for column name");
            continue;
        }
        strcpy(name_copy, name);
        char** slot = (char**)alist_append(&(*result)->column_names);
        *slot = name_copy;
    }
}

static void process_aggregate_result_rows(QueryResult* result, ArrayList* agg_results,
                                   SelectNode* select, int col_count) {
    int agg_count = alist_length(agg_results);
    int limit = select->limit > 0 ? select->limit : 1;

    for (int i = 0; i < 1 && i < agg_count && alist_length(&result->rows) < limit; i++) {
        int* row_idx = (int*)alist_append(&result->rows);
        *row_idx = i;

        for (int j = 0; j < col_count && j < agg_count; j++) {
            Value* agg_val = (Value*)alist_get(agg_results, j);
            Value val;
            if (agg_val) {
                val = copy_string_value(agg_val);
            } else {
                val.type = TYPE_NULL;
                val.char_val = NULL;
                val.int_val = 0;
                val.float_val = 0;
                val.date_val = 0;
                val.time_val = 0;
            }
            Value* val_slot = (Value*)alist_append(&result->values);
            *val_slot = val;
        }
    }
}

static void process_regular_result_rows(QueryResult* result, Table* table, SelectNode* select,
                                 bool is_select_star, int col_count) {
    int row_count = alist_length(&table->rows);
    int limit = select->limit > 0 ? select->limit : row_count;

    for (int i = 0; i < row_count && alist_length(&result->rows) < limit; i++) {
        Row* row = (Row*)alist_get(&table->rows, i);
        if (!row || alist_length(row) == 0) continue;

        if (select->where_clause) {
            if (!eval_expression(select->where_clause, row, &table->schema)) {
                continue;
            }
        }

        int* row_idx = (int*)alist_append(&result->rows);
        *row_idx = i;

        for (int j = 0; j < col_count; j++) {
            Value val;
            if (is_select_star) {
                Value* row_val = (Value*)alist_get(row, j);
                val = copy_string_value(row_val);
            } else {
                Expr** expr = (Expr**)alist_get(&select->expressions, j);
                val = eval_select_expression(expr[0], row, &table->schema);
            }
            Value* val_slot = (Value*)alist_append(&result->values);
            *val_slot = val;
        }
    }
}

static void print_query_output(QueryResult* result) {
    print_pretty_result(result);
}

void exec_project_ast(ASTNode* ast, uint8_t table_id) {
    SelectNode* select = &ast->select;
    Table* table = get_table_by_id(table_id);
    if (!table) return;

    int expr_count = alist_length(&select->expressions);
    if (expr_count == 0) return;

    Expr** first_expr = (Expr**)alist_get(&select->expressions, 0);
    bool is_select_star =
        (first_expr && first_expr[0]->type == EXPR_VALUE && first_expr[0]->value.char_val &&
         strcmp(first_expr[0]->value.char_val, "*") == 0);

    int col_count = is_select_star ? alist_length(&table->schema.columns) : expr_count;
    if (col_count <= 0) return;

    free_query_result(g_last_result);
    g_last_result = NULL;

    setup_query_result(&g_last_result, table, select, is_select_star, col_count);
    if (!g_last_result) return;

    if (g_in_agg_context) {
        process_aggregate_result_rows(g_last_result, &g_agg_res, select, col_count);
    } else {
        process_regular_result_rows(g_last_result, table, select, is_select_star, col_count);
    }

    print_query_output(g_last_result);

    log_msg(LOG_INFO, "Projected %d rows from table '%s'", alist_length(&g_last_result->rows),
            table->name);
}
