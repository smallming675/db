#include "executor.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "arraylist.h"
#include "db.h"
#include "executor_internal.h"
#include "logger.h"
#include "table.h"
#include "values.h"

static QueryResult* g_last_result = NULL;
static ArrayList g_aggregate_results;
static bool g_in_aggregate_context = false;

QueryResult* get_last_query_result(void) { return g_last_result; }

void set_last_query_result(QueryResult* result) { g_last_result = result; }

static void free_row_contents(void* ptr);
static bool has_aggregate_expr(ASTNode* ast);
static void exec_create_table_ast(ASTNode* ast);
static void exec_insert_row_ast(ASTNode* ast);
static uint16_t exec_join_ast(ASTNode* ast);
static void exec_filter_ast(ASTNode* ast, uint8_t table_id);
static void exec_aggregate_ast(ASTNode* ast, uint8_t table_id);
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
static void exec_project_ast(ASTNode* ast, uint8_t table_id);
static void exec_drop_table_ast(ASTNode* ast);
static void exec_update_row_ast(ASTNode* ast);
static void exec_delete_row_ast(ASTNode* ast);
static void exec_create_index_ast(ASTNode* ast);
static void exec_drop_index_ast(ASTNode* ast);
static Value copy_string_value(const Value* src);

static void free_row_contents(void* ptr) {
    Row* row = (Row*)ptr;
    if (!row) return;
    for (int i = 0; i < row->value_count; i++) {
        if (row->values[i].type == TYPE_STRING && row->values[i].char_val) {
            free(row->values[i].char_val);
        }
    }
    free(row->values);
}

static bool has_aggregate_expr(ASTNode* ast) {
    if (!ast || ast->type != AST_SELECT) return false;
    SelectNode* select = &ast->select;
    int count = alist_length(&select->expressions);
    for (int i = 0; i < count; i++) {
        Expr** expr_ptr = (Expr**)alist_get(&select->expressions, i);
        if (expr_ptr && *expr_ptr && (*expr_ptr)->type == EXPR_AGGREGATE_FUNC) {
            return true;
        }
    }
    return false;
}

void exec_ast(ASTNode* ast) {
    if (!ast) {
        log_msg(LOG_WARN, "exec_ast: Called with NULL AST");
        return;
    }

    log_msg(LOG_DEBUG, "exec_ast: executing AST");

    g_in_aggregate_context = false;
    alist_init(&g_aggregate_results, sizeof(Value), free_value);

    ASTNode* current = ast;
    while (current) {
        switch (current->type) {
            case AST_CREATE_TABLE:
                exec_create_table_ast(current);
                break;
            case AST_INSERT_ROW:
                exec_insert_row_ast(current);
                break;
            case AST_SELECT: {
                SelectNode* select = &current->select;
                bool has_agg = has_aggregate_expr(current);
                bool has_join = (select->join_type != JOIN_NONE && select->join_table_id >= 0);
                uint8_t result_table_id = select->table_id;

                if (has_join) {
                    result_table_id = exec_join_ast(current);
                    if (result_table_id == 0) {
                        log_msg(LOG_ERROR, "exec_ast: JOIN failed");
                        return;
                    }
                }

                if (select->where_clause) {
                    exec_filter_ast(current, result_table_id);
                }

                if (has_agg) {
                    exec_aggregate_ast(current, result_table_id);
                }

                exec_project_ast(current, result_table_id);
                break;
            }
            case AST_DROP_TABLE:
                exec_drop_table_ast(current);
                break;
            case AST_UPDATE_ROW:
                exec_update_row_ast(current);
                break;
            case AST_DELETE_ROW:
                exec_delete_row_ast(current);
                break;
            case AST_CREATE_INDEX:
                exec_create_index_ast(current);
                break;
            case AST_DROP_INDEX:
                exec_drop_index_ast(current);
                break;
            default:
                log_msg(LOG_WARN, "exec_ast: Unknown AST node type: %d", current->type);
                break;
        }

        current = current->next;
    }

    log_msg(LOG_DEBUG, "exec_ast: AST execution completed");
}

static void exec_create_table_ast(ASTNode* ast) {
    CreateTableNode* ct = &ast->create_table;
    Table* table = NULL;
    for (int i = 0; i < alist_length(&tables); i++) {
        Table* t = (Table*)alist_get(&tables, i);
        if (t && strcmp(t->name, ct->table_name) == 0) {
            table = t;
            break;
        }
    }
    if (table) {
        log_msg(LOG_WARN, "Table '%s' already exists", ct->table_name);
        return;
    }

    if (alist_length(&tables) >= MAX_TABLES) {
        log_msg(LOG_ERROR, "create_table: Maximum table limit (%d) reached", MAX_TABLES);
        return;
    }

    table = malloc(sizeof(Table));
    if (!table) {
        log_msg(LOG_ERROR, "Failed to allocate table");
        return;
    }

    strcpy(table->name, ct->table_name);
    alist_init(&table->rows, sizeof(Row), free_row_contents);
    alist_init(&table->schema.columns, sizeof(ColumnDef), NULL);
    table->schema.strict = ct->strict;

    int col_count = alist_length(&ct->columns);
    for (int i = 0; i < col_count; i++) {
        ColumnDef* col = (ColumnDef*)alist_get(&ct->columns, i);
        if (col) {
            ColumnDef* new_col = (ColumnDef*)alist_append(&table->schema.columns);
            if (new_col) *new_col = *col;
        }
    }

    table->table_id = alist_length(&tables);
    Table* t = (Table*)alist_append(&tables);
    if (t) *t = *table;

    log_msg(LOG_INFO, "Created table '%s' with %d columns (STRICT=%s)", ct->table_name, col_count,
            ct->strict ? "true" : "false");

    free(table);
}

static Value copy_string_value(const Value* src) {
    Value copy = *src;
    if (src->type == TYPE_STRING && src->char_val) {
        copy.char_val = malloc(strlen(src->char_val) + 1);
        strcpy(copy.char_val, src->char_val);
    }
    return copy;
}

static void exec_insert_row_ast(ASTNode* ast) {
    InsertNode* ins = &ast->insert;
    Table* table = get_table_by_id(ins->table_id);
    if (!table) {
        log_msg(LOG_ERROR, "Table with ID %d not found", ins->table_id);
        return;
    }

    int row_count = alist_length(&ins->value_rows);
    if (row_count == 0) return;

    int inserted = 0;
    for (int r = 0; r < row_count; r++) {
        ArrayList* value_row = (ArrayList*)alist_get(&ins->value_rows, r);
        if (!value_row) continue;

        int value_count = alist_length(value_row);
        if (value_count == 0) continue;

        Row* row = (Row*)alist_append(&table->rows);
        if (!row) continue;

        row->value_count = value_count;
        row->values = malloc(value_count * sizeof(Value));

        for (int i = 0; i < value_count; i++) {
            ColumnValue* cv = (ColumnValue*)alist_get(value_row, i);
            if (cv) {
                row->values[i] = copy_string_value(&cv->value);
            }
        }
        inserted++;
    }

    log_msg(LOG_INFO, "Inserted %d row%s into table '%s'", inserted, inserted == 1 ? "" : "s", table->name);
}

static void copy_columns_to_result(Table* result, Table* source, int col_count);
static Row* create_joined_row(Row* left, Row* right, int left_cols, int right_cols);
static Row* create_left_join_null_row(Row* left, int right_cols);

static uint16_t exec_join_ast(ASTNode* ast) {
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

    Table* result_table = malloc(sizeof(Table));
    if (!result_table) return 0;

    snprintf(result_table->name, MAX_TABLE_NAME_LEN, "_join_%d_%d", select->table_id,
             select->join_table_id);
    result_table->table_id = result_id;
    alist_init(&result_table->rows, sizeof(Row), free_row_contents);
    alist_init(&result_table->schema.columns, sizeof(ColumnDef), NULL);
    result_table->schema.strict = false;

    int left_cols = alist_length(&left_table->schema.columns);
    int right_cols = alist_length(&right_table->schema.columns);

    copy_columns_to_result(result_table, left_table, left_cols);
    copy_columns_to_result(result_table, right_table, right_cols);

    int left_rows = alist_length(&left_table->rows);
    int right_rows = alist_length(&right_table->rows);

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
                    if (slot) *slot = *new_row;
                    free(new_row);
                }
            }
        }

        if (!had_match && select->join_type == JOIN_LEFT) {
            Row* new_row = create_left_join_null_row(left_row, right_cols);
            if (new_row) {
                Row* slot = (Row*)alist_append(&result_table->rows);
                if (slot) *slot = *new_row;
                free(new_row);
            }
        }
    }

    Table* t = (Table*)alist_append(&tables);
    if (t) *t = *result_table;

    log_msg(LOG_INFO, "Created join table '%s' with %d rows", result_table->name,
            alist_length(&result_table->rows));

    free(result_table);
    return result_id;
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

static Row* create_joined_row(Row* left, Row* right, int left_cols, int right_cols) {
    (void)left_cols;
    (void)right_cols;
    Row* row = malloc(sizeof(Row));
    if (!row) return NULL;

    row->value_count = left->value_count + right->value_count;
    row->values = malloc(row->value_count * sizeof(Value));
    memcpy(row->values, left->values, left->value_count * sizeof(Value));
    memcpy(row->values + left->value_count, right->values, right->value_count * sizeof(Value));
    return row;
}

static Row* create_left_join_null_row(Row* left, int right_cols) {
    Row* row = malloc(sizeof(Row));
    if (!row) return NULL;

    row->value_count = left->value_count + right_cols;
    row->values = malloc(row->value_count * sizeof(Value));
    memcpy(row->values, left->values, left->value_count * sizeof(Value));
    for (int k = 0; k < right_cols; k++) {
        row->values[left->value_count + k].type = TYPE_NULL;
    }
    return row;
}

static void exec_filter_ast(ASTNode* ast, uint8_t table_id) {
    SelectNode* select = &ast->select;
    Table* table = get_table_by_id(table_id);
    if (!table) return;

    int row_count = alist_length(&table->rows);
    int match_count = 0;

    for (int i = 0; i < row_count; i++) {
        Row* row = (Row*)alist_get(&table->rows, i);
        if (!row) continue;

        if (eval_expression(select->where_clause, row, &table->schema)) {
            match_count++;
        }
    }

    log_msg(LOG_INFO, "Filtered table '%s' to %d rows", table->name, match_count);
}

static void exec_aggregate_ast(ASTNode* ast, uint8_t table_id) {
    SelectNode* select = &ast->select;
    Table* table = get_table_by_id(table_id);
    if (!table) return;

    int row_count = alist_length(&table->rows);
    if (row_count == 0) return;

    int expr_count = alist_length(&select->expressions);
    if (expr_count == 0) return;

    g_in_aggregate_context = true;
    alist_clear(&g_aggregate_results);

    ArrayList filtered_indices;
    alist_init(&filtered_indices, sizeof(int), NULL);
    if (select->where_clause) {
        for (int i = 0; i < row_count; i++) {
            Row* row = (Row*)alist_get(&table->rows, i);
            if (row && eval_expression(select->where_clause, row, &table->schema)) {
                int* idx = (int*)alist_append(&filtered_indices);
                *idx = i;
            }
        }
    }

    int filtered_count = alist_length(&filtered_indices);
    int loop_count = select->where_clause ? filtered_count : row_count;

    for (int expr_idx = 0; expr_idx < expr_count; expr_idx++) {
        Expr** expr = (Expr**)alist_get(&select->expressions, expr_idx);
        if (!expr || !*expr) continue;
        if ((*expr)->type != EXPR_AGGREGATE_FUNC) continue;

        AggFuncType func_type = (*expr)->aggregate.func_type;
        Expr* operand = (*expr)->aggregate.operand;
        bool count_all = (*expr)->aggregate.count_all;

        Value result = {0};
        result.type = TYPE_FLOAT;
        result.char_val = NULL;

        switch (func_type) {
            case FUNC_COUNT:
                result = compute_count_aggregate(table, &filtered_indices, loop_count, select,
                                                 operand, count_all);
                break;
            case FUNC_SUM:
                result =
                    compute_sum_aggregate(table, &filtered_indices, loop_count, select, operand);
                break;
            case FUNC_AVG:
                result =
                    compute_avg_aggregate(table, &filtered_indices, loop_count, select, operand);
                break;
            case FUNC_MIN:
                result =
                    compute_min_aggregate(table, &filtered_indices, loop_count, select, operand);
                break;
            case FUNC_MAX:
                result =
                    compute_max_aggregate(table, &filtered_indices, loop_count, select, operand);
                break;
            default:
                result.float_val = 0;
        }

        Value* result_copy = (Value*)malloc(sizeof(Value));
        *result_copy = result;
        void* slot = alist_append(&g_aggregate_results);
        *(Value**)slot = result_copy;
    }

    alist_destroy(&filtered_indices);
    log_msg(LOG_INFO, "Aggregated table '%s' to 1 row", table->name);
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

static void setup_query_result(QueryResult** result, Table* table, SelectNode* select,
                               bool is_select_star, int col_count);
static void process_aggregate_result_rows(QueryResult* result, ArrayList* g_aggregate_results,
                                          SelectNode* select, int col_count);
static void process_regular_result_rows(QueryResult* result, Table* table, SelectNode* select,
                                        bool is_select_star, int col_count);
static void print_query_output(QueryResult* result, SelectNode* select, Table* table,
                               bool is_select_star, int col_count);

static void exec_project_ast(ASTNode* ast, uint8_t table_id) {
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

    if (g_in_aggregate_context) {
        process_aggregate_result_rows(g_last_result, &g_aggregate_results, select, col_count);
    } else {
        process_regular_result_rows(g_last_result, table, select, is_select_star, col_count);
    }

    print_query_output(g_last_result, select, table, is_select_star, col_count);

    log_msg(LOG_INFO, "Projected %d rows from table '%s'", g_last_result->row_count, table->name);
}

static void setup_query_result(QueryResult** result, Table* table, SelectNode* select,
                               bool is_select_star, int col_count) {
    (void)select;
    *result = malloc(sizeof(QueryResult));
    if (!*result) return;

    memset(*result, 0, sizeof(QueryResult));
    (*result)->col_count = col_count;
    (*result)->column_names = malloc(col_count * sizeof(char*));

    for (int i = 0; i < col_count; i++) {
        if (is_select_star) {
            ColumnDef* col = (ColumnDef*)alist_get(&table->schema.columns, i);
            const char* name = col ? col->name : "unknown";
            (*result)->column_names[i] = malloc(strlen(name) + 1);
            strcpy((*result)->column_names[i], name);
        } else {
            Expr** expr = (Expr**)alist_get(&select->expressions, i);
            const char* alias = expr[0]->alias;
            const char* name =
                alias[0] ? alias : (expr[0]->type == EXPR_COLUMN ? expr[0]->column_name : "expr");
            (*result)->column_names[i] = malloc(strlen(name) + 1);
            strcpy((*result)->column_names[i], name);
        }
    }

    int capacity = 16;
    (*result)->values = malloc(capacity * sizeof(Value));
    (*result)->rows = malloc(capacity * sizeof(int));
    (*result)->row_count = 0;
}

static void process_aggregate_result_rows(QueryResult* result, ArrayList* g_aggregate_results,
                                          SelectNode* select, int col_count) {
    int agg_count = alist_length(g_aggregate_results);
    int limit = select->limit > 0 ? select->limit : 1;
    int capacity = 16;

    for (int i = 0; i < 1 && i < agg_count && result->row_count < limit; i++) {
        if (result->row_count * col_count >= capacity) {
            capacity *= 2;
            result->values = realloc(result->values, capacity * sizeof(Value));
            result->rows = realloc(result->rows, capacity * sizeof(int));
        }

        for (int j = 0; j < col_count && j < agg_count; j++) {
            Value* agg_val = (Value*)alist_get(g_aggregate_results, j);
            Value val;
            if (agg_val) {
                val = copy_string_value(agg_val);
            } else {
                val.type = TYPE_NULL;
                val.char_val = NULL;
                val.int_val = 0;
                val.float_val = 0;
                val.date_val.date_val = 0;
                val.time_val.time_val = 0;
            }
            result->values[result->row_count * col_count + j] = val;
        }
        result->rows[result->row_count] = i;
        result->row_count++;
    }
}

static void process_regular_result_rows(QueryResult* result, Table* table, SelectNode* select,
                                        bool is_select_star, int col_count) {
    int row_count = alist_length(&table->rows);
    int limit = select->limit > 0 ? select->limit : row_count;
    int capacity = 16;

    for (int i = 0; i < row_count && result->row_count < limit; i++) {
        Row* row = (Row*)alist_get(&table->rows, i);
        if (!row || row->value_count == 0) continue;

        if (select->where_clause) {
            if (!eval_expression(select->where_clause, row, &table->schema)) {
                continue;
            }
        }

        if (result->row_count * col_count >= capacity) {
            capacity *= 2;
            result->values = realloc(result->values, capacity * sizeof(Value));
            result->rows = realloc(result->rows, capacity * sizeof(int));
        }

        for (int j = 0; j < col_count; j++) {
            Value val;
            if (is_select_star) {
                val = copy_string_value(&row->values[j]);
            } else {
                Expr** expr = (Expr**)alist_get(&select->expressions, j);
                val = eval_select_expression(expr[0], row, &table->schema);
            }
            result->values[result->row_count * col_count + j] = val;
        }
        result->rows[result->row_count] = i;
        result->row_count++;
    }
}

static void print_query_output(QueryResult* result, SelectNode* select, Table* table,
                               bool is_select_star, int col_count) {
    (void)select;
    (void)table;
    (void)is_select_star;
    (void)col_count;
    print_pretty_result(result);
}

static void exec_drop_table_ast(ASTNode* ast) {
    DropTableNode* drop = &ast->drop_table;
    Table* table = get_table_by_id(drop->table_id);
    if (!table) {
        log_msg(LOG_WARN, "Table not found for drop");
        return;
    }

    char table_name[MAX_TABLE_NAME_LEN];
    strcpy(table_name, table->name);

    for (int i = 0; i < alist_length(&tables); i++) {
        Table* t = (Table*)alist_get(&tables, i);
        if (t && t->table_id == drop->table_id) {
            alist_remove(&tables, i);
            break;
        }
    }

    log_msg(LOG_INFO, "Dropped table '%s'", table_name);
}

static void exec_update_row_ast(ASTNode* ast) {
    UpdateNode* update = &ast->update;
    Table* table = get_table_by_id(update->table_id);
    if (!table) return;

    int updated = 0;
    int row_count = alist_length(&table->rows);
    for (int i = 0; i < row_count; i++) {
        Row* row = (Row*)alist_get(&table->rows, i);
        if (!row) continue;

        if (eval_expression(update->where_clause, row, &table->schema)) {
            for (int j = 0; j < alist_length(&update->values); j++) {
                ColumnValue* cv = (ColumnValue*)alist_get(&update->values, j);
                if (cv && cv->column_name[0]) {
                    for (int k = 0; k < row->value_count; k++) {
                        ColumnDef* col = (ColumnDef*)alist_get(&table->schema.columns, k);
                        if (col && strcmp(col->name, cv->column_name) == 0) {
                            if (row->values[k].type == TYPE_STRING && row->values[k].char_val) {
                                free(row->values[k].char_val);
                            }
                            row->values[k] = copy_string_value(&cv->value);
                            break;
                        }
                    }
                }
            }
            updated++;
        }
    }

    log_msg(LOG_INFO, "Updated %d rows in table '%s'", updated, table->name);
}

static void exec_delete_row_ast(ASTNode* ast) {
    DeleteNode* del = &ast->delete;
    Table* table = get_table_by_id(del->table_id);
    if (!table) return;

    ArrayList kept_rows;
    alist_init(&kept_rows, sizeof(Row), free_row_contents);

    int deleted_rows = 0;
    int row_count = alist_length(&table->rows);
    for (int i = 0; i < row_count; i++) {
        Row* row = (Row*)alist_get(&table->rows, i);
        if (!row) continue;

        if (eval_expression(del->where_clause, row, &table->schema)) {
            deleted_rows++;
        } else {
            Row* kept = (Row*)alist_append(&kept_rows);
            if (kept) {
                kept->value_capacity = row->value_count > 0 ? row->value_count : 4;
                kept->values = malloc(kept->value_capacity * sizeof(Value));
                kept->value_count = 0;
                if (kept->values) {
                    copy_row(kept, row, 0);
                }
            }
        }
    }

    alist_destroy(&table->rows);
    table->rows = kept_rows;

    log_msg(LOG_INFO, "Deleted %d rows from table '%s'", deleted_rows, table->name);
}

static void exec_create_index_ast(ASTNode* ast) {
    CreateIndexNode* ci = &ast->create_index;
    Table* table = get_table_by_id(ci->table_id);
    if (!table) {
        log_msg(LOG_ERROR, "Table not found for index creation");
        return;
    }

    index_table_column(table->name, ci->column_name, ci->index_name);
}

static void exec_drop_index_ast(ASTNode* ast) {
    DropIndexNode* di = &ast->drop_index;
    drop_index_by_name(di->index_name);
}

void free_query_result(QueryResult* result) {
    if (!result) return;
    if (result->values) {
        for (int i = 0; i < result->row_count * result->col_count; i++) {
            if (result->values[i].type == TYPE_STRING && result->values[i].char_val) {
                free(result->values[i].char_val);
            }
        }
        free(result->values);
    }
    if (result->rows) free(result->rows);
    if (result->column_names) {
        for (int i = 0; i < result->col_count; i++) {
            if (result->column_names[i]) free(result->column_names[i]);
        }
        free(result->column_names);
    }
    free(result);
}

QueryResult* exec_query(const char* sql) {
    Token* tokens = tokenize(sql);
    ASTNode* ast = parse(tokens);
    if (!ast) {
        free_tokens(tokens);
        return NULL;
    }

    exec_ast(ast);

    free_tokens(tokens);
    free_ast(ast);

    QueryResult* result = g_last_result;
    g_last_result = NULL;
    return result;
}
