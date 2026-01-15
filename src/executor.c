#include <ctype.h>
#include <limits.h>
#include <math.h>

#include "db.h"
#include "logger.h"

const Value VAL_NULL = {.type = TYPE_NULL};
const Value VAL_ERROR = {.type = TYPE_ERROR};

static Value create_float(float val) {
    Value result;
    result.type = TYPE_FLOAT;
    result.float_val = val;
    return result;
}
static Value create_int(int val) {
    Value result;
    result.type = TYPE_INT;
    result.int_val = val;
    return result;
}

Table tables[MAX_TABLES];
int table_count = 0;

static Table* find_table(const char* name);
static Value get_column_value(const Row* row, const TableDef* schema, const char* column_name);
static bool eval_expression(const Expr* expr, const Row* row, const TableDef* schema);
static bool eval_comparison(Value left, Value right, OperatorType op);
static int compare_values(const Value* left, const Value* right);
static void exec_filter(const IRNode* current);
static void exec_update_row(const IRNode* current);
static void exec_delete_row(const IRNode* current);
static void exec_aggregate(const IRNode* current);
static void exec_project(const IRNode* current);
static Value eval_select_expression(Expr* expr, const Row* row, const TableDef* schema);
static Value compute_aggregate(AggFuncType func_type, AggState* state, DataType return_type);
static void update_aggregate_state(AggState* state, const Value* val, const IRAggregate* agg);
static bool is_null(const Value* val);
static void print_table_header(const TableDef* schema);
static void print_table_separator(int column_count);
static void print_row_data(const Row* row, const TableDef* schema);
static void print_project_header(const IRNode* current, const TableDef* schema);
static void print_project_separator(const IRNode* current, const TableDef* schema);
static void print_project_row(const IRNode* current, const Row* row, const TableDef* schema);
static Value eval_scalar_function(const Expr* expr, const Row* row, const TableDef* schema);
static Value eval_math_function(const Expr* expr, const Row* row, const TableDef* schema);
static Value eval_math_function(const Expr* expr, const Row* row, const TableDef* schema);
static Value eval_string_function(const Expr* expr, const Row* row, const TableDef* schema);
static const char* repr(const Value* val);

static Table* find_table(const char* name) {
    for (int i = 0; i < table_count; i++) {
        if (strcmp(tables[i].name, name) == 0) {
            return &tables[i];
        }
    }
    return NULL;
}

static Value get_column_value(const Row* row, const TableDef* schema, const char* column_name) {
    for (int i = 0; i < schema->column_count; i++) {
        if (strcmp(schema->columns[i].name, column_name) == 0) {
            if (row->is_null[i]) {
                return VAL_NULL;
            } else {
                return row->values[i];
            }
            break;
        }
    }
    return VAL_NULL;
}

static int compare_values(const Value* left, const Value* right) {
    if (is_null(left) || is_null(right)) return 0;

    if (left->type == TYPE_INT && right->type == TYPE_INT) {
        return left->int_val - right->int_val;
    } else if (left->type == TYPE_FLOAT && right->type == TYPE_FLOAT) {
        float left_val = left->float_val;
        float right_val = right->float_val;
        if (left_val < right_val) return -1;
        if (left_val > right_val) return 1;
        return 0;
    } else if (left->type == TYPE_INT && right->type == TYPE_FLOAT) {
        int left_val = left->int_val;
        float right_val = right->float_val;
        if (left_val < right_val) return -1;
        if (left_val > right_val) return 1;
        return 0;
    } else if (left->type == TYPE_FLOAT && right->type == TYPE_INT) {
        float left_val = left->float_val;
        int right_val = right->int_val;
        if (left_val < right_val) return -1;
        if (left_val > right_val) return 1;
        return 0;
    }

    if (left->type == TYPE_STRING && right->type == TYPE_STRING) {
        return strcmp(left->char_val, right->char_val);
    }
    return 0;
}

static bool eval_like_expression(const Value* left, const Value* right) {
    if (right->type != TYPE_STRING) {
        log_msg(LOG_ERROR, "Right hand side of LIKE expression (expected: %s, got: %s)",
                TYPE_STRING, right->type);
        return false;
    }

    if (is_null(left) || is_null(right)) return false;

    const char* text = repr(left);
    const char* pattern = right->char_val;
    const char* p = pattern;
    bool match = true;

    while (*p && *text) {
        if (*p == '%' || *p == '*') {
            p++;
            while (*text && *text != *p) text++;
        } else if (*p == '_' || *p == '?') {
            p++;
            if (*text) {
                text++;
            } else {
                match = false;
                break;
            }
        } else if (*p == '\\') {
            p++;
            if (*text) {
                text++;
            } else {
                match = false;
                break;
            }
        } else {
            if (*text == *p) {
                p++;
                text++;
            } else {
                match = false;
                break;
            }
        }
    }

    match = match && (*p == '\0' && *text == '\0');
    return match;
}

static bool eval_comparison(Value left, Value right, OperatorType op) {
    int cmp = compare_values(&left, &right);
    switch (op) {
        case OP_EQUALS:
            return cmp == 0;
        case OP_NOT_EQUALS:
            return cmp != 0;
        case OP_LESS:
            return cmp < 0;
        case OP_LESS_EQUAL:
            return cmp <= 0;
        case OP_GREATER:
            return cmp > 0;
        case OP_GREATER_EQUAL:
            return cmp >= 0;
        case OP_LIKE:
            return eval_like_expression(&left, &right);
        default:
            return false;
    }
}

static bool eval_expression(const Expr* expr, const Row* row, const TableDef* schema) {
    if (!expr) return true;

    switch (expr->type) {
        case EXPR_COLUMN: {
            Value val = get_column_value(row, schema, expr->column_name);
            return !is_null(&val);
        }
        case EXPR_VALUE: {
            return !is_null(&expr->value);
        }
        case EXPR_BINARY_OP: {
            bool left_val = eval_expression(expr->binary.left, row, schema);
            bool right_val = eval_expression(expr->binary.right, row, schema);

            switch (expr->binary.op) {
                case OP_AND:
                    return left_val && right_val;
                case OP_OR:
                    return left_val || right_val;
                case OP_EQUALS:
                case OP_NOT_EQUALS:
                case OP_LESS:
                case OP_LESS_EQUAL:
                case OP_GREATER:
                case OP_GREATER_EQUAL: {
                    if (expr->binary.left->type == EXPR_COLUMN &&
                        expr->binary.right->type == EXPR_VALUE) {
                        Value col_val =
                            get_column_value(row, schema, expr->binary.left->column_name);
                        return eval_comparison(col_val, expr->binary.right->value, expr->binary.op);
                    } else if (expr->binary.left->type == EXPR_VALUE &&
                               expr->binary.right->type == EXPR_COLUMN) {
                        Value col_val =
                            get_column_value(row, schema, expr->binary.right->column_name);
                        return eval_comparison(expr->binary.left->value, col_val, expr->binary.op);
                    } else if (expr->binary.left->type == EXPR_COLUMN &&
                               expr->binary.right->type == EXPR_COLUMN) {
                        Value left_val =
                            get_column_value(row, schema, expr->binary.left->column_name);
                        Value right_val =
                            get_column_value(row, schema, expr->binary.right->column_name);
                        return eval_comparison(left_val, right_val, expr->binary.op);
                    }
                    return false;
                }
                default:
                    return false;
            }
        }
        case EXPR_UNARY_OP: {
            bool operand_val = eval_expression(expr->unary.operand, row, schema);
            if (expr->unary.op == OP_NOT) {
                return !operand_val;
            }
            return false;
        }
        default:
            return false;
    }
}

static void exec_create_table(const IRNode* current) {
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

static void exec_insert_row(const IRNode* current) {
    log_msg(LOG_DEBUG, "exec_insert_row: Inserting row into table: %s",
            current->insert_row.table_name);

    Table* table = find_table(current->insert_row.table_name);
    if (!table) {
        log_msg(LOG_ERROR, "exec_insert_row: Table '%s' does not exist",
                current->insert_row.table_name);
        log_msg(LOG_ERROR, "exec_insert_row: Cannot insert into table '%s': table does not exist",
                current->insert_row.table_name);
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
        row->values[i] = current->insert_row.values[i];
        row->is_null[i] = is_null(&current->insert_row.values[i]);
    }

    log_msg(LOG_INFO, "exec_insert_row: Row inserted into table '%s' (row %d)",
            current->insert_row.table_name, table->row_count + 1);
    table->row_count++;
}

static void exec_scan_table(const IRNode* current) {
    log_msg(LOG_DEBUG, "exec_scan_table: Scanning table: %s", current->scan_table.table_name);

    Table* table = find_table(current->scan_table.table_name);
    if (!table) {
        log_msg(LOG_ERROR, "exec_scan_table: Table '%s' does not exist",
                current->scan_table.table_name);
        log_msg(LOG_ERROR, "exec_scan_table: Cannot scan table '%s': table does not exist",
                current->scan_table.table_name);
        return;
    }

    log_msg(LOG_INFO, "exec_scan_table: Displaying table '%s' with %d rows", table->name,
            table->row_count);

    print_table_header(&table->schema);
    print_table_separator(table->schema.column_count);

    for (int row_idx = 0; row_idx < table->row_count; row_idx++) {
        Row* row = &table->rows[row_idx];
        print_row_data(row, &table->schema);
    }
}

static void exec_drop_table(const IRNode* current) {
    log_msg(LOG_DEBUG, "exec_drop_table: Dropping table: %s", current->drop_table.table_name);

    Table* table = find_table(current->drop_table.table_name);
    if (!table) {
        log_msg(LOG_ERROR, "exec_drop_table: Table '%s' does not exist",
                current->drop_table.table_name);
        log_msg(LOG_ERROR, "exec_drop_table: Cannot drop table '%s': table does not exist",
                current->drop_table.table_name);
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

static void exec_filter(const IRNode* current) {
    log_msg(LOG_DEBUG, "exec_filter: Filtering table: %s", current->filter.table_name);

    Table* table = find_table(current->filter.table_name);
    if (!table) {
        log_msg(LOG_ERROR, "exec_filter: Table '%s' does not exist", current->filter.table_name);
        log_msg(LOG_ERROR, "exec_filter: Cannot filter table '%s': table does not exist",
                current->filter.table_name);
        return;
    }

    log_msg(LOG_INFO, "exec_filter: Displaying filtered table '%s'", table->name);

    print_table_header(&table->schema);
    print_table_separator(table->schema.column_count);

    for (int row_idx = 0; row_idx < table->row_count; row_idx++) {
        Row* row = &table->rows[row_idx];
        if (eval_expression(current->filter.filter_expr, row, &table->schema)) {
            print_row_data(row, &table->schema);
        }
    }
}

static void exec_update_row(const IRNode* current) {
    log_msg(LOG_DEBUG, "exec_update_row: Updating rows in table: %s",
            current->update_row.table_name);

    Table* table = find_table(current->update_row.table_name);
    if (!table) {
        log_msg(LOG_ERROR, "exec_update_row: Table '%s' does not exist",
                current->update_row.table_name);
        log_msg(LOG_ERROR, "exec_update_row: Cannot update table '%s': table does not exist",
                current->update_row.table_name);
        return;
    }

    int updated_rows = 0;
    for (int row_idx = 0; row_idx < table->row_count; row_idx++) {
        Row* row = &table->rows[row_idx];

        if (!current->update_row.where_clause ||
            eval_expression(current->update_row.where_clause, row, &table->schema)) {
            for (int i = 0; i < current->update_row.value_count; i++) {
                for (int col_idx = 0; col_idx < table->schema.column_count; col_idx++) {
                    if (strcmp(table->schema.columns[col_idx].name,
                               current->update_row.values[i].column_name) == 0) {
                        row->values[col_idx] = current->update_row.values[i].value;
                        row->is_null[col_idx] = is_null(&current->update_row.values[i].value);
                        break;
                    }
                }
                updated_rows++;
            }
        }
    }
    log_msg(LOG_INFO, "exec_update_row: Updated %d rows in table '%s'", updated_rows,
            current->update_row.table_name);
}

static void exec_aggregate(const IRNode* current) {
    log_msg(LOG_DEBUG, "exec_aggregate: Computing aggregate: %s",
            current->aggregate.func_type == FUNC_SUM     ? "SUM"
            : current->aggregate.func_type == FUNC_AVG   ? "AVG"
            : current->aggregate.func_type == FUNC_COUNT ? "COUNT"
            : current->aggregate.func_type == FUNC_MIN   ? "MIN"
            : current->aggregate.func_type == FUNC_MAX   ? "MAX"
                                                         : "UNKNOWN");

    Table* table = find_table(current->aggregate.table_name);
    if (!table) {
        log_msg(LOG_ERROR, "exec_aggregate: Table '%s' does not exist",
                current->aggregate.table_name);
        return;
    }

    AggState state = {0};
    if (current->aggregate.distinct) {
        state.is_distinct = true;
    }

    if (current->aggregate.func_type == FUNC_COUNT && current->aggregate.count_all) {
        log_msg(LOG_DEBUG, "exec_aggregate: COUNT(*) detected, using row_count");
        state.count = table->row_count;
    } else {
        log_msg(LOG_DEBUG, "exec_aggregate: %s with operand, count_all=%d",
                (current->aggregate.func_type == FUNC_COUNT ? "COUNT"
                 : current->aggregate.func_type == FUNC_SUM ? "SUM"
                 : current->aggregate.func_type == FUNC_AVG ? "AVG"
                 : current->aggregate.func_type == FUNC_MIN ? "MIN"
                 : current->aggregate.func_type == FUNC_MAX ? "MAX"
                                                            : "UNKNOWN"),
                current->aggregate.count_all);
        for (int row_idx = 0; row_idx < table->row_count; row_idx++) {
            Value val = VAL_NULL;
            if (current->aggregate.operand->type == EXPR_COLUMN) {
                val = get_column_value(&table->rows[row_idx], &table->schema, 
                                      current->aggregate.operand->column_name);
            } else if (current->aggregate.operand->type == EXPR_VALUE) {
                val = current->aggregate.operand->value;
            } else {
                val = eval_select_expression(current->aggregate.operand, &table->rows[row_idx],
                                             &table->schema);
            }
            update_aggregate_state(&state, &val, &current->aggregate);
            if (current->aggregate.distinct && !is_null(&val)) {
                for (int i = 0; i < state.distinct_count - 1; i++) {
                    if (state.seen_values[i]) {
                        free(state.seen_values[i]);
                        state.seen_values[i] = NULL;
                    }
                }
            }
        }
    }

    Value result = compute_aggregate(current->aggregate.func_type, &state, TYPE_FLOAT);

    if (result.type == TYPE_INT) {
        printf("%-20d\n", result.int_val);
    } else if (result.type == TYPE_FLOAT) {
        printf("%-20.6f\n", result.float_val);
    } else if (result.type == TYPE_STRING) {
        printf("%-20s\n", result.char_val);
    } else {
        printf("%-20s\n", "NULL");
    }
    if (current->aggregate.distinct) {
        for (int i = 0; i < MAX_ROWS; i++) {
            if (state.seen_values[i]) {
                free(state.seen_values[i]);
            }
        }
    }
}

static void exec_project(const IRNode* current) {
    log_msg(LOG_DEBUG, "exec_project: Projecting expressions for table: %s",
            current->project.table_name);

    Table* table = find_table(current->project.table_name);
    if (!table) {
        log_msg(LOG_ERROR, "exec_project: Table '%s' does not exist", current->project.table_name);
        return;
    }

    print_project_header(current, &table->schema);
    print_project_separator(current, &table->schema);

    for (int row_idx = 0; row_idx < table->row_count; row_idx++) {
        print_project_row(current, &table->rows[row_idx], &table->schema);
    }
}

static bool is_null(const Value* val) {
    if (!val) return true;
    if (val->type == TYPE_ERROR) {
        log_msg(LOG_ERROR, "Error value located, exiting...");
        exit(1);
    }
    return val->type == TYPE_NULL;
}

static double value_to_double(const Value* arg) {
    if (is_null(arg)) return 0.0;

    switch (arg->type) {
        case TYPE_INT:
            return (double)arg->int_val;
        case TYPE_FLOAT:
            return arg->float_val;
        default:
            return 0.0;
    }
}

static const char* repr(const Value* val) {
    static char buffer[MAX_STRING_LEN];

    if (!val || is_null(val)) {
        strcpy(buffer, "NULL");
        return buffer;
    }

    switch (val->type) {
        case TYPE_INT:
            snprintf(buffer, MAX_STRING_LEN, "%d", val->int_val);
            break;
        case TYPE_FLOAT:
            snprintf(buffer, MAX_STRING_LEN, "%.6f", val->float_val);
            break;
        case TYPE_STRING:
            strncpy(buffer, val->char_val, MAX_STRING_LEN - 1);
            buffer[MAX_STRING_LEN - 1] = '\0';
            break;
        case TYPE_TIME:
            snprintf(buffer, MAX_STRING_LEN, "%02d:%02d:%02d", val->time_val.hour,
                     val->time_val.minute, val->time_val.second);
            break;
        case TYPE_DATE:
            snprintf(buffer, MAX_STRING_LEN, "%04d-%02d-%02d", val->date_val.year,
                     val->date_val.month, val->date_val.day);
            break;
        case TYPE_ERROR:
            strcpy(buffer, "ERROR");
            break;
        default:
            strcpy(buffer, "UNKNOWN");
            break;
    }

    return buffer;
}

static void print_table_header(const TableDef* schema) {
    for (int i = 0; i < schema->column_count; i++) {
        printf("%-20s", schema->columns[i].name);
    }
    printf("\n");
}

static void print_table_separator(int column_count) {
    for (int i = 0; i < column_count; i++) {
        printf("%-20s", "--------------------");
    }
    printf("\n");
}

static void print_row_data(const Row* row, const TableDef* schema) {
    for (int col_idx = 0; col_idx < schema->column_count; col_idx++) {
        if (row->is_null[col_idx]) {
            printf("%-20s", "NULL");
        } else {
            printf("%-20s", repr(&row->values[col_idx]));
        }
    }
    printf("\n");
}

static void print_project_header(const IRNode* current, const TableDef* schema) {
    for (int i = 0; i < current->project.expression_count; i++) {
        if (current->project.expressions[i]->type == EXPR_COLUMN) {
            printf("%-20s", current->project.expressions[i]->column_name);
        } else if (current->project.expressions[i]->type == EXPR_AGGREGATE_FUNC) {
            printf("%-20s", 
                current->project.expressions[i]->aggregate.func_type == FUNC_COUNT ? "COUNT" :
                current->project.expressions[i]->aggregate.func_type == FUNC_SUM ? "SUM" :
                current->project.expressions[i]->aggregate.func_type == FUNC_AVG ? "AVG" :
                current->project.expressions[i]->aggregate.func_type == FUNC_MIN ? "MIN" :
                current->project.expressions[i]->aggregate.func_type == FUNC_MAX ? "MAX" : "UNKNOWN");
        } else if (current->project.expressions[i]->type == EXPR_VALUE &&
                   strcmp(current->project.expressions[i]->value.char_val, "*") == 0) {
            for (int j = 0; j < schema->column_count; j++) {
                printf("%-20s", schema->columns[j].name);
            }
            break;
        } else {
            printf("%-20s", "expression");
        }
    }
    printf("\n");
}

static void print_project_separator(const IRNode* current, const TableDef* schema) {
    for (int i = 0; i < current->project.expression_count; i++) {
        if (current->project.expressions[i]->type == EXPR_VALUE &&
            strcmp(current->project.expressions[i]->value.char_val, "*") == 0) {
            for (int j = 0; j < schema->column_count; j++) {
                printf("%-20s", "--------------------");
            }
            break;
        } else {
            printf("%-20s", "--------------------");
        }
    }
    printf("\n");
}

static void print_project_row(const IRNode* current, const Row* row, const TableDef* schema) {
    for (int col_idx = 0; col_idx < current->project.expression_count; col_idx++) {
        if (current->project.expressions[col_idx]->type == EXPR_VALUE &&
            strcmp(current->project.expressions[col_idx]->value.char_val, "*") == 0) {
            for (int j = 0; j < schema->column_count; j++) {
                if (row->is_null[j]) {
                    printf("%-20s", "NULL");
                } else {
                    printf("%-20s", repr(&row->values[j]));
                }
            }
            break;
        } else if (current->project.expressions[col_idx]->type == EXPR_AGGREGATE_FUNC) {
            // Skip aggregate functions in project - they were already handled by exec_aggregate
            printf("%-20s", "");
        } else {
            Value val = eval_select_expression(current->project.expressions[col_idx], row, schema);
            if (is_null(&val)) {
                printf("%-20s", "NULL");
            } else {
                printf("%-20s", repr(&val));
            }
        }
    }
    printf("\n");
}

static void update_aggregate_state(AggState* state, const Value* val,
                                   const IRAggregate* agg) {
    if (is_null(val)) {
        return;  // Skip NULL values
    }

    if (agg->distinct) {
        for (int i = 0; i < state->distinct_count; i++) {
            if (state->seen_values[i] && compare_values(val, (Value*)state->seen_values[i]) == 0) {
                return;  // Skip duplicate
            }
        }
        if (state->distinct_count < MAX_ROWS) {
            const char* char_val = val->char_val;
            if (val->type == TYPE_INT) {
                static char int_buf[32];
                snprintf(int_buf, sizeof(int_buf), "%d", val->int_val);
                char_val = int_buf;
            } else if (val->type == TYPE_FLOAT) {
                static char float_buf[32];
                snprintf(float_buf, sizeof(float_buf), "%.6f", val->float_val);
                char_val = float_buf;
            }
            char* val_copy = malloc(strlen(char_val) + 1);
            if (val_copy) {
                strcpy(val_copy, char_val);
                state->seen_values[state->distinct_count] = val_copy;
                state->distinct_count++;
            }
        }
    }

    if (agg->func_type == FUNC_SUM || agg->func_type == FUNC_AVG) {
        if (val->type == TYPE_FLOAT) {
            state->sum += val->float_val;
        } else if (val->type == TYPE_INT) {
            state->sum += val->int_val;
        } else {
            log_msg(LOG_ERROR, "Summing non-numeric type '%s' of type '%s'", repr(val), val->type);
        }
    }

    if (agg->func_type == FUNC_COUNT || agg->func_type == FUNC_AVG) {
        if (!agg->count_all) {
            state->count++;  // Count non-NULL values
        }
    }

    if (agg->func_type == FUNC_MIN) {
        if (!state->has_min) {
            state->min_val = *val;
            state->has_min = true;
        } else {
            if (compare_values(val, &(state->min_val)) < 0) {
                state->min_val = *val;
            }
        }
    }

    if (agg->func_type == FUNC_MAX) {
        if (!state->has_max) {
            state->max_val = *val;
            state->has_max = true;
        } else {
            if (compare_values(val, &(state->max_val)) > 0) {
                state->max_val = *val;
            }
        }
    }
}

static Value compute_aggregate(AggFuncType func_type, AggState* state,
                               DataType return_type) {
    Value result;

    if (func_type == FUNC_SUM) {
        if (return_type == TYPE_FLOAT) {
            result.type = TYPE_FLOAT;
            result.float_val = state->sum;
            return result;
        } else {
            // Check for overflow
            if (state->sum > INT_MAX || state->sum < INT_MIN) {
                log_msg(LOG_ERROR, "compute_aggregate: Integer overflow in SUM");
                return VAL_ERROR;
            } else {
                result.type = TYPE_INT;
                result.int_val = (int)state->sum;
                return result;
            }
        }
    } else if (func_type == FUNC_COUNT) {
        result.type = TYPE_INT;
        result.int_val = state->count;
        return result;
    } else if (func_type == FUNC_AVG) {
        if (state->count == 0) {
            return VAL_NULL;
        } else {
            result.type = TYPE_FLOAT;
            result.float_val = state->sum / state->count;
            return result;
        }
    } else if (func_type == FUNC_MIN) {
        if (!state->has_min) {
            return VAL_NULL;
        } else {
            return state->min_val;
        }
    } else if (func_type == FUNC_MAX) {
        if (!state->has_max) {
            return VAL_NULL;
        } else {
            return state->max_val;
        }
    }

    return VAL_ERROR;
}

static Value eval_select_expression(Expr* expr, const Row* row, const TableDef* schema) {
    if (!expr) return VAL_NULL;
    switch (expr->type) {
        case EXPR_COLUMN:
            return get_column_value(row, schema, expr->column_name);
        case EXPR_VALUE:
            return expr->value;
        case EXPR_AGGREGATE_FUNC:
            log_msg(LOG_ERROR,
                    "eval_select_expression: Aggregate function in expression evaluation");
            return VAL_ERROR;
        case EXPR_SCALAR_FUNC:
            return eval_scalar_function(expr, row, schema);
        default: {
            return VAL_ERROR;
        }
    }
}

static Value eval_math_function(const Expr* expr, const Row* row, const TableDef* schema) {
    Value result = VAL_ERROR;

    if (expr->scalar.func_type == FUNC_ABS) {
        if (expr->scalar.arg_count != 1) return result;
        Value arg = eval_select_expression(expr->scalar.args[0], row, schema);
        if (is_null(&arg)) return VAL_NULL;

        if (arg.type == TYPE_INT) {
            int abs_val = arg.int_val < 0 ? -arg.int_val : arg.int_val;
            result = create_int(abs_val);
        }
        if (arg.type == TYPE_FLOAT) {
            double abs_val = arg.float_val < 0 ? -arg.float_val : arg.float_val;
            result = create_float(abs_val);
        }
    } else if (expr->scalar.func_type == FUNC_SQRT) {
        if (expr->scalar.arg_count != 1) return result;
        Value arg = eval_select_expression(expr->scalar.args[0], row, schema);
        if (is_null(&arg)) return VAL_NULL;
        if (arg.type != TYPE_INT && arg.type != TYPE_FLOAT) {
            log_msg(LOG_ERROR,
                    "eval_scalar_function: %s function found non-numeric value '%s' of "
                    "type '%s'",
                    expr->scalar.func_type, repr(&arg), arg.type);
            return VAL_ERROR;
        }

        double val;
        if (arg.type == TYPE_FLOAT) {
            val = arg.float_val;
            if (val < 0) return VAL_NULL;
        } else if (arg.type == TYPE_INT) {
            val = arg.int_val;
            if (val < 0) return VAL_NULL;
        } else {
            return VAL_ERROR;
        }

        result.float_val = sqrt(val);
        result.type = TYPE_FLOAT;
    } else if (expr->scalar.func_type == FUNC_MOD) {
        if (expr->scalar.arg_count != 2) return result;
        Value arg1 = eval_select_expression(expr->scalar.args[0], row, schema);
        Value arg2 = eval_select_expression(expr->scalar.args[1], row, schema);

        if (is_null(&arg1) || is_null(&arg2)) {
            return VAL_NULL;
        }

        double val1 = value_to_double(&arg1);
        double val2 = value_to_double(&arg2);
        if (val2 == 0) {
            return VAL_NULL;
        }

        double mod_val = fmod(val1, val2);
        result = create_float(mod_val);
    } else if (expr->scalar.func_type == FUNC_POW) {
        if (expr->scalar.arg_count != 2) return result;
        Value arg1 = eval_select_expression(expr->scalar.args[0], row, schema);
        Value arg2 = eval_select_expression(expr->scalar.args[1], row, schema);

        if (is_null(&arg1) || is_null(&arg2)) {
            return VAL_NULL;
        }

        double val1 = value_to_double(&arg1);
        double val2 = value_to_double(&arg2);

        double power_val = pow(val1, val2);
        result = create_float(power_val);
    } else if (expr->scalar.func_type == FUNC_ROUND) {
        if (expr->scalar.arg_count < 1 || expr->scalar.arg_count > 2) return result;
        Value arg = eval_select_expression(expr->scalar.args[0], row, schema);
        if (is_null(&arg)) return VAL_NULL;
        if (arg.type != TYPE_INT && arg.type != TYPE_FLOAT) {
            log_msg(LOG_ERROR,
                    "eval_scalar_function: %s function found non-numeric value '%s' of "
                    "type '%s'",
                    expr->scalar.func_type, repr(&arg), arg.type);
            return VAL_ERROR;
        }

        double val = value_to_double(&arg);
        int decimals = 0;

        if (expr->scalar.arg_count == 2) {
            Value dec_arg = eval_select_expression(expr->scalar.args[1], row, schema);
            if (is_null(&dec_arg)) return VAL_NULL;
            if (dec_arg.type != TYPE_INT) {
                log_msg(LOG_ERROR,
                        "eval_scalar_function: %s function found non-integer value '%s' of "
                        "type '%s'",
                        expr->scalar.func_type, repr(&arg), arg.type);
                return VAL_ERROR;
            }
            decimals = dec_arg.int_val;
        }

        if (decimals == 0) {
            double rounded = round(val);
            result.int_val = (int)rounded;
            result.type = TYPE_INT;
        } else {
            double factor = pow(10, decimals);
            double rounded = round(val * factor) / factor;
            result.float_val = rounded;
            result.type = TYPE_FLOAT;
        }
    } else if (expr->scalar.func_type == FUNC_FLOOR) {
        if (expr->scalar.arg_count != 1) return result;
        Value arg = eval_select_expression(expr->scalar.args[0], row, schema);
        if (is_null(&arg)) return VAL_NULL;
        if (arg.type != TYPE_INT && arg.type != TYPE_FLOAT) {
            log_msg(LOG_ERROR,
                    "eval_scalar_function: %s function found non-numeric value '%s' of "
                    "type '%s'",
                    expr->scalar.func_type, repr(&arg), arg.type);
            return VAL_ERROR;
        }

        double val = value_to_double(&arg);
        double floored = floor(val);
        result.int_val = (int)floored;
        result.type = TYPE_INT;
    } else if (expr->scalar.func_type == FUNC_CEIL) {
        if (expr->scalar.arg_count != 1) return result;
        Value arg = eval_select_expression(expr->scalar.args[0], row, schema);
        if (is_null(&arg)) {
            return VAL_NULL;
        }

        double val = value_to_double(&arg);
        double ceiled = ceil(val);
        result = create_int((int)ceiled);
        result.type = TYPE_INT;
    }

    return result;
}

static Value eval_string_function(const Expr* expr, const Row* row, const TableDef* schema) {
    Value result = VAL_ERROR;

    if (expr->scalar.func_type == FUNC_UPPER) {
        if (expr->scalar.arg_count != 1) return result;
        Value arg = eval_select_expression(expr->scalar.args[0], row, schema);
        if (is_null(&arg)) return VAL_NULL;
        if (arg.type != TYPE_STRING) {
            log_msg(LOG_ERROR,
                    "eval_scalar_function: %s function found non-string value '%s' of type '%s'",
                    expr->scalar.func_type, repr(&arg), arg.type);
            return VAL_ERROR;
        }

        result.char_val = malloc(strlen(arg.char_val) + 1);
        for (int i = 0; arg.char_val[i] && i < MAX_STRING_LEN - 1; i++) {
            result.char_val[i] = toupper(arg.char_val[i]);
        }
        result.char_val[strlen(arg.char_val)] = '\0';
        result.type = TYPE_STRING;
    } else if (expr->scalar.func_type == FUNC_LOWER) {
        if (expr->scalar.arg_count != 1) return result;
        Value arg = eval_select_expression(expr->scalar.args[0], row, schema);
        if (is_null(&arg)) return VAL_NULL;
        if (arg.type != TYPE_STRING) {
            log_msg(LOG_ERROR,
                    "eval_scalar_function: %s function found non-string value '%s' of type '%s'",
                    expr->scalar.func_type, repr(&arg), arg.type);
            return VAL_ERROR;
        }

        result.char_val = malloc(strlen(arg.char_val) + 1);
        for (int i = 0; arg.char_val[i] && i < MAX_STRING_LEN - 1; i++) {
            result.char_val[i] = tolower(arg.char_val[i]);
        }
        result.char_val[strlen(arg.char_val)] = '\0';
        result.type = TYPE_STRING;
    } else if (expr->scalar.func_type == FUNC_LEN) {
        if (expr->scalar.arg_count != 1) return result;
        Value arg = eval_select_expression(expr->scalar.args[0], row, schema);
        if (is_null(&arg)) return VAL_NULL;
        if (arg.type != TYPE_STRING) {
            log_msg(LOG_ERROR,
                    "eval_scalar_function: %s function found non-string value '%s' of type '%s'",
                    expr->scalar.func_type, repr(&arg), arg.type);
            return VAL_ERROR;
        }

        int len = strlen(arg.char_val);
        result = create_int(len);
    } else if (expr->scalar.func_type == FUNC_MID) {
        if (expr->scalar.arg_count < 2 || expr->scalar.arg_count > 3) return result;
        Value str_arg = eval_select_expression(expr->scalar.args[0], row, schema);
        Value start_arg = eval_select_expression(expr->scalar.args[1], row, schema);

        if (is_null(&str_arg) || is_null(&start_arg)) return VAL_NULL;
        if (str_arg.type != TYPE_STRING) {
            log_msg(LOG_ERROR,
                    "eval_scalar_function: %s function string input found non-string value '%s' of "
                    "type '%s'",
                    expr->scalar.func_type, repr(&str_arg), str_arg.type);
            return VAL_ERROR;
        }

        if (start_arg.type != TYPE_INT) {
            log_msg(LOG_ERROR,
                    "eval_scalar_function: %s function index input found non-integer value '%s' of "
                    "type '%s'",
                    expr->scalar.func_type, repr(&start_arg), start_arg.type);
            return VAL_ERROR;
        }

        int start = start_arg.int_val - 1;
        if (start < 0) start = 0;

        int len = strlen(str_arg.char_val);
        if (start >= len) {
            result.char_val = malloc(1);
            strcpy(result.char_val, "");
            result.type = TYPE_STRING;
            return result;
        }

        int max_len = len - start;
        if (expr->scalar.arg_count == 3) {
            Value length_arg = eval_select_expression(expr->scalar.args[2], row, schema);
            if (is_null(&length_arg)) return VAL_NULL;
            int requested_len = atoi(length_arg.char_val);
            if (requested_len < max_len) max_len = requested_len;
        }

        result.char_val = malloc(max_len + 1);
        strncpy(result.char_val, str_arg.char_val + start, max_len);
        result.char_val[max_len] = '\0';
        result.type = TYPE_STRING;
    } else if (expr->scalar.func_type == FUNC_LEFT) {
        if (expr->scalar.arg_count != 2) return result;
        Value str_arg = eval_select_expression(expr->scalar.args[0], row, schema);
        Value len_arg = eval_select_expression(expr->scalar.args[1], row, schema);

        if (is_null(&str_arg) || is_null(&len_arg)) return VAL_NULL;
        if (str_arg.type != TYPE_STRING) {
            log_msg(LOG_ERROR,
                    "eval_scalar_function: %s function string input found non-string value '%s' of "
                    "type '%s'",
                    expr->scalar.func_type, repr(&str_arg), str_arg.type);
            return VAL_ERROR;
        }

        if (len_arg.type != TYPE_INT) {
            log_msg(LOG_ERROR,
                    "eval_scalar_function: %s function index input found non-integer value '%s' of "
                    "type '%s'",
                    expr->scalar.func_type, repr(&len_arg), len_arg.type);
            return VAL_ERROR;
        }

        int len = len_arg.int_val;
        int str_len = strlen(str_arg.char_val);
        if (len > str_len) len = str_len;
        if (len < 0) len = 0;

        result.char_val = malloc(len + 1);
        strncpy(result.char_val, str_arg.char_val, len);
        result.char_val[len] = '\0';
        result.type = TYPE_STRING;
    } else if (expr->scalar.func_type == FUNC_RIGHT) {
        if (expr->scalar.arg_count != 2) return result;
        Value str_arg = eval_select_expression(expr->scalar.args[0], row, schema);
        Value len_arg = eval_select_expression(expr->scalar.args[1], row, schema);

        if (is_null(&str_arg) || is_null(&len_arg)) return VAL_NULL;
        if (str_arg.type != TYPE_STRING) {
            log_msg(LOG_ERROR,
                    "eval_scalar_function: %s function string input found non-string value '%s' of "
                    "type '%s'",
                    expr->scalar.func_type, repr(&str_arg), str_arg.type);
            return VAL_ERROR;
        }

        if (len_arg.type != TYPE_INT) {
            log_msg(LOG_ERROR,
                    "eval_scalar_function: %s function index input found non-integer value '%s' of "
                    "type '%s'",
                    expr->scalar.func_type, repr(&len_arg), len_arg.type);
            return VAL_ERROR;
        }

        int len = len_arg.int_val;
        int str_len = strlen(str_arg.char_val);
        if (len > str_len) len = str_len;
        if (len < 0) len = 0;

        int start = str_len - len;
        result.char_val = malloc(len + 1);
        strcpy(result.char_val, str_arg.char_val + start);
        result.type = TYPE_STRING;
    } else if (expr->scalar.func_type == FUNC_CONCAT) {
        if (expr->scalar.arg_count < 2) return result;

        result.char_val = malloc(MAX_STRING_LEN);
        strcpy(result.char_val, "");
        for (int i = 0; i < expr->scalar.arg_count; i++) {
            Value arg = eval_select_expression(expr->scalar.args[i], row, schema);
            if (is_null(&arg)) {
                free(result.char_val);
                return VAL_NULL;
            }

            if (strlen(result.char_val) + strlen(arg.char_val) < MAX_STRING_LEN - 1) {
                strcat(result.char_val, arg.char_val);
            }
        }
        result.type = TYPE_STRING;
    }

    return result;
}

static Value eval_scalar_function(const Expr* expr, const Row* row, const TableDef* schema) {
    Value result = eval_math_function(expr, row, schema);
    if (result.type != TYPE_ERROR) {
        return result;
    }

    result = eval_string_function(expr, row, schema);
    if (result.type != TYPE_ERROR) {
        return result;
    }

    log_msg(LOG_ERROR, "eval_scalar_function: Unknown scalar function '%s'",
            expr->scalar.func_type);
    return VAL_ERROR;
}

static void exec_delete_row(const IRNode* current) {
    log_msg(LOG_DEBUG, "exec_delete_row: Deleting rows from table: %s",
            current->delete_row.table_name);

    Table* table = find_table(current->delete_row.table_name);
    if (!table) {
        log_msg(LOG_ERROR, "exec_delete_row: Table '%s' does not exist",
                current->delete_row.table_name);
        log_msg(LOG_ERROR, "exec_delete_row: Cannot delete from table '%s': table does not exist",
                current->delete_row.table_name);
        return;
    }

    int deleted_rows = 0;
    int write_idx = 0;

    for (int read_idx = 0; read_idx < table->row_count; read_idx++) {
        Row* row = &table->rows[read_idx];

        if (!current->delete_row.where_clause ||
            !eval_expression(current->delete_row.where_clause, row, &table->schema)) {
            if (read_idx != write_idx) {
                table->rows[write_idx] = table->rows[read_idx];
            }
            write_idx++;
        } else {
            deleted_rows++;
        }
    }

    table->row_count = write_idx;

    log_msg(LOG_INFO, "exec_delete_row: Deleted %d rows from table '%s'", deleted_rows,
            current->delete_row.table_name);
}

void exec_ir(IRNode* ir) {
    if (!ir) {
        log_msg(LOG_WARN, "exec_ir: Called with NULL IR");
        return;
    }

    log_msg(LOG_DEBUG, "exec_ir: executing IR");

    IRNode* current = ir;
    while (current) {

        switch (current->type) {
            case IR_CREATE_TABLE:
                exec_create_table(current);
                break;

            case IR_INSERT_ROW:
                exec_insert_row(current);
                break;

            case IR_SCAN_TABLE:
                exec_scan_table(current);
                break;

            case IR_FILTER:
                exec_filter(current);
                break;

            case IR_DROP_TABLE:
                exec_drop_table(current);
                break;

            case IR_UPDATE_ROW:
                exec_update_row(current);
                break;

            case IR_DELETE_ROW:
                exec_delete_row(current);
                break;

            case IR_AGGREGATE:
                exec_aggregate(current);
                break;

            case IR_PROJECT:
                exec_project(current);
                break;

            default:
                log_msg(LOG_WARN, "exec_ir: Unknown IR node type: %d", current->type);
                break;
        }

        current = current->next;
    }

    log_msg(LOG_DEBUG, "exec_ir: IR execution completed");
}
