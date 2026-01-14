#include <limits.h>

#include "db.h"
#include "logger.h"

Table tables[MAX_TABLES];
int table_count = 0;

static Table* find_table(const char* name);
static Value get_column_value(const Row* row, const TableDef* schema, const char* column_name);
static bool eval_expression(const Expr* expr, const Row* row, const TableDef* schema);
static bool eval_comparison(Value left, Value right, OperatorType op);
static bool string_to_bool(const char* str);
static int compare_values(const Value* left, const Value* right);
static void exec_filter(const IRNode* current);
static void exec_update_row(const IRNode* current);
static void exec_delete_row(const IRNode* current);
static void exec_aggregate(const IRNode* current);
static void exec_project(const IRNode* current);
static Value eval_select_expression(Expr* expr, const Row* row, const TableDef* schema);
static Value compute_aggregate(const char* func_name, AggregateState* state, DataType return_type);
static void update_aggregate_state(AggregateState* state, Value val, const IRAggregate* agg);
static bool is_null(Value val);
static void print_table_header(const TableDef* schema);
static void print_table_separator(int column_count);
static void print_row_data(const Row* row, const TableDef* schema);
static void print_project_header(const IRNode* current, const TableDef* schema);
static void print_project_separator(const IRNode* current, const TableDef* schema);
static void print_project_row(const IRNode* current, const Row* row, const TableDef* schema);

static Table* find_table(const char* name) {
    for (int i = 0; i < table_count; i++) {
        if (strcmp(tables[i].name, name) == 0) {
            return &tables[i];
        }
    }
    return NULL;
}

static Value get_column_value(const Row* row, const TableDef* schema, const char* column_name) {
    Value val;
    val.type = TYPE_STRING;
    strcpy(val.value, "NULL");

    for (int i = 0; i < schema->column_count; i++) {
        if (strcmp(schema->columns[i].name, column_name) == 0) {
            if (row->is_null[i]) {
                strcpy(val.value, "NULL");
                val.type = TYPE_STRING;
            } else {
                val = row->values[i];
            }
            break;
        }
    }
    return val;
}

static int compare_values(const Value* left, const Value* right) {
    if (strcmp(left->value, "NULL") == 0 || strcmp(right->value, "NULL") == 0) {
        return 0;
    }

    if (left->type == TYPE_INT && right->type == TYPE_INT) {
        int left_val = atoi(left->value);
        int right_val = atoi(right->value);
        return left_val - right_val;
    } else if (left->type == TYPE_FLOAT && right->type == TYPE_FLOAT) {
        float left_val = atof(left->value);
        float right_val = atof(right->value);
        if (left_val < right_val) return -1;
        if (left_val > right_val) return 1;
        return 0;
    } else if ((left->type == TYPE_INT && right->type == TYPE_FLOAT) ||
               (left->type == TYPE_FLOAT && right->type == TYPE_INT)) {
        float left_val = atof(left->value);
        float right_val = atof(right->value);
        if (left_val < right_val) return -1;
        if (left_val > right_val) return 1;
        return 0;
    } else {
        return strcmp(left->value, right->value);
    }
}

static bool eval_like_expression(Value left, Value right) {
    const char* text = left.value;
    const char* pattern = right.value;
    if (strcmp(text, "NULL") == 0 || strcmp(pattern, "NULL") == 0) return false;
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
            return eval_like_expression(left, right);
        default:
            return false;
    }
}

static bool eval_expression(const Expr* expr, const Row* row, const TableDef* schema) {
    if (!expr) return true;

    switch (expr->type) {
        case EXPR_COLUMN: {
            Value val = get_column_value(row, schema, expr->column_name);
            return string_to_bool(val.value);
        }
        case EXPR_VALUE: {
            return string_to_bool(expr->value.value);
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
                        return eval_comparison(col_val, expr->binary.right->value,
                                                   expr->binary.op);
                    } else if (expr->binary.left->type == EXPR_VALUE &&
                               expr->binary.right->type == EXPR_COLUMN) {
                        Value col_val =
                            get_column_value(row, schema, expr->binary.right->column_name);
                        return eval_comparison(expr->binary.left->value, col_val,
                                                   expr->binary.op);
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

static bool string_to_bool(const char* str) {
    if (strcmp(str, "NULL") == 0) return false;
    if (strcmp(str, "0") == 0) return false;
    if (strcmp(str, "false") == 0) return false;
    if (strcmp(str, "") == 0) return false;
    return true;
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
        log_msg(LOG_ERROR,
                "exec_insert_row: Cannot insert into table '%s': table does not exist",
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
        row->is_null[i] = (strcmp(current->insert_row.values[i].value, "NULL") == 0);
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

    printf("%d rows\n", table->row_count);
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

    int matching_rows = 0;
    for (int row_idx = 0; row_idx < table->row_count; row_idx++) {
        Row* row = &table->rows[row_idx];
        if (eval_expression(current->filter.filter_expr, row, &table->schema)) {
            print_row_data(row, &table->schema);
            matching_rows++;
        }
    }

    printf("%d rows (filtered)\n", matching_rows);
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
                        row->is_null[col_idx] =
                            (strcmp(current->update_row.values[i].value.value, "NULL") == 0);
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
            current->aggregate.aggregate_func);

    Table* table = find_table(current->aggregate.table_name);
    if (!table) {
        log_msg(LOG_ERROR, "exec_aggregate: Table '%s' does not exist",
                current->aggregate.table_name);
        return;
    }

    AggregateState state = {0};
    if (current->aggregate.distinct) {
        state.is_distinct = true;
    }

    if (strcmp(current->aggregate.aggregate_func, "COUNT") == 0 && current->aggregate.count_all) {
        state.count = table->row_count;
    } else {
        for (int row_idx = 0; row_idx < table->row_count; row_idx++) {
            Value val = eval_select_expression(current->aggregate.operand,
                                                   &table->rows[row_idx], &table->schema);
            update_aggregate_state(&state, val, &current->aggregate);
            if (current->aggregate.distinct && !is_null(val)) {
                for (int i = 0; i < state.distinct_count - 1; i++) {
                    if (state.seen_values[i]) {
                        free(state.seen_values[i]);
                        state.seen_values[i] = NULL;
                    }
                }
            }
        }
    }

    Value result = compute_aggregate(current->aggregate.aggregate_func, &state, TYPE_FLOAT);

    printf("%-20s\n", result.value);
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
        log_msg(LOG_ERROR, "exec_project: Table '%s' does not exist",
                current->project.table_name);
        return;
    }

    print_project_header(current, &table->schema);
    print_project_separator(current, &table->schema);

    int rows_processed = 0;
    for (int row_idx = 0; row_idx < table->row_count; row_idx++) {
        print_project_row(current, &table->rows[row_idx], &table->schema);
        rows_processed++;
    }

    printf("%d rows\n", rows_processed);
}

static bool is_null(Value val) { return strcmp(val.value, "NULL") == 0; }

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
            printf("%-20s", row->values[col_idx].value);
        }
    }
    printf("\n");
}

static void print_project_header(const IRNode* current, const TableDef* schema) {
    for (int i = 0; i < current->project.expression_count; i++) {
        if (current->project.expressions[i]->type == EXPR_COLUMN) {
            printf("%-20s", current->project.expressions[i]->column_name);
        } else if (current->project.expressions[i]->type == EXPR_AGGREGATE_FUNC) {
            printf("%-20s", current->project.expressions[i]->aggregate.func_name);
        } else if (current->project.expressions[i]->type == EXPR_VALUE &&
                   strcmp(current->project.expressions[i]->value.value, "*") == 0) {
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
            strcmp(current->project.expressions[i]->value.value, "*") == 0) {
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
            strcmp(current->project.expressions[col_idx]->value.value, "*") == 0) {
            for (int j = 0; j < schema->column_count; j++) {
                if (row->is_null[j]) {
                    printf("%-20s", "NULL");
                } else {
                    printf("%-20s", row->values[j].value);
                }
            }
            break;
        } else {
            Value val = eval_select_expression(current->project.expressions[col_idx],
                                                   row, schema);

            if (is_null(val)) {
                printf("%-20s", "NULL");
            } else {
                printf("%-20s", val.value);
            }
        }
    }
    printf("\n");
}
static void update_aggregate_state(AggregateState* state, Value val, const IRAggregate* agg) {
    if (is_null(val)) {
        return;  // Skip NULL values
    }

    if (agg->distinct) {
        for (int i = 0; i < state->distinct_count; i++) {
            if (state->seen_values[i] && compare_values(&val, (Value*)state->seen_values[i]) == 0) {
                return;  // Skip duplicate
            }
        }
        if (state->distinct_count < MAX_ROWS) {
            char* val_copy = malloc(strlen(val.value) + 1);
            if (val_copy) {
                strcpy(val_copy, val.value);
                state->seen_values[state->distinct_count] = val_copy;
                state->distinct_count++;
            }
        }
    }

    if (strcmp(agg->aggregate_func, "SUM") == 0 || strcmp(agg->aggregate_func, "AVG") == 0) {
        if (val.type == TYPE_FLOAT) {
            state->sum += atof(val.value);
        } else {
            state->sum += atoi(val.value);
        }
    }

    if (strcmp(agg->aggregate_func, "COUNT") == 0) {
        if (!agg->count_all) {
            state->count++;  // Count non-NULL values
        }
    }

    if (strcmp(agg->aggregate_func, "MIN") == 0) {
        if (!state->has_min) {
            state->min_val = val;
            state->has_min = true;
        } else {
            if (compare_values(&val, &state->min_val) < 0) {
                state->min_val = val;
            }
        }
    }

    if (strcmp(agg->aggregate_func, "MAX") == 0) {
        if (!state->has_max) {
            state->max_val = val;
            state->has_max = true;
        } else {
            if (compare_values(&val, &state->max_val) > 0) {
                state->max_val = val;
            }
        }
    }
}

static Value compute_aggregate(const char* func_name, AggregateState* state, DataType return_type) {
    Value result;

    if (strcmp(func_name, "SUM") == 0) {
        if (return_type == TYPE_FLOAT) {
            snprintf(result.value, MAX_STRING_LEN, "%.6f", state->sum);
            result.type = TYPE_FLOAT;
        } else {
            // Check for overflow
            if (state->sum > INT_MAX || state->sum < INT_MIN) {
                log_msg(LOG_ERROR, "compute_aggregate: Integer overflow in SUM");
                strcpy(result.value, "ERROR");
                result.type = TYPE_STRING;
            } else {
                snprintf(result.value, MAX_STRING_LEN, "%.0f", state->sum);
                result.type = TYPE_INT;
            }
        }
    } else if (strcmp(func_name, "COUNT") == 0) {
        snprintf(result.value, MAX_STRING_LEN, "%d", state->count);
        result.type = TYPE_INT;
    } else if (strcmp(func_name, "AVG") == 0) {
        if (state->count == 0) {
            strcpy(result.value, "NULL");
            result.type = TYPE_STRING;
        } else {
            snprintf(result.value, MAX_STRING_LEN, "%.6f", state->sum / state->count);
            result.type = TYPE_FLOAT;
        }
    } else if (strcmp(func_name, "MIN") == 0) {
        if (!state->has_min) {
            strcpy(result.value, "NULL");
            result.type = TYPE_STRING;
        } else {
            result = state->min_val;
        }
    } else if (strcmp(func_name, "MAX") == 0) {
        if (!state->has_max) {
            strcpy(result.value, "NULL");
            result.type = TYPE_STRING;
        } else {
            result = state->max_val;
        }
    } else {
        strcpy(result.value, "ERROR");
        result.type = TYPE_STRING;
    }

    return result;
}

static Value eval_select_expression(Expr* expr, const Row* row, const TableDef* schema) {
    if (!expr) {
        Value null_val;
        strcpy(null_val.value, "NULL");
        null_val.type = TYPE_STRING;
        return null_val;
    }

    switch (expr->type) {
        case EXPR_COLUMN:
            return get_column_value(row, schema, expr->column_name);
        case EXPR_VALUE:
            return expr->value;
        case EXPR_AGGREGATE_FUNC:
            log_msg(LOG_WARN,
                    "eval_select_expression: Aggregate function in expression evaluation");
            {
                Value error_val;
                strcpy(error_val.value, "ERROR");
                error_val.type = TYPE_STRING;
                return error_val;
            }
        default: {
            Value error_val;
            strcpy(error_val.value, "ERROR");
            error_val.type = TYPE_STRING;
            return error_val;
        }
    }
}

static void exec_delete_row(const IRNode* current) {
    log_msg(LOG_DEBUG, "exec_delete_row: Deleting rows from table: %s",
            current->delete_row.table_name);

    Table* table = find_table(current->delete_row.table_name);
    if (!table) {
        log_msg(LOG_ERROR, "exec_delete_row: Table '%s' does not exist",
                current->delete_row.table_name);
        log_msg(LOG_ERROR,
                "exec_delete_row: Cannot delete from table '%s': table does not exist",
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
