#include <ctype.h>
#include <limits.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>

#include "db.h"
#include "logger.h"
#include "table.h"
#include "values.h"

static void print_centered(const char* str, int width) {
    int len = strlen(str);
    int padding = width - len;
    int left_pad = padding / 2;

    printf("%*s%s%*s", left_pad, "", str, width - left_pad - len, "");
}

static Value get_column_value(const Row* row, const TableDef* schema, const char* column_name);
static bool eval_expression(const Expr* expr, const Row* row, const TableDef* schema);
static void exec_filter(const IRNode* current);
static void exec_update_row(const IRNode* current);
static void exec_delete_row(const IRNode* current);
static void exec_aggregate(const IRNode* current);
static void exec_project(const IRNode* current);
static void exec_sort(const IRNode* current);
static Value eval_select_expression(Expr* expr, const Row* row, const TableDef* schema);
static void print_project_header(const IRNode* current, const TableDef* schema);
static void print_project_row(const IRNode* current, const Row* row, const TableDef* schema);
static Value eval_scalar_function(const Expr* expr, const Row* row, const TableDef* schema);
static Value eval_math_function(const Expr* expr, const Row* row, const TableDef* schema);
static Value eval_string_function(const Expr* expr, const Row* row, const TableDef* schema);
static const char* scalar_func_name(ScalarFuncType func_type) {
    switch (func_type) {
        case FUNC_ABS:
            return "ABS";
        case FUNC_SQRT:
            return "SQRT";
        case FUNC_MOD:
            return "MOD";
        case FUNC_POW:
            return "POWER";
        case FUNC_ROUND:
            return "ROUND";
        case FUNC_FLOOR:
            return "FLOOR";
        case FUNC_CEIL:
            return "CEIL";
        case FUNC_UPPER:
            return "UPPER";
        case FUNC_LOWER:
            return "LOWER";
        case FUNC_LEN:
            return "LENGTH";
        case FUNC_MID:
            return "MID";
        case FUNC_LEFT:
            return "LEFT";
        case FUNC_RIGHT:
            return "RIGHT";
        case FUNC_CONCAT:
            return "CONCAT";
        default:
            return "UNKNOWN";
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

/* Apply WHERE filter */
static void exec_filter(const IRNode* current) {
    log_msg(LOG_DEBUG, "exec_filter: Filtering table: %s", current->filter.table_name);

    Table* table = get_table(current->filter.table_name);
    if (!table) {
        char suggestion[256];
        const char* table_names[MAX_TABLES];
        extern Table tables[];
        extern int table_count;
        for (int i = 0; i < table_count; i++) {
            table_names[i] = tables[i].name;
        }
        suggest_similar(current->filter.table_name, table_names, table_count, suggestion,
                        sizeof(suggestion));
        show_prominent_error("Table '%s' does not exist", current->filter.table_name);
        if (strlen(suggestion) > 0) {
            printf("  %s\n", suggestion);
        }
        return;
    }

    log_msg(LOG_INFO, "exec_filter: Displaying filtered table '%s'", table->name);

    int filtered_count = 0;
    for (int row_idx = 0; row_idx < table->row_count; row_idx++) {
        Row* row = &table->rows[row_idx];
        if (eval_expression(current->filter.filter_expr, row, &table->schema)) {
            filtered_count++;
        }
    }

    log_msg(LOG_INFO, "exec_filter: Found %d matching rows", filtered_count);
}

static void exec_update_row(const IRNode* current) {
    log_msg(LOG_DEBUG, "exec_update_row: Updating rows in table: %s",
            current->update_row.table_name);

    Table* table = get_table(current->update_row.table_name);
    if (!table) {
        char suggestion[256];
        const char* table_names[MAX_TABLES];
        extern Table tables[];
        extern int table_count;
        for (int i = 0; i < table_count; i++) {
            table_names[i] = tables[i].name;
        }
        suggest_similar(current->update_row.table_name, table_names, table_count, suggestion,
                        sizeof(suggestion));
        show_prominent_error("Table '%s' does not exist", current->update_row.table_name);
        if (strlen(suggestion) > 0) {
            printf("  %s\n", suggestion);
        }
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

    Table* table = get_table(current->aggregate.table_name);
    if (!table) {
        char suggestion[256];
        const char* table_names[MAX_TABLES];
        extern Table tables[];
        extern int table_count;
        for (int i = 0; i < table_count; i++) {
            table_names[i] = tables[i].name;
        }
        suggest_similar(current->aggregate.table_name, table_names, table_count, suggestion,
                        sizeof(suggestion));
        show_prominent_error("Table '%s' does not exist", current->aggregate.table_name);
        if (strlen(suggestion) > 0) {
            printf("  %s\n", suggestion);
        }
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
        printf("%-20.2f\n", result.float_val);
    } else if (result.type == TYPE_STRING) {
        print_centered(result.char_val, 20);
        printf("\n");
    } else {
        print_centered("NULL", 20);
        printf("\n");
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

    Table* table = get_table(current->project.table_name);
    if (!table) {
        return;
    }

    int display_limit = table->row_count;
    if (current->project.limit > 0 && current->project.limit < display_limit) {
        display_limit = current->project.limit;
    }

    print_project_header(current, &table->schema);

    for (int row_idx = 0; row_idx < display_limit; row_idx++) {
        print_project_row(current, &table->rows[row_idx], &table->schema);
    }

    int column_count = current->project.expression_count;

    if (column_count == 1 && current->project.expressions[0]->type == EXPR_VALUE &&
        strcmp(current->project.expressions[0]->value.char_val, "*") == 0) {
        column_count = table->schema.column_count;
    }

    printf("└");
    for (int i = 0; i < column_count; i++) {
        for (int j = 0; j < 20; j++) {
            printf("─");
        }
        if (i < column_count - 1) {
            printf("┴");
        }
    }
    printf("┘\n");
}

static Value create_int(int val) {
    Value result;
    result.type = TYPE_INT;
    result.int_val = val;
    return result;
}

static Value create_float(float val) {
    Value result;
    result.type = TYPE_FLOAT;
    result.float_val = val;
    return result;
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

static void print_project_header(const IRNode* current, const TableDef* schema) {
    int column_count = current->project.expression_count;

    if (column_count == 1 && current->project.expressions[0]->type == EXPR_VALUE &&
        strcmp(current->project.expressions[0]->value.char_val, "*") == 0) {
        column_count = schema->column_count;
    }

    printf("┌");
    for (int i = 0; i < column_count; i++) {
        for (int j = 0; j < 20; j++) {
            printf("─");
        }
        if (i < column_count - 1) {
            printf("┬");
        }
    }
    printf("┐\n");

    printf("│");
    for (int i = 0; i < current->project.expression_count; i++) {
        if (current->project.expressions[i]->type == EXPR_COLUMN) {
            print_centered(current->project.expressions[i]->column_name, 20);
        } else if (current->project.expressions[i]->type == EXPR_AGGREGATE_FUNC) {
            print_centered(
                current->project.expressions[i]->aggregate.func_type == FUNC_COUNT ? "COUNT"
                : current->project.expressions[i]->aggregate.func_type == FUNC_SUM ? "SUM"
                : current->project.expressions[i]->aggregate.func_type == FUNC_AVG ? "AVG"
                : current->project.expressions[i]->aggregate.func_type == FUNC_MIN ? "MIN"
                : current->project.expressions[i]->aggregate.func_type == FUNC_MAX ? "MAX"
                                                                                   : "UNKNOWN",
                20);
        } else if (current->project.expressions[i]->type == EXPR_VALUE &&
                   strcmp(current->project.expressions[i]->value.char_val, "*") == 0) {
            for (int j = 0; j < schema->column_count; j++) {
                print_centered(schema->columns[j].name, 20);
                printf("│");
            }
            break;
        } else {
            print_centered("expression", 20);
        }
        if (!(current->project.expressions[i]->type == EXPR_VALUE &&
              strcmp(current->project.expressions[i]->value.char_val, "*") == 0 && i == 0)) {
            printf("│");
        }
    }
    printf("\n");

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

static void print_project_row(const IRNode* current, const Row* row, const TableDef* schema) {
    printf("│");
    for (int col_idx = 0; col_idx < current->project.expression_count; col_idx++) {
        if (current->project.expressions[col_idx]->type == EXPR_VALUE &&
            strcmp(current->project.expressions[col_idx]->value.char_val, "*") == 0) {
            for (int j = 0; j < schema->column_count; j++) {
                if (row->is_null[j]) {
                    print_centered("NULL", 20);
                } else {
                    print_centered(repr(&row->values[j]), 20);
                }
                printf("│");
            }
            break;
        } else if (current->project.expressions[col_idx]->type == EXPR_AGGREGATE_FUNC) {
            // Skip aggregate functions
            print_centered("", 20);
        } else {
            Value val = eval_select_expression(current->project.expressions[col_idx], row, schema);
            if (is_null(&val)) {
                print_centered("NULL", 20);
            } else {
                print_centered(repr(&val), 20);
            }
        }
        if (!(current->project.expressions[col_idx]->type == EXPR_VALUE &&
              strcmp(current->project.expressions[col_idx]->value.char_val, "*") == 0 &&
              col_idx == 0)) {
            printf("│");
        }
    }
    printf("\n");
}

/* Returns <0 if a<b, 0 if equal, >0 if a>b */
static int compare_rows_for_sort(const Row* a, const Row* b, const TableDef* schema,
                                 Expr* const* order_by, int order_by_count) {
    for (int i = 0; i < order_by_count; i++) {
        if (order_by[i]->type == EXPR_COLUMN) {
            const char* col_name = order_by[i]->column_name;
            Value val_a = get_column_value(a, schema, col_name);
            Value val_b = get_column_value(b, schema, col_name);

            int cmp = 0;
            if (val_a.type == TYPE_INT && val_b.type == TYPE_INT) {
                cmp = val_a.int_val - val_b.int_val;
            } else if (val_a.type == TYPE_FLOAT || val_b.type == TYPE_FLOAT) {
                cmp = (int)(value_to_double(&val_a) - value_to_double(&val_b));
            } else {
                cmp = strcmp(val_a.char_val, val_b.char_val);
            }

            if (cmp != 0) {
                return cmp;
            }
        }
    }
    return 0;
}

/* Bubble sort ORDER BY columns (asc/desc) */
static void exec_sort(const IRNode* current) {
    log_msg(LOG_DEBUG, "exec_sort: Sorting table: %s by %d columns", current->sort.table_name,
            current->sort.order_by_count);

    Table* table = get_table(current->sort.table_name);
    if (!table) {
        return;
    }

    for (int i = 0; i < table->row_count - 1; i++) {
        for (int j = 0; j < table->row_count - i - 1; j++) {
            int cmp = compare_rows_for_sort(&table->rows[j], &table->rows[j + 1], &table->schema,
                                            current->sort.order_by, current->sort.order_by_count);
            bool swap_needed = false;
            if (cmp != 0) {
                if (current->sort.order_by_desc[0]) {
                    swap_needed = cmp < 0;
                } else {
                    swap_needed = cmp > 0;
                }
            }
            if (swap_needed) {
                Row temp;
                copy_row(&temp, &table->rows[j], table->schema.column_count);
                copy_row(&table->rows[j], &table->rows[j + 1], table->schema.column_count);
                copy_row(&table->rows[j + 1], &temp, table->schema.column_count);
            }
        }
    }
}

static Value eval_select_expression(Expr* expr, const Row* row, const TableDef* schema) {
    if (!expr) return VAL_NULL;
    switch (expr->type) {
        case EXPR_COLUMN:
            return get_column_value(row, schema, expr->column_name);
        case EXPR_VALUE:
            return expr->value;
        case EXPR_BINARY_OP: {
            Value left_val = eval_select_expression(expr->binary.left, row, schema);
            Value right_val = eval_select_expression(expr->binary.right, row, schema);

            if (is_null(&left_val) || is_null(&right_val)) return VAL_NULL;
            if (left_val.type == TYPE_ERROR || right_val.type == TYPE_ERROR) return VAL_ERROR;

            switch (expr->binary.op) {
                case OP_ADD: {
                    if (left_val.type == TYPE_FLOAT || right_val.type == TYPE_FLOAT) {
                        return create_float(value_to_double(&left_val) +
                                            value_to_double(&right_val));
                    } else {
                        return create_int(value_to_double(&left_val) + value_to_double(&right_val));
                    }
                    break;
                }
                case OP_SUBTRACT: {
                    if (left_val.type == TYPE_FLOAT || right_val.type == TYPE_FLOAT) {
                        return create_float(value_to_double(&left_val) -
                                            value_to_double(&right_val));
                    } else {
                        return create_int(value_to_double(&left_val) - value_to_double(&right_val));
                    }
                    break;
                }
                case OP_MULTIPLY: {
                    if (left_val.type == TYPE_FLOAT || right_val.type == TYPE_FLOAT) {
                        return create_float(value_to_double(&left_val) *
                                            value_to_double(&right_val));
                    } else {
                        return create_int(value_to_double(&left_val) * value_to_double(&right_val));
                    }
                    break;
                }
                case OP_DIVIDE: {
                    double right_d = value_to_double(&right_val);
                    if (right_d == 0.0) return VAL_ERROR;
                    if (left_val.type == TYPE_FLOAT || right_val.type == TYPE_FLOAT) {
                        return create_float(value_to_double(&left_val) / right_d);
                    } else {
                        return create_int(value_to_double(&left_val) / right_d);
                    }
                    break;
                }
                case OP_MODULUS: {
                    int right_i = (int)value_to_double(&right_val);
                    if (right_i == 0) return VAL_ERROR;
                    return create_int((int)value_to_double(&left_val) % right_i);
                    break;
                }
                default:
                    return VAL_ERROR;
            }
        }
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
                    "eval_scalar_function: %s function found non-numeric value of type %d",
                    scalar_func_name(expr->scalar.func_type), arg.type);
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
                    "eval_scalar_function: %s function found non-numeric value of type %d",
                    scalar_func_name(expr->scalar.func_type), arg.type);
            return VAL_ERROR;
        }

        double val = value_to_double(&arg);
        int decimals = 0;

        if (expr->scalar.arg_count == 2) {
            Value dec_arg = eval_select_expression(expr->scalar.args[1], row, schema);
            if (is_null(&dec_arg)) return VAL_NULL;
            if (dec_arg.type != TYPE_INT) {
                log_msg(LOG_ERROR,
                        "eval_scalar_function: %s function found non-numeric value of type %d",
                        scalar_func_name(expr->scalar.func_type), arg.type);
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
                    "eval_scalar_function: %s function found non-numeric value of type %d",
                    scalar_func_name(expr->scalar.func_type), arg.type);
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
                    "eval_scalar_function: %s function found non-string value of type %d",
                    scalar_func_name(expr->scalar.func_type), arg.type);
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
                    "eval_scalar_function: %s function found non-string value of type %d",
                    scalar_func_name(expr->scalar.func_type), arg.type);
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
                    "eval_scalar_function: %s function found non-string value of type %d",
                    scalar_func_name(expr->scalar.func_type), arg.type);
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
            log_msg(
                LOG_ERROR,
                "eval_scalar_function: %s function string input found non-string value of type %d",
                scalar_func_name(expr->scalar.func_type), str_arg.type);
            return VAL_ERROR;
        }

        if (start_arg.type != TYPE_INT) {
            log_msg(
                LOG_ERROR,
                "eval_scalar_function: %s function index input found non-integer value of type %d",
                scalar_func_name(expr->scalar.func_type), start_arg.type);
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
            log_msg(
                LOG_ERROR,
                "eval_scalar_function: %s function string input found non-string value of type %d",
                scalar_func_name(expr->scalar.func_type), str_arg.type);
            return VAL_ERROR;
        }

        if (len_arg.type != TYPE_INT) {
            log_msg(
                LOG_ERROR,
                "eval_scalar_function: %s function index input found non-integer value of type %d",
                scalar_func_name(expr->scalar.func_type), len_arg.type);
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
            log_msg(
                LOG_ERROR,
                "eval_scalar_function: %s function string input found non-string value of type %d",
                scalar_func_name(expr->scalar.func_type), str_arg.type);
            return VAL_ERROR;
        }

        if (len_arg.type != TYPE_INT) {
            log_msg(
                LOG_ERROR,
                "eval_scalar_function: %s function index input found non-integer value of type %d",
                scalar_func_name(expr->scalar.func_type), len_arg.type);
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

    log_msg(LOG_ERROR, "eval_scalar_function: Unknown scalar function with type %d",
            expr->scalar.func_type);
    return VAL_ERROR;
}

static void exec_delete_row(const IRNode* current) {
    log_msg(LOG_DEBUG, "exec_delete_row: Deleting rows from table: %s",
            current->delete_row.table_name);

    Table* table = get_table(current->delete_row.table_name);
    if (!table) {
        char suggestion[256];
        const char* table_names[MAX_TABLES];
        extern Table tables[];
        extern int table_count;
        for (int i = 0; i < table_count; i++) {
            table_names[i] = tables[i].name;
        }
        suggest_similar(current->delete_row.table_name, table_names, table_count, suggestion,
                        sizeof(suggestion));
        show_prominent_error("Table '%s' does not exist", current->delete_row.table_name);
        if (strlen(suggestion) > 0) {
            printf("  %s\n", suggestion);
        }
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

            case IR_SORT:
                exec_sort(current);
                break;

            default:
                log_msg(LOG_WARN, "exec_ir: Unknown IR node type: %d", current->type);
                break;
        }

        current = current->next;
    }

    log_msg(LOG_DEBUG, "exec_ir: IR execution completed");
}
