#include <ctype.h>
#include <limits.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>

#include "arraylist.h"
#include "db.h"
#include "logger.h"
#include "table.h"
#include "values.h"

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
    log_msg(LOG_DEBUG, "exec_filter: Filtering table ID: %d", current->filter.table_id);

    Table* table = get_table_by_id(current->filter.table_id);
    if (!table) {
        log_msg(LOG_ERROR, "exec_filter: Table with ID %d does not exist", current->filter.table_id);
        return;
    }

    log_msg(LOG_INFO, "exec_filter: Displaying filtered table '%s'", table->name);

    int filtered_count = 0;
    int row_count = alist_length(&table->rows);
    for (int row_idx = 0; row_idx < row_count; row_idx++) {
        Row* row = (Row*)alist_get(&table->rows, row_idx);
        if (row && eval_expression(current->filter.filter_expr, row, &table->schema)) {
            filtered_count++;
        }
    }

    log_msg(LOG_INFO, "exec_filter: Found %d matching rows", filtered_count);
}

static void exec_update_row(const IRNode* current) {
    log_msg(LOG_DEBUG, "exec_update_row: Updating rows in table ID: %d", current->update_row.table_id);

    Table* table = get_table_by_id(current->update_row.table_id);
    if (!table) {
        log_msg(LOG_ERROR, "exec_update_row: Table with ID %d does not exist", current->update_row.table_id);
        return;
    }

    int updated_rows = 0;
    int row_count = alist_length(&table->rows);
    int value_count = alist_length(&current->update_row.values);
    for (int row_idx = 0; row_idx < row_count; row_idx++) {
        Row* row = (Row*)alist_get(&table->rows, row_idx);
        if (!row) continue;

        if (!current->update_row.where_clause ||
            eval_expression(current->update_row.where_clause, row, &table->schema)) {
            for (int i = 0; i < value_count; i++) {
                ColumnValue* cv = (ColumnValue*)alist_get(&current->update_row.values, i);
                if (!cv) continue;
                int schema_col_count = alist_length(&table->schema.columns);
                for (int col_idx = 0; col_idx < schema_col_count; col_idx++) {
                    ColumnDef* col = (ColumnDef*)alist_get(&table->schema.columns, col_idx);
                    if (!col) continue;
                    if (strcmp(col->name, cv->column_name) == 0) {
                        DataType expected_type = col->type;
                        Value source_val = cv->value;

                        if (source_val.type != expected_type && source_val.type != TYPE_NULL) {
                            Value converted;
                            if (try_convert_value(&source_val, expected_type, &converted)) {
                                if (row->values[col_idx].type == TYPE_STRING && row->values[col_idx].char_val) {
                                    free(row->values[col_idx].char_val);
                                }
                                row->values[col_idx] = converted;
                            } else {
                                log_msg(LOG_ERROR,
                                        "exec_update_row: Type mismatch for column '%s' (expected %s, got %s)",
                                        col->name,
                                        expected_type == TYPE_INT ? "INT" :
                                        expected_type == TYPE_FLOAT ? "FLOAT" :
                                        expected_type == TYPE_STRING ? "STRING" : "UNKNOWN",
                                        source_val.type == TYPE_INT ? "INT" :
                                        source_val.type == TYPE_FLOAT ? "FLOAT" :
                                        source_val.type == TYPE_STRING ? "STRING" : "UNKNOWN");
                                if (row->values[col_idx].type == TYPE_STRING && row->values[col_idx].char_val) {
                                    free(row->values[col_idx].char_val);
                                }
                                row->values[col_idx] = VAL_NULL;
                            }
                        } else {
                            if (row->values[col_idx].type == TYPE_STRING && row->values[col_idx].char_val) {
                                free(row->values[col_idx].char_val);
                            }
                            row->values[col_idx] = copy_value(&source_val);
                        }

                        if (!check_not_null_constraint(table, col_idx, &row->values[col_idx])) {
                            log_msg(LOG_ERROR, "exec_update_row: NOT NULL constraint violated for column '%s'",
                                    col->name);
                            return;
                        }

                        if (col->is_unique) {
                            if (!check_unique_constraint(table, col_idx, &row->values[col_idx], row_idx)) {
                                log_msg(LOG_ERROR, "exec_update_row: UNIQUE constraint violated for column '%s'",
                                        col->name);
                                return;
                            }
                        }

                        if (col->is_foreign_key) {
                            if (!check_foreign_key_constraint(table, col_idx, &row->values[col_idx])) {
                                log_msg(LOG_ERROR, "exec_update_row: FOREIGN KEY constraint violated for column '%s'",
                                        col->name);
                                return;
                            }
                        }
                        break;
                    }
                }
                updated_rows++;
            }
        }
    }
    log_msg(LOG_INFO, "exec_update_row: Updated %d rows in table '%s'", updated_rows,
            table->name);
}

static void exec_aggregate(const IRNode* current) {
    log_msg(LOG_DEBUG, "exec_aggregate: Computing aggregate: %s",
            current->aggregate.func_type == FUNC_SUM     ? "SUM"
            : current->aggregate.func_type == FUNC_AVG   ? "AVG"
            : current->aggregate.func_type == FUNC_COUNT ? "COUNT"
            : current->aggregate.func_type == FUNC_MIN   ? "MIN"
            : current->aggregate.func_type == FUNC_MAX ? "MAX"
                                                         : "UNKNOWN");

    Table* table = get_table_by_id(current->aggregate.table_id);
    if (!table) {
        log_msg(LOG_ERROR, "exec_aggregate: Table with ID %d does not exist", current->aggregate.table_id);
        return;
    }

    AggState state = {0};
    if (current->aggregate.distinct) {
        state.is_distinct = true;
    }

    int table_row_count = alist_length(&table->rows);
    if (current->aggregate.func_type == FUNC_COUNT && current->aggregate.count_all) {
        log_msg(LOG_DEBUG, "exec_aggregate: COUNT(*) detected, using row_count");
        state.count = table_row_count;
    } else {
        log_msg(LOG_DEBUG, "exec_aggregate: %s with operand, count_all=%d",
                (current->aggregate.func_type == FUNC_COUNT ? "COUNT"
                 : current->aggregate.func_type == FUNC_SUM ? "SUM"
                 : current->aggregate.func_type == FUNC_AVG ? "AVG"
                 : current->aggregate.func_type == FUNC_MIN ? "MIN"
                 : current->aggregate.func_type == FUNC_MAX ? "MAX"
                                                            : "UNKNOWN"),
                current->aggregate.count_all);
        for (int row_idx = 0; row_idx < table_row_count; row_idx++) {
            Row* row = (Row*)alist_get(&table->rows, row_idx);
            if (!row) continue;
            Value val = VAL_NULL;
            if (current->aggregate.operand->type == EXPR_COLUMN) {
                val = get_column_value(row, &table->schema,
                                       current->aggregate.operand->column_name);
            } else if (current->aggregate.operand->type == EXPR_VALUE) {
                val = current->aggregate.operand->value;
            } else {
                val = eval_select_expression(current->aggregate.operand, row,
                                             &table->schema);
            }
            update_aggregate_state(&state, &val, &current->aggregate);
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
        int seen_count = alist_length(&state.seen_values);
        for (int i = 0; i < seen_count; i++) {
            Value* seen = (Value*)alist_get(&state.seen_values, i);
            if (seen) {
                free(seen);
            }
        }
    }
}

static void exec_project(const IRNode* current) {
    log_msg(LOG_DEBUG, "exec_project: Projecting expressions for table ID: %d",
            current->project.table_id);

    Table* table = get_table_by_id(current->project.table_id);
    if (!table) {
        log_msg(LOG_ERROR, "exec_project: Table with ID %d does not exist", current->project.table_id);
        return;
    }

    int table_row_count = alist_length(&table->rows);
    int display_limit = table_row_count;
    if (current->project.limit > 0 && current->project.limit < display_limit) {
        display_limit = current->project.limit;
    }

    print_project_header(current, &table->schema);

    for (int row_idx = 0; row_idx < display_limit; row_idx++) {
        Row* row = (Row*)alist_get(&table->rows, row_idx);
        if (row) {
            print_project_row(current, row, &table->schema);
        }
    }

    int column_count = alist_length(&current->project.expressions);

    Expr** first_expr = (Expr**)alist_get(&current->project.expressions, 0);
    if (column_count == 1 && first_expr && first_expr[0]->type == EXPR_VALUE &&
        strcmp(first_expr[0]->value.char_val, "*") == 0) {
        column_count = alist_length(&table->schema.columns);
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
    int column_count = alist_length(&schema->columns);
    for (int i = 0; i < column_count; i++) {
        ColumnDef* col = (ColumnDef*)alist_get(&schema->columns, i);
        if (!col) continue;
        if (strcmp(col->name, column_name) == 0) {
            if (row->values[i].type == TYPE_NULL) {
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
    int column_count = alist_length(&current->project.expressions);

    Expr** first_expr = (Expr**)alist_get(&current->project.expressions, 0);
    if (column_count == 1 && first_expr && first_expr[0]->type == EXPR_VALUE &&
        strcmp(first_expr[0]->value.char_val, "*") == 0) {
        column_count = alist_length(&schema->columns);
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
    int expr_count = alist_length(&current->project.expressions);
    for (int i = 0; i < expr_count; i++) {
        Expr** expr = (Expr**)alist_get(&current->project.expressions, i);
        if (expr[0]->type == EXPR_COLUMN) {
            print_centered(expr[0]->column_name, 20);
        } else if (expr[0]->type == EXPR_AGGREGATE_FUNC) {
            print_centered(
                expr[0]->aggregate.func_type == FUNC_COUNT ? "COUNT"
                : expr[0]->aggregate.func_type == FUNC_SUM ? "SUM"
                : expr[0]->aggregate.func_type == FUNC_AVG ? "AVG"
                : expr[0]->aggregate.func_type == FUNC_MIN ? "MIN"
                : expr[0]->aggregate.func_type == FUNC_MAX ? "MAX"
                                                           : "UNKNOWN",
                20);
        } else if (expr[0]->type == EXPR_VALUE &&
                   strcmp(expr[0]->value.char_val, "*") == 0) {
            int schema_col_count = alist_length(&schema->columns);
            for (int j = 0; j < schema_col_count; j++) {
                ColumnDef* col = (ColumnDef*)alist_get(&schema->columns, j);
                if (col) {
                    print_centered(col->name, 20);
                }
                printf("│");
            }
            break;
        } else {
            print_centered("expression", 20);
        }
        if (!(expr[0]->type == EXPR_VALUE &&
              strcmp(expr[0]->value.char_val, "*") == 0 && i == 0)) {
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
    int expr_count = alist_length(&current->project.expressions);
    for (int col_idx = 0; col_idx < expr_count; col_idx++) {
        Expr** expr = (Expr**)alist_get(&current->project.expressions, col_idx);
        if (expr[0]->type == EXPR_VALUE &&
            strcmp(expr[0]->value.char_val, "*") == 0) {
            int schema_col_count = alist_length(&schema->columns);
            for (int j = 0; j < schema_col_count; j++) {
                if (row->values[j].type == TYPE_NULL) {
                    print_centered("NULL", 20);
                } else {
                    print_centered(repr(&row->values[j]), 20);
                }
                printf("│");
            }
            break;
        } else if (expr[0]->type == EXPR_AGGREGATE_FUNC) {
            print_centered("", 20);
        } else {
            Value val = eval_select_expression(expr[0], row, schema);
            if (is_null(&val)) {
                print_centered("NULL", 20);
            } else {
                print_centered(repr(&val), 20);
            }
        }
        if (!(expr[0]->type == EXPR_VALUE &&
              strcmp(expr[0]->value.char_val, "*") == 0 &&
              col_idx == 0)) {
            printf("│");
        }
    }
    printf("\n");
}

/* Returns <0 if a<b, 0 if equal, >0 if a>b */
static int compare_rows_for_sort(const Row* a, const Row* b, const TableDef* schema,
                                 const ArrayList* order_by, int order_by_count) {
    for (int i = 0; i < order_by_count; i++) {
        Expr** expr = (Expr**)alist_get(order_by, i);
        if (expr[0]->type == EXPR_COLUMN) {
            const char* col_name = expr[0]->column_name;
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
    int order_by_count = alist_length(&current->sort.order_by);
    log_msg(LOG_DEBUG, "exec_sort: Sorting table ID: %d by %d columns", current->sort.table_id,
            order_by_count);

    Table* table = get_table_by_id(current->sort.table_id);
    if (!table) {
        log_msg(LOG_ERROR, "exec_sort: Table with ID %d does not exist", current->sort.table_id);
        return;
    }

    int row_count = alist_length(&table->rows);
    for (int i = 0; i < row_count - 1; i++) {
        for (int j = 0; j < row_count - i - 1; j++) {
            Row* row_j = (Row*)alist_get(&table->rows, j);
            Row* row_j1 = (Row*)alist_get(&table->rows, j + 1);
            if (!row_j || !row_j1) continue;
            int cmp = compare_rows_for_sort(row_j, row_j1, &table->schema,
                                            &current->sort.order_by, order_by_count);
            bool swap_needed = false;
            if (cmp != 0) {
                if (*(bool*)alist_get(&current->sort.order_by_desc, 0)) {
                    swap_needed = cmp < 0;
                } else {
                    swap_needed = cmp > 0;
                }
            }
            if (swap_needed) {
                Row temp;
                memcpy(&temp, row_j, sizeof(Row));
                memcpy(row_j, row_j1, sizeof(Row));
                memcpy(row_j1, &temp, sizeof(Row));
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
            int requested_len = (int)value_to_double(&length_arg);
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

            char str_buf[64];
            if (arg.type == TYPE_STRING) {
                snprintf(str_buf, sizeof(str_buf), "%s", arg.char_val);
            } else if (arg.type == TYPE_INT) {
                snprintf(str_buf, sizeof(str_buf), "%d", arg.int_val);
            } else if (arg.type == TYPE_FLOAT) {
                snprintf(str_buf, sizeof(str_buf), "%.2f", arg.float_val);
            } else {
                continue;
            }

            if (strlen(result.char_val) + strlen(str_buf) < MAX_STRING_LEN - 1) {
                strcat(result.char_val, str_buf);
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
    log_msg(LOG_DEBUG, "exec_delete_row: Deleting rows from table ID: %d",
            current->delete_row.table_id);

    Table* table = get_table_by_id(current->delete_row.table_id);
    if (!table) {
        log_msg(LOG_ERROR, "exec_delete_row: Table with ID %d does not exist", current->delete_row.table_id);
        return;
    }

    int deleted_rows = 0;
    ArrayList kept_rows;
    alist_init(&kept_rows, sizeof(Row), free_row_contents);

    int row_count = alist_length(&table->rows);
    for (int read_idx = 0; read_idx < row_count; read_idx++) {
        Row* row = (Row*)alist_get(&table->rows, read_idx);
        if (!row) continue;

        if (!current->delete_row.where_clause ||
            !eval_expression(current->delete_row.where_clause, row, &table->schema)) {
            Row* kept = (Row*)alist_append(&kept_rows);
            if (kept) {
                kept->value_capacity = row->value_count > 0 ? row->value_count : 4;
                kept->values = malloc(kept->value_capacity * sizeof(Value));
                kept->value_count = 0;
                if (kept->values) {
                    copy_row(kept, row, 0);
                }
            }
        } else {
            deleted_rows++;
        }
    }

    alist_destroy(&table->rows);
    table->rows = kept_rows;

    log_msg(LOG_INFO, "exec_delete_row: Deleted %d rows from table '%s'", deleted_rows,
            table->name);
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

            case IR_CREATE_INDEX:
                exec_create_index(current);
                break;

            case IR_DROP_INDEX:
                exec_drop_index(current);
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

static QueryResult* g_last_result = NULL;

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

static void capture_project_result(const IRNode* current) {
    free_query_result(g_last_result);
    g_last_result = NULL;

    Table* table = get_table_by_id(current->project.table_id);
    if (!table) {
        log_msg(LOG_ERROR, "capture_project_result: Table with ID %d does not exist", current->project.table_id);
        return;
    }

    g_last_result = malloc(sizeof(QueryResult));
    if (!g_last_result) return;

    memset(g_last_result, 0, sizeof(QueryResult));
    g_last_result->col_count = alist_length(&current->project.expressions);
    g_last_result->column_names = malloc(g_last_result->col_count * sizeof(char*));
    for (int i = 0; i < g_last_result->col_count; i++) {
        Expr** expr = (Expr**)alist_get(&current->project.expressions, i);
        const char* alias = expr[0]->alias;
        g_last_result->column_names[i] = malloc(strlen(alias) + 1);
        strcpy(g_last_result->column_names[i], alias);
        if (!g_last_result->column_names[i][0]) {
            free(g_last_result->column_names[i]);
            g_last_result->column_names[i] = malloc(5);
            strcpy(g_last_result->column_names[i], "expr");
        }
    }

    int capacity = 16;
    g_last_result->values = malloc(capacity * sizeof(Value));
    g_last_result->rows = malloc(capacity * sizeof(int));
    g_last_result->row_count = 0;

    int table_row_count = alist_length(&table->rows);
    for (int i = 0; i < table_row_count; i++) {
        Row* row = (Row*)alist_get(&table->rows, i);
        if (!row || row->value_count == 0) continue;

        for (int j = 0; j < g_last_result->col_count; j++) {
            if (g_last_result->row_count * g_last_result->col_count + j >= capacity) {
                capacity *= 2;
                g_last_result->values = realloc(g_last_result->values, capacity * sizeof(Value));
                g_last_result->rows = realloc(g_last_result->rows, capacity * sizeof(int));
            }
            Expr** expr = (Expr**)alist_get(&current->project.expressions, j);
            Value val = eval_select_expression(expr[0], row, &table->schema);
            g_last_result->values[g_last_result->row_count * g_last_result->col_count + j] = val;
        }
        g_last_result->rows[g_last_result->row_count] = i;
        g_last_result->row_count++;
    }
}

void clear_query_result(void) {
    free_query_result(g_last_result);
    g_last_result = NULL;
}

QueryResult* exec_query(const char* sql) {
    Token* tokens = tokenize(sql);
    ASTNode* ast = parse(tokens);
    if (!ast) {
        free_tokens(tokens);
        return NULL;
    }

    IRNode* ir = ast_to_ir(ast);
    if (!ir) {
        free_tokens(tokens);
        free_ast(ast);
        return NULL;
    }

    for (IRNode* current = ir; current != NULL; current = current->next) {
        switch (current->type) {
            case IR_PROJECT:
                capture_project_result(current);
                break;
            default:
                break;
        }
    }

    exec_ir(ir);
    free_ir(ir);
    free_tokens(tokens);
    free_ast(ast);

    QueryResult* result = g_last_result;
    g_last_result = NULL;
    return result;
}
