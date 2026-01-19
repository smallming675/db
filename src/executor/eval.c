#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#include "arraylist.h"
#include "db.h"
#include "executor_internal.h"
#include "logger.h"
#include "values.h"

bool eval_expression(const Expr* expr, const Row* row, const TableDef* schema) {
    if (!expr) return true;

    switch (expr->type) {
        case EXPR_COLUMN: {
            Value val = get_column_value(row, schema, expr->column_name);
            return !is_null(&val);
        }
        case EXPR_VALUE:
            return !is_null(&expr->value);
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
                case OP_GREATER_EQUAL:
                case OP_LIKE: {
                    Expr* left = expr->binary.left;
                    Expr* right = expr->binary.right;
                    if (left->type == EXPR_COLUMN && right->type == EXPR_VALUE) {
                        return eval_comparison(get_column_value(row, schema, left->column_name),
                                               right->value, expr->binary.op);
                    }
                    if (left->type == EXPR_VALUE && right->type == EXPR_COLUMN) {
                        return eval_comparison(left->value,
                                               get_column_value(row, schema, right->column_name),
                                               expr->binary.op);
                    }
                    if (left->type == EXPR_COLUMN && right->type == EXPR_COLUMN) {
                        return eval_comparison(get_column_value(row, schema, left->column_name),
                                               get_column_value(row, schema, right->column_name),
                                               expr->binary.op);
                    }
                    return false;
                }
                default:
                    return false;
            }
        }
        case EXPR_UNARY_OP: {
            bool operand_val = eval_expression(expr->unary.operand, row, schema);
            return expr->unary.op == OP_NOT ? !operand_val : false;
        }
        default:
            return false;
    }
}

bool eval_cmp_operation(const Expr* expr, const Row* row, const TableDef* left_schema,
                        const TableDef* right_schema, int left_col_count) {
    Expr* left = expr->binary.left;
    Expr* right = expr->binary.right;
    if (left->type == EXPR_COLUMN && right->type == EXPR_VALUE) {
        return eval_comparison(get_column_value_from_join(row, left_schema, right_schema,
                                                          left_col_count, left->column_name),
                               right->value, expr->binary.op);
    }
    if (left->type == EXPR_VALUE && right->type == EXPR_COLUMN) {
        return eval_comparison(left->value,
                               get_column_value_from_join(row, left_schema, right_schema,
                                                          left_col_count, right->column_name),
                               expr->binary.op);
    }
    if (left->type == EXPR_COLUMN && right->type == EXPR_COLUMN) {
        return eval_comparison(get_column_value_from_join(row, left_schema, right_schema,
                                                          left_col_count, left->column_name),
                               get_column_value_from_join(row, left_schema, right_schema,
                                                          left_col_count, right->column_name),
                               expr->binary.op);
    }
    return false;
}
bool eval_binary_operation(const Expr* expr, const Row* row, const TableDef* left_schema,
                           const TableDef* right_schema, int left_col_count) {
    bool left_val =
        eval_expression_for_join(expr->binary.left, row, left_schema, right_schema, left_col_count);
    bool right_val = eval_expression_for_join(expr->binary.right, row, left_schema, right_schema,
                                              left_col_count);

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
        case OP_GREATER_EQUAL:
            return eval_cmp_operation(expr, row, left_schema, right_schema, left_col_count);
        default:
            return false;
    }
}

bool eval_expression_for_join(const Expr* expr, const Row* row, const TableDef* left_schema,
                              const TableDef* right_schema, int left_col_count) {
    if (!expr) return true;

    switch (expr->type) {
        case EXPR_COLUMN: {
            Value val = get_column_value_from_join(row, left_schema, right_schema, left_col_count,
                                                   expr->column_name);
            return !is_null(&val);
        }
        case EXPR_VALUE:
            return !is_null(&expr->value);
        case EXPR_BINARY_OP:
            return eval_binary_operation(expr, row, left_schema, right_schema, left_col_count);

        case EXPR_UNARY_OP: {
            bool operand_val = eval_expression_for_join(expr->unary.operand, row, left_schema,
                                                        right_schema, left_col_count);
            return expr->unary.op == OP_NOT ? !operand_val : false;
        }
        default:
            return false;
    }
}

static Value eval_arithmetic_op(OperatorType op, Value left, Value right) {
    Value result = {0};

    switch (op) {
        case OP_ADD:
            if (left.type == TYPE_INT && right.type == TYPE_INT) {
                result.type = TYPE_INT;
                result.int_val = left.int_val + right.int_val;
            } else {
                result.type = TYPE_FLOAT;
                double l = left.type == TYPE_INT ? (double)left.int_val : left.float_val;
                double r = right.type == TYPE_INT ? (double)right.int_val : right.float_val;
                result.float_val = l + r;
            }
            break;
        case OP_SUBTRACT:
            if (left.type == TYPE_INT && right.type == TYPE_INT) {
                result.type = TYPE_INT;
                result.int_val = left.int_val - right.int_val;
            } else {
                result.type = TYPE_FLOAT;
                double l = left.type == TYPE_INT ? (double)left.int_val : left.float_val;
                double r = right.type == TYPE_INT ? (double)right.int_val : right.float_val;
                result.float_val = l - r;
            }
            break;
        case OP_MULTIPLY:
            if (left.type == TYPE_INT && right.type == TYPE_INT) {
                result.type = TYPE_INT;
                result.int_val = left.int_val * right.int_val;
            } else {
                result.type = TYPE_FLOAT;
                double l = left.type == TYPE_INT ? (double)left.int_val : left.float_val;
                double r = right.type == TYPE_INT ? (double)right.int_val : right.float_val;
                result.float_val = l * r;
            }
            break;
        case OP_DIVIDE: {
            double denom = right.type == TYPE_INT ? (double)right.int_val : right.float_val;
            if (denom != 0) {
                if (left.type == TYPE_INT && right.type == TYPE_INT) {
                    result.type = TYPE_INT;
                    result.int_val = left.int_val / right.int_val;
                } else {
                    result.type = TYPE_FLOAT;
                    double l = left.type == TYPE_INT ? (double)left.int_val : left.float_val;
                    result.float_val = l / denom;
                }
            }
            break;
        }
        case OP_MODULUS:
            if (left.type == TYPE_INT && right.type == TYPE_INT) {
                result.type = TYPE_INT;
                result.int_val = left.int_val % right.int_val;
            }
            break;
        default:
            break;
    }
    return result;
}

static Value eval_comparison_op(OperatorType op, Value left, Value right) {
    Value result = {0};
    result.type = TYPE_INT;
    result.int_val = eval_comparison(left, right, op) ? 1 : 0;
    return result;
}

static Value eval_subquery(const Expr* expr) {
    Value result = {0};
    result.type = TYPE_NULL;

    if (!expr || expr->type != EXPR_SUBQUERY || !expr->subquery.subquery) {
        log_msg(LOG_DEBUG, "eval_subquery: early return, expr=%p", expr);
        return result;
    }

    log_msg(LOG_DEBUG, "eval_subquery: executing subquery");

    ASTNode* subquery_ast = expr->subquery.subquery;

    QueryResult* saved_result = get_last_query_result();
    set_last_query_result(NULL);

    exec_ast(subquery_ast);

    QueryResult* last_result = get_last_query_result();
    if (last_result && alist_length(&last_result->rows) > 0) {
        int col_count = last_result->col_count;
        if (col_count > 0) {
            Value* val = (Value*)alist_get(&last_result->values, 0);
            if (val) result = copy_string_value(val);
        }
    }

    free_query_result(last_result);
    set_last_query_result(saved_result);

    return result;
}

Value eval_scalar_function(const Expr* expr, const Row* row, const TableDef* schema);

Value eval_select_expression(Expr* expr, const Row* row, const TableDef* schema) {
    Value result = {0};
    result.type = TYPE_NULL;

    if (!expr) return result;

    switch (expr->type) {
        case EXPR_COLUMN: {
            Value col_val = get_column_value(row, schema, expr->column_name);
            return copy_string_value(&col_val);
        }
        case EXPR_VALUE:
            return copy_string_value(&expr->value);
        case EXPR_BINARY_OP: {
            Value left = eval_select_expression(expr->binary.left, row, schema);
            Value right = eval_select_expression(expr->binary.right, row, schema);

            switch (expr->binary.op) {
                case OP_ADD:
                case OP_SUBTRACT:
                case OP_MULTIPLY:
                case OP_DIVIDE:
                case OP_MODULUS:
                    return eval_arithmetic_op(expr->binary.op, left, right);
                case OP_EQUALS:
                case OP_NOT_EQUALS:
                case OP_LESS:
                case OP_LESS_EQUAL:
                case OP_GREATER:
                case OP_GREATER_EQUAL:
                case OP_LIKE:
                case OP_AND:
                case OP_OR:
                    return eval_comparison_op(expr->binary.op, left, right);
                default:
                    break;
            }
            break;
        }
        case EXPR_UNARY_OP: {
            Value operand = eval_select_expression(expr->unary.operand, row, schema);
            if (expr->unary.op == OP_NOT) {
                result.type = TYPE_INT;
                result.int_val = (operand.type == TYPE_NULL) ? 1 : 0;
            } else {
                result = operand;
            }
            break;
        }
        case EXPR_AGGREGATE_FUNC:
            log_msg(LOG_ERROR,
                    "eval_select_expression: Cannot evaluate aggregate in non-aggregate context");
            break;
        case EXPR_SCALAR_FUNC:
            return eval_scalar_function(expr, row, schema);
        case EXPR_SUBQUERY:
            return eval_subquery(expr);
        default:
            log_msg(LOG_WARN, "eval_select_expression: Unknown expression type %d", expr->type);
    }
    return result;
}
