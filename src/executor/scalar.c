#include <ctype.h>
#include <math.h>
#include <string.h>

#include "db.h"
#include "executor_internal.h"

static Value eval_string_func_upper(const Value* arg1) {
    Value result = {0};
    result.type = TYPE_STRING;
    if (arg1->type == TYPE_STRING && arg1->char_val) {
        result.char_val = malloc(strlen(arg1->char_val) + 1);
        for (size_t i = 0; i <= strlen(arg1->char_val); i++) {
            result.char_val[i] = toupper((unsigned char)arg1->char_val[i]);
        }
    } else {
        result.char_val = malloc(1);
        result.char_val[0] = '\0';
    }
    return result;
}

static Value eval_string_func_lower(const Value* arg1) {
    Value result = {0};
    result.type = TYPE_STRING;
    if (arg1->type == TYPE_STRING && arg1->char_val) {
        result.char_val = malloc(strlen(arg1->char_val) + 1);
        for (size_t i = 0; i <= strlen(arg1->char_val); i++) {
            result.char_val[i] = tolower((unsigned char)arg1->char_val[i]);
        }
    } else {
        result.char_val = malloc(1);
        result.char_val[0] = '\0';
    }
    return result;
}

static Value eval_string_func_len(const Value* arg1) {
    Value result = {0};
    result.type = TYPE_INT;
    result.int_val = (arg1->type == TYPE_STRING && arg1->char_val) ? strlen(arg1->char_val) : 0;
    return result;
}

static Value eval_string_func_mid(const Value* arg1, const Value* arg2, const Value* arg3) {
    Value result = {0};
    result.type = TYPE_STRING;
    if (arg1->type == TYPE_STRING && arg1->char_val) {
        int start = arg2->type == TYPE_INT ? arg2->int_val : 0;
        int len = arg3->type == TYPE_INT ? arg3->int_val : 0;
        int src_len = strlen(arg1->char_val);
        if (start < src_len) {
            int copy_len = (start + len > src_len) ? (src_len - start) : len;
            result.char_val = malloc(copy_len + 1);
            memcpy(result.char_val, arg1->char_val + start, copy_len);
            result.char_val[copy_len] = '\0';
        } else {
            result.char_val = malloc(1);
            result.char_val[0] = '\0';
        }
    } else {
        result.char_val = malloc(1);
        result.char_val[0] = '\0';
    }
    return result;
}

static Value eval_string_func_left(const Value* arg1, const Value* arg2) {
    Value result = {0};
    result.type = TYPE_STRING;
    if (arg1->type == TYPE_STRING && arg1->char_val) {
        int len = arg2->type == TYPE_INT ? arg2->int_val : 0;
        int src_len = strlen(arg1->char_val);
        int copy_len = len < src_len ? len : src_len;
        result.char_val = malloc(copy_len + 1);
        memcpy(result.char_val, arg1->char_val, copy_len);
        result.char_val[copy_len] = '\0';
    } else {
        result.char_val = malloc(1);
        result.char_val[0] = '\0';
    }
    return result;
}

static Value eval_string_func_right(const Value* arg1, const Value* arg2) {
    Value result = {0};
    result.type = TYPE_STRING;
    if (arg1->type == TYPE_STRING && arg1->char_val) {
        int len = arg2->type == TYPE_INT ? arg2->int_val : 0;
        int src_len = strlen(arg1->char_val);
        int start = src_len - len;
        if (start < 0) start = 0;
        int copy_len = src_len - start;
        result.char_val = malloc(copy_len + 1);
        memcpy(result.char_val, arg1->char_val + start, copy_len);
        result.char_val[copy_len] = '\0';
    } else {
        result.char_val = malloc(1);
        result.char_val[0] = '\0';
    }
    return result;
}

static Value eval_string_func_concat(const Expr* expr, const Row* row, const TableDef* schema) {
    Value result = {0};
    result.type = TYPE_STRING;
    size_t total_len = 0;
    
    for (int i = 0; i < expr->scalar.arg_count; i++) {
        Value arg = eval_select_expression(expr->scalar.args[i], row, schema);
        if (arg.type == TYPE_STRING && arg.char_val) {
            total_len += strlen(arg.char_val);
        }
    }
    result.char_val = malloc(total_len + 1);
    result.char_val[0] = '\0';
    for (int i = 0; i < expr->scalar.arg_count; i++) {
        Value arg = eval_select_expression(expr->scalar.args[i], row, schema);
        if (arg.type == TYPE_STRING && arg.char_val) {
            strcat(result.char_val, arg.char_val);
        }
    }
    return result;
}

static Value eval_string_function(const Expr* expr, const Row* row, const TableDef* schema) {
    Value arg1 = {0};
    if (expr->scalar.arg_count > 0) {
        arg1 = eval_select_expression(expr->scalar.args[0], row, schema);
    }
    if (arg1.type == TYPE_NULL) {
        Value null_result = {0};
        null_result.type = TYPE_NULL;
        return null_result;
    }

    switch (expr->scalar.func_type) {
        case FUNC_UPPER: return eval_string_func_upper(&arg1);
        case FUNC_LOWER: return eval_string_func_lower(&arg1);
        case FUNC_LEN:   return eval_string_func_len(&arg1);
        case FUNC_MID: {
            Value arg2 = {0}, arg3 = {0};
            if (expr->scalar.arg_count >= 3) {
                arg2 = eval_select_expression(expr->scalar.args[1], row, schema);
                arg3 = eval_select_expression(expr->scalar.args[2], row, schema);
            }
            return eval_string_func_mid(&arg1, &arg2, &arg3);
        }
        case FUNC_LEFT: {
            Value arg2 = {0};
            if (expr->scalar.arg_count >= 2) {
                arg2 = eval_select_expression(expr->scalar.args[1], row, schema);
            }
            return eval_string_func_left(&arg1, &arg2);
        }
        case FUNC_RIGHT: {
            Value arg2 = {0};
            if (expr->scalar.arg_count >= 2) {
                arg2 = eval_select_expression(expr->scalar.args[1], row, schema);
            }
            return eval_string_func_right(&arg1, &arg2);
        }
        case FUNC_CONCAT: return eval_string_func_concat(expr, row, schema);
        default: break;
    }
    Value err = {0};
    err.type = TYPE_ERROR;
    return err;
}

static Value eval_math_func_abs(const Value* arg) {
    Value result = {0};
    if (arg->type == TYPE_INT) {
        result.type = TYPE_INT;
        result.int_val = abs(arg->int_val);
    } else if (arg->type == TYPE_FLOAT) {
        result.type = TYPE_FLOAT;
        result.float_val = fabs(arg->float_val);
    }
    return result;
}

static Value eval_math_func_sqrt(const Value* arg) {
    Value result = {0};
    if (arg->type == TYPE_INT || arg->type == TYPE_FLOAT) {
        result.type = TYPE_FLOAT;
        result.float_val = sqrt(arg->type == TYPE_INT ? (double)arg->int_val : arg->float_val);
    }
    return result;
}

static Value eval_math_func_mod(const Value* arg) {
    Value result = {0};
    if (arg->type == TYPE_INT) {
        result.type = TYPE_INT;
        result.int_val = arg->int_val % 10;
    }
    return result;
}

static Value eval_math_func_pow(const Value* arg) {
    Value result = {0};
    if (arg->type == TYPE_INT || arg->type == TYPE_FLOAT) {
        result.type = TYPE_FLOAT;
        result.float_val = pow(arg->type == TYPE_INT ? (double)arg->int_val : arg->float_val, 2.0);
    }
    return result;
}

static Value eval_math_func_round(const Value* arg) {
    Value result = {0};
    if (arg->type == TYPE_FLOAT) {
        result.type = TYPE_FLOAT;
        result.float_val = round(arg->float_val);
    }
    return result;
}

static Value eval_math_func_floor(const Value* arg) {
    Value result = {0};
    if (arg->type == TYPE_FLOAT) {
        result.type = TYPE_FLOAT;
        result.float_val = floor(arg->float_val);
    }
    return result;
}

static Value eval_math_func_ceil(const Value* arg) {
    Value result = {0};
    if (arg->type == TYPE_FLOAT) {
        result.type = TYPE_FLOAT;
        result.float_val = ceil(arg->float_val);
    }
    return result;
}

static Value eval_math_function(const Expr* expr, const Row* row, const TableDef* schema) {
    Value arg = {0};
    if (expr->scalar.arg_count > 0) {
        arg = eval_select_expression(expr->scalar.args[0], row, schema);
    }
    if (arg.type == TYPE_NULL) {
        Value null_result = {0};
        null_result.type = TYPE_NULL;
        return null_result;
    }

    switch (expr->scalar.func_type) {
        case FUNC_ABS:   return eval_math_func_abs(&arg);
        case FUNC_SQRT:  return eval_math_func_sqrt(&arg);
        case FUNC_MOD:   return eval_math_func_mod(&arg);
        case FUNC_POW:   return eval_math_func_pow(&arg);
        case FUNC_ROUND: return eval_math_func_round(&arg);
        case FUNC_FLOOR: return eval_math_func_floor(&arg);
        case FUNC_CEIL:  return eval_math_func_ceil(&arg);
        default: break;
    }
    Value err = {0};
    err.type = TYPE_ERROR;
    return err;
}

Value eval_scalar_function(const Expr* expr, const Row* row, const TableDef* schema) {
    Value result = eval_math_function(expr, row, schema);
    if (result.type != TYPE_ERROR) {
        return result;
    }
    return eval_string_function(expr, row, schema);
}
