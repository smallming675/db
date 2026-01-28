#include "values.h"

#include <limits.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "db.h"
#include "logger.h"
#include "utils.h"
#include "table.h"

const Value VAL_NULL = {.type = TYPE_NULL};
const Value VAL_ERROR = {.type = TYPE_ERROR};

int time_hour(unsigned int time_val) {
    return (time_val >> 12) & 0xFF;
}
int time_minute(unsigned int time_val) {
    return (time_val >> 6) & 0x3F;
}
int time_second(unsigned int time_val) {
    return time_val & 0x3F;
}
int date_year(unsigned int date_val) {
    return (date_val >> 9) & 0x3FFFFF;
}
int date_month(unsigned int date_val) {
    return (date_val >> 5) & 0xF;
}
int date_day(unsigned int date_val) {
    return date_val & 0x1F;
}

bool is_null(const Value *val) {
    if (!val)
        return true;
    if (val->type == TYPE_ERROR) {
        log_msg(LOG_ERROR, "Error value located, exiting...");
        exit(1);
    }
    return val->type == TYPE_NULL;
}

const char *repr(const Value *val) {
    static char buffer[MAX_STRING_LEN];

    if (!val || is_null(val)) {
        strcopy(buffer, sizeof(buffer), "NULL");
        return buffer;
    }

    switch (val->type) {
    case TYPE_INT:
        string_format(buffer, sizeof(buffer), "%lld", (long long)val->int_val);
        break;
    case TYPE_FLOAT:
        string_format(buffer, sizeof(buffer), "%.2f", val->float_val);
        break;
    case TYPE_BOOLEAN:
        strcopy(buffer, sizeof(buffer), val->bool_val ? "TRUE" : "FALSE");
        break;
    case TYPE_DECIMAL: {
        int scale = val->decimal_val.scale > 0 ? val->decimal_val.scale : 2;
        double d = (double)val->decimal_val.value / (double)pow(10, scale);
        string_format(buffer, sizeof(buffer), "%.2f", d);
        break;
    }
    case TYPE_BLOB:
        string_format(buffer, sizeof(buffer), "<BLOB:%zu bytes>", val->blob_val.length);
        break;
    case TYPE_STRING:
        strcopy(buffer, sizeof(buffer), val->char_val);
        break;
    case TYPE_TIME:
        string_format(buffer, sizeof(buffer), "%02d:%02d:%02d", time_hour(val->time_val),
                      time_minute(val->time_val), time_second(val->time_val));
        break;
    case TYPE_DATE:
        string_format(buffer, sizeof(buffer), "%04d-%02d-%02d", date_year(val->date_val),
                      date_month(val->date_val), date_day(val->date_val));
        break;
    case TYPE_ERROR:
        strcopy(buffer, sizeof(buffer), "ERROR");
        break;
    default:
        strcopy(buffer, sizeof(buffer), "UNKNOWN");
        break;
    }

    return buffer;
}

static int compare_numeric(double left_val, double right_val) {
    if (left_val < right_val)
        return -1;
    if (left_val > right_val)
        return 1;
    return 0;
}

/* returns -1/0/1 for ordering */
static int compare_values_local(const Value *left, const Value *right) {
    if (is_null(left) || is_null(right))
        return 0;

    if (left->type == TYPE_INT && right->type == TYPE_INT) {
        if (left->int_val < right->int_val)
            return -1;
        if (left->int_val > right->int_val)
            return 1;
        return 0;
    }

    bool left_is_numeric = (left->type == TYPE_FLOAT || left->type == TYPE_DECIMAL);
    bool right_is_numeric = (right->type == TYPE_FLOAT || right->type == TYPE_DECIMAL);

    if (left_is_numeric && right_is_numeric) {
        return compare_numeric((double)left->float_val, (double)right->float_val);
    }

    if (left->type == TYPE_INT && right_is_numeric) {
        double right_val = (double)right->float_val;
        if (left->int_val < right_val)
            return -1;
        if (left->int_val > right_val)
            return 1;
        return 0;
    }

    if (left_is_numeric && right->type == TYPE_INT) {
        double left_val = (double)left->float_val;
        if (left_val < right->int_val)
            return -1;
        if (left_val > right->int_val)
            return 1;
        return 0;
    }

    if (left->type == TYPE_BOOLEAN && right->type == TYPE_BOOLEAN) {
        return left->bool_val < right->bool_val ? -1 : (left->bool_val > right->bool_val ? 1 : 0);
    }

    if (left->type == TYPE_DECIMAL && right->type == TYPE_DECIMAL) {
        if (left->decimal_val.scale == right->decimal_val.scale) {
            return left->decimal_val.value < right->decimal_val.value
                       ? -1
                       : (left->decimal_val.value > right->decimal_val.value ? 1 : 0);
        }
        int scale_diff = left->decimal_val.scale - right->decimal_val.scale;
        long long left_val =
            left->decimal_val.value * (long long)pow(10, scale_diff > 0 ? 0 : -scale_diff);
        long long right_val =
            right->decimal_val.value * (long long)pow(10, scale_diff > 0 ? -scale_diff : 0);
        return left_val < right_val ? -1 : (left_val > right_val ? 1 : 0);
    }

    if (left->type == TYPE_STRING && right->type == TYPE_STRING) {
        return strcmp(left->char_val, right->char_val);
    }

    /* Dates and time are stored as integers, where later datetimes are always larger
       thus it can be compared directly */
    if (left->type == TYPE_DATE && right->type == TYPE_DATE) {
        return left->date_val < right->date_val ? -1 : (left->date_val > right->date_val ? 1 : 0);
    }

    if (left->type == TYPE_TIME && right->type == TYPE_TIME) {
        return left->time_val < right->time_val ? -1 : (left->time_val > right->time_val ? 1 : 0);
    }

    if (left->type == TYPE_BLOB && right->type == TYPE_BLOB) {
        if (left->blob_val.length != right->blob_val.length) {
            return left->blob_val.length < right->blob_val.length ? -1 : 1;
        }
        int cmp = memcmp(left->blob_val.data, right->blob_val.data, left->blob_val.length);
        return cmp < 0 ? -1 : (cmp > 0 ? 1 : 0);
    }

    return 0;
}

/* LIKE pattern matching */
static bool eval_like_expression(const Value *left, const Value *right) {
    if (right->type != TYPE_STRING) {
        log_msg(LOG_ERROR, "Right hand side of LIKE expression (expected: %s, got: %s)",
                TYPE_STRING, right->type);
        return false;
    }

    if (is_null(left) || is_null(right))
        return false;

    const char *text = repr(left);
    const char *pattern = right->char_val;
    const char *p = pattern;
    bool match = true;

    while (*p && *text) {
        if (*p == '%' || *p == '*') {
            p++;
            while (*text && *text != *p)
                text++;
        } else if (*p == '_' || *p == '?') {
            p++;
            text++;
        } else if (*p == '\\') {
            p++;
            text++;
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

    while (match && (*p == '%' || *p == '*')) {
        p++;
    }

    match = match && (*p == '\0' && *text == '\0');
    return match;
}

bool eval_comparison(Value left, Value right, OperatorType op) {
    int cmp = compare_values_local(&left, &right);
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

Value compute_aggregate(AggFuncType func_type, AggState *state, DataType return_type) {
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
        if (state->type != AGG_MIN || !state->data.min.has_min) {
            return VAL_NULL;
        } else {
            return state->data.min.min_val;
        }
    } else if (func_type == FUNC_MAX) {
        if (state->type != AGG_MAX || !state->data.max.has_max) {
            return VAL_NULL;
        } else {
            return state->data.max.max_val;
        }
    }

    return VAL_ERROR;
}

void agg_init(AggState *state, AggFuncType func_type, bool distinct) {
    state->sum = 0.0;
    state->count = 0;

    if (distinct) {
        state->type = AGG_DISTINCT;
        alist_init(&state->data.distinct.seen_values, sizeof(Value), NULL);
        state->data.distinct.distinct_count = 0;
    } else if (func_type == FUNC_MIN) {
        state->type = AGG_MIN;
        state->data.min.has_min = false;
    } else if (func_type == FUNC_MAX) {
        state->type = AGG_MAX;
        state->data.max.has_max = false;
    } else {
        state->type = AGG_PLAIN;
    }
}

void agg_add_value(AggState *state, Value *value) {
    if (is_null(value))
        return;

    if (state->type == AGG_DISTINCT) {
        // Check if value is already in distinct set
        for (int i = 0; i < state->data.distinct.seen_values.length; i++) {
            Value *existing = alist_get(&state->data.distinct.seen_values, i);
            if (compare_values(existing, value) == 0) {
                return; // Already seen
            }
        }
        Value *slot = alist_append(&state->data.distinct.seen_values);
        *slot = copy_value(value);
        state->data.distinct.distinct_count++;
    }

    state->count++;
    if (value->type == TYPE_INT) {
        state->sum += value->int_val;
    } else if (value->type == TYPE_FLOAT) {
        state->sum += value->float_val;
    }

    if (state->type == AGG_MIN && !state->data.min.has_min) {
        state->data.min.min_val = copy_value(value);
        state->data.min.has_min = true;
    } else if (state->type == AGG_MAX && !state->data.max.has_max) {
        state->data.max.max_val = copy_value(value);
        state->data.max.has_max = true;
    }
}

Value agg_get_result(AggState *state) {
    Value result = {0};

    if (state->type == AGG_DISTINCT) {
        result.type = TYPE_INT;
        result.int_val = state->data.distinct.distinct_count;
    } else if (state->type == AGG_MIN) {
        if (state->data.min.has_min) {
            return copy_value(&state->data.min.min_val);
        }
        result.type = TYPE_NULL;
    } else if (state->type == AGG_MAX) {
        if (state->data.max.has_max) {
            return copy_value(&state->data.max.max_val);
        }
        result.type = TYPE_NULL;
    } else {
        // Plain aggregate
        result.type = TYPE_FLOAT;
        if (state->count > 0) {
            result.float_val = state->sum;
        } else {
            result.type = TYPE_NULL;
        }
    }

    return result;
}

void agg_cleanup(AggState *state) {
    if (state->type == AGG_DISTINCT) {
        for (int i = 0; i < state->data.distinct.seen_values.length; i++) {
            Value *val = alist_get(&state->data.distinct.seen_values, i);
            free_value(val);
        }
        alist_destroy(&state->data.distinct.seen_values);
    } else if (state->type == AGG_MIN && state->data.min.has_min) {
        free_value(&state->data.min.min_val);
    } else if (state->type == AGG_MAX && state->data.max.has_max) {
        free_value(&state->data.max.max_val);
    }
}
