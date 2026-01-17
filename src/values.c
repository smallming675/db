#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "db.h"
#include "logger.h"

const Value VAL_NULL = {.type = TYPE_NULL};
const Value VAL_ERROR = {.type = TYPE_ERROR};

int time_hour(unsigned int time_val) { return (time_val >> 12) & 0xFF; }
int time_minute(unsigned int time_val) { return (time_val >> 6) & 0x3F; }
int time_second(unsigned int time_val) { return time_val & 0x3F; }
int date_year(unsigned int date_val) { return (date_val >> 9) & 0x3FFFFF; }
int date_month(unsigned int date_val) { return (date_val >> 5) & 0xF; }
int date_day(unsigned int date_val) { return date_val & 0x1F; }

static Value convert_value_impl(const Value* val, DataType target_type, bool* success) {
    Value result;
    *success = true;

    if (val->type == TYPE_NULL) {
        result.type = target_type;
        return result;
    }

    if (val->type == target_type) {
        return *val;
    }

    switch (target_type) {
        case TYPE_INT:
            if (val->type == TYPE_FLOAT) {
                result.type = TYPE_INT;
                result.int_val = (int)val->float_val;
            } else if (val->type == TYPE_STRING) {
                result.type = TYPE_INT;
                result.int_val = atoi(val->char_val);
            } else {
                *success = false;
                result.type = TYPE_ERROR;
            }
            break;

        case TYPE_FLOAT:
            if (val->type == TYPE_INT) {
                result.type = TYPE_FLOAT;
                result.float_val = (double)val->int_val;
            } else if (val->type == TYPE_STRING) {
                result.type = TYPE_FLOAT;
                result.float_val = atof(val->char_val);
            } else {
                *success = false;
                result.type = TYPE_ERROR;
            }
            break;

        case TYPE_STRING:
            result.type = TYPE_STRING;
            switch (val->type) {
                case TYPE_INT:
                    result.char_val = malloc(32);
                    snprintf(result.char_val, 32, "%d", val->int_val);
                    break;
                case TYPE_FLOAT:
                    result.char_val = malloc(64);
                    snprintf(result.char_val, 64, "%.2f", val->float_val);
                    break;
                case TYPE_STRING:
                    result.char_val = malloc(strlen(val->char_val) + 1);
                    strcpy(result.char_val, val->char_val);
                    break;
                default:
                    *success = false;
                    result.type = TYPE_ERROR;
            }
            break;

        default:
            *success = false;
            result.type = TYPE_ERROR;
            break;
    }

    return result;
}

Value convert_value(const Value* val, DataType target_type) {
    bool success;
    return convert_value_impl(val, target_type, &success);
}

bool try_convert_value(const Value* val, DataType target_type, Value* out_result) {
    bool success;
    *out_result = convert_value_impl(val, target_type, &success);
    return success;
}

bool is_null(const Value* val) {
    if (!val) return true;
    if (val->type == TYPE_ERROR) {
        log_msg(LOG_ERROR, "Error value located, exiting...");
        exit(1);
    }
    return val->type == TYPE_NULL;
}



const char* repr(const Value* val) {
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
            snprintf(buffer, MAX_STRING_LEN, "%.2f", val->float_val);
            break;
        case TYPE_STRING:
            strncpy(buffer, val->char_val, MAX_STRING_LEN - 1);
            buffer[MAX_STRING_LEN - 1] = '\0';
            break;
        case TYPE_TIME:
            snprintf(buffer, MAX_STRING_LEN, "%02d:%02d:%02d",
                     time_hour(val->time_val.time_val), time_minute(val->time_val.time_val), time_second(val->time_val.time_val));
            break;
        case TYPE_DATE:
            snprintf(buffer, MAX_STRING_LEN, "%04d-%02d-%02d",
                     date_year(val->date_val.date_val), date_month(val->date_val.date_val), date_day(val->date_val.date_val));
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

/* returns -1/0/1 for ordering */
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

/* LIKE pattern matching */
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

bool eval_comparison(Value left, Value right, OperatorType op) {
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



void update_aggregate_state(AggState* state, const Value* val,
                                    const IRAggregate* agg) {
    if (is_null(val)) {
        return;  // Skip NULL values
    }

    if (agg->distinct) {
        int seen_count = alist_length(&state->seen_values);
        for (int i = 0; i < seen_count; i++) {
            char* seen = (char*)alist_get(&state->seen_values, i);
            if (seen && compare_values(val, (Value*)seen) == 0) {
                return;  // Skip duplicate
            }
        }
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
            char** appended = (char**)alist_append(&state->seen_values);
            if (appended) {
                *appended = val_copy;
            } else {
                free(val_copy);
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

Value compute_aggregate(AggFuncType func_type, AggState* state,
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
