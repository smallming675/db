#include <ctype.h>
#include <limits.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "arraylist.h"
#include "db.h"
#include "logger.h"
#include "table.h"
#include "values.h"

static QueryResult* g_last_result = NULL;
static ArrayList g_aggregate_results;
static bool g_in_aggregate_context = false;

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

static Value get_column_value_by_index(const Row* row, const TableDef* schema, int col_idx);
static Value get_column_value(const Row* row, const TableDef* schema, const char* column_name);
static Value get_column_value_from_join(const Row* row, const TableDef* left_schema, 
                                         const TableDef* right_schema, int left_col_count,
                                         const char* column_name);
static bool eval_expression(const Expr* expr, const Row* row, const TableDef* schema);
static bool eval_expression_for_join(const Expr* expr, const Row* row, 
                                      const TableDef* left_schema, 
                                      const TableDef* right_schema,
                                      int left_col_count);
static Value eval_select_expression(Expr* expr, const Row* row, const TableDef* schema);
static Value eval_scalar_function(const Expr* expr, const Row* row, const TableDef* schema);
static void exec_create_table_ast(ASTNode* ast);
static void exec_insert_row_ast(ASTNode* ast);
static uint16_t exec_join_ast(ASTNode* ast);
static void exec_filter_ast(ASTNode* ast, uint8_t table_id);
static void exec_aggregate_ast(ASTNode* ast, uint8_t table_id);
static void exec_project_ast(ASTNode* ast, uint8_t table_id);
static void exec_drop_table_ast(ASTNode* ast);
static void exec_update_row_ast(ASTNode* ast);
static void exec_delete_row_ast(ASTNode* ast);
static void exec_create_index_ast(ASTNode* ast);
static void exec_drop_index_ast(ASTNode* ast);

static Value get_column_value_by_index(const Row* row, const TableDef* schema, int col_idx) {
    (void)schema;
    Value val;
    val.type = TYPE_NULL;
    val.char_val = NULL;

    if (col_idx >= 0 && col_idx < row->value_count) {
        val = row->values[col_idx];
    }
    return val;
}

static Value get_column_value(const Row* row, const TableDef* schema, const char* column_name) {
    Value val;
    val.type = TYPE_NULL;
    val.char_val = NULL;

    int col_count = alist_length(&schema->columns);
    for (int i = 0; i < col_count; i++) {
        ColumnDef* col = (ColumnDef*)alist_get(&schema->columns, i);
        if (col && strcmp(col->name, column_name) == 0) {
            return get_column_value_by_index(row, schema, i);
        }
    }
    log_msg(LOG_WARN, "get_column_value: Column '%s' not found in schema", column_name);
    return val;
}

static Value get_column_value_from_join(const Row* row, const TableDef* left_schema, 
                                         const TableDef* right_schema, int left_col_count,
                                         const char* column_name) {
    Value val;
    val.type = TYPE_NULL;
    val.char_val = NULL;

    int left_col_idx = -1;
    for (int i = 0; i < alist_length(&left_schema->columns); i++) {
        ColumnDef* col = (ColumnDef*)alist_get(&left_schema->columns, i);
        if (col && strcmp(col->name, column_name) == 0) {
            left_col_idx = i;
            break;
        }
    }

    if (left_col_idx >= 0 && left_col_idx < left_col_count) {
        return get_column_value_by_index(row, left_schema, left_col_idx);
    }

    int right_col_idx = -1;
    for (int i = 0; i < alist_length(&right_schema->columns); i++) {
        ColumnDef* col = (ColumnDef*)alist_get(&right_schema->columns, i);
        if (col && strcmp(col->name, column_name) == 0) {
            right_col_idx = i;
            break;
        }
    }

    if (right_col_idx >= 0) {
        int actual_idx = left_col_count + right_col_idx;
        return get_column_value_by_index(row, NULL, actual_idx);
    }

    return val;
}

static bool eval_expression(const Expr* expr, const Row* row, const TableDef* schema) {
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
                case OP_GREATER_EQUAL: {
                    Expr* left = expr->binary.left;
                    Expr* right = expr->binary.right;
                    if (left->type == EXPR_COLUMN && right->type == EXPR_VALUE) {
                        return eval_comparison(get_column_value(row, schema, left->column_name), right->value, expr->binary.op);
                    } else if (left->type == EXPR_VALUE && right->type == EXPR_COLUMN) {
                        return eval_comparison(left->value, get_column_value(row, schema, right->column_name), expr->binary.op);
                    } else if (left->type == EXPR_COLUMN && right->type == EXPR_COLUMN) {
                        return eval_comparison(get_column_value(row, schema, left->column_name), get_column_value(row, schema, right->column_name), expr->binary.op);
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

static bool eval_expression_for_join(const Expr* expr, const Row* row, 
                                      const TableDef* left_schema, 
                                      const TableDef* right_schema,
                                      int left_col_count) {
    if (!expr) return true;

    switch (expr->type) {
        case EXPR_COLUMN: {
            Value val = get_column_value_from_join(row, left_schema, right_schema, left_col_count, expr->column_name);
            return !is_null(&val);
        }
        case EXPR_VALUE:
            return !is_null(&expr->value);
        case EXPR_BINARY_OP: {
            bool left_val = eval_expression_for_join(expr->binary.left, row, left_schema, right_schema, left_col_count);
            bool right_val = eval_expression_for_join(expr->binary.right, row, left_schema, right_schema, left_col_count);

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
                    Expr* left = expr->binary.left;
                    Expr* right = expr->binary.right;
                    if (left->type == EXPR_COLUMN && right->type == EXPR_VALUE) {
                        return eval_comparison(get_column_value_from_join(row, left_schema, right_schema, left_col_count, left->column_name), right->value, expr->binary.op);
                    } else if (left->type == EXPR_VALUE && right->type == EXPR_COLUMN) {
                        return eval_comparison(left->value, get_column_value_from_join(row, left_schema, right_schema, left_col_count, right->column_name), expr->binary.op);
                    } else if (left->type == EXPR_COLUMN && right->type == EXPR_COLUMN) {
                        return eval_comparison(get_column_value_from_join(row, left_schema, right_schema, left_col_count, left->column_name), get_column_value_from_join(row, left_schema, right_schema, left_col_count, right->column_name), expr->binary.op);
                    }
                    return false;
                }
                default:
                    return false;
            }
        }
        case EXPR_UNARY_OP: {
            bool operand_val = eval_expression_for_join(expr->unary.operand, row, left_schema, right_schema, left_col_count);
            if (expr->unary.op == OP_NOT) {
                return !operand_val;
            }
            return false;
        }
        default:
            return false;
    }
}

static Value copy_string_value(const Value* src) {
    Value copy = *src;
    if (src->type == TYPE_STRING && src->char_val) {
        copy.char_val = malloc(strlen(src->char_val) + 1);
        strcpy(copy.char_val, src->char_val);
    }
    return copy;
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

    Value result = {0};
    result.type = TYPE_ERROR;
    result.char_val = NULL;

    switch (expr->scalar.func_type) {
        case FUNC_UPPER: {
            result.type = TYPE_STRING;
            if (arg1.type == TYPE_STRING && arg1.char_val) {
                result.char_val = malloc(strlen(arg1.char_val) + 1);
                for (size_t i = 0; i <= strlen(arg1.char_val); i++) {
                    result.char_val[i] = toupper((unsigned char)arg1.char_val[i]);
                }
            } else {
                result.char_val = malloc(1);
                result.char_val[0] = '\0';
            }
            break;
        }
        case FUNC_LOWER: {
            result.type = TYPE_STRING;
            if (arg1.type == TYPE_STRING && arg1.char_val) {
                result.char_val = malloc(strlen(arg1.char_val) + 1);
                for (size_t i = 0; i <= strlen(arg1.char_val); i++) {
                    result.char_val[i] = tolower((unsigned char)arg1.char_val[i]);
                }
            } else {
                result.char_val = malloc(1);
                result.char_val[0] = '\0';
            }
            break;
        }
        case FUNC_LEN: {
            result.type = TYPE_INT;
            result.int_val = (arg1.type == TYPE_STRING && arg1.char_val) ? strlen(arg1.char_val) : 0;
            break;
        }
        case FUNC_MID: {
            result.type = TYPE_STRING;
            if (arg1.type == TYPE_STRING && arg1.char_val && expr->scalar.arg_count >= 3) {
                Value arg2 = eval_select_expression(expr->scalar.args[1], row, schema);
                Value arg3 = eval_select_expression(expr->scalar.args[2], row, schema);
                int start = arg2.type == TYPE_INT ? arg2.int_val : 0;
                int len = arg3.type == TYPE_INT ? arg3.int_val : 0;
                int src_len = strlen(arg1.char_val);
                if (start < src_len) {
                    int copy_len = (start + len > src_len) ? (src_len - start) : len;
                    result.char_val = malloc(copy_len + 1);
                    memcpy(result.char_val, arg1.char_val + start, copy_len);
                    result.char_val[copy_len] = '\0';
                } else {
                    result.char_val = malloc(1);
                    result.char_val[0] = '\0';
                }
            } else {
                result.char_val = malloc(1);
                result.char_val[0] = '\0';
            }
            break;
        }
        case FUNC_LEFT: {
            result.type = TYPE_STRING;
            if (arg1.type == TYPE_STRING && arg1.char_val && expr->scalar.arg_count >= 2) {
                Value arg2 = eval_select_expression(expr->scalar.args[1], row, schema);
                int len = arg2.type == TYPE_INT ? arg2.int_val : 0;
                int src_len = strlen(arg1.char_val);
                int copy_len = len < src_len ? len : src_len;
                result.char_val = malloc(copy_len + 1);
                memcpy(result.char_val, arg1.char_val, copy_len);
                result.char_val[copy_len] = '\0';
            } else {
                result.char_val = malloc(1);
                result.char_val[0] = '\0';
            }
            break;
        }
        case FUNC_RIGHT: {
            result.type = TYPE_STRING;
            if (arg1.type == TYPE_STRING && arg1.char_val && expr->scalar.arg_count >= 2) {
                Value arg2 = eval_select_expression(expr->scalar.args[1], row, schema);
                int len = arg2.type == TYPE_INT ? arg2.int_val : 0;
                int src_len = strlen(arg1.char_val);
                int start = src_len - len;
                if (start < 0) start = 0;
                int copy_len = src_len - start;
                result.char_val = malloc(copy_len + 1);
                memcpy(result.char_val, arg1.char_val + start, copy_len);
                result.char_val[copy_len] = '\0';
            } else {
                result.char_val = malloc(1);
                result.char_val[0] = '\0';
            }
            break;
        }
        case FUNC_CONCAT: {
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
            break;
        }
        default:
            break;
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

    Value result = {0};
    result.type = TYPE_ERROR;
    result.char_val = NULL;

    switch (expr->scalar.func_type) {
        case FUNC_ABS:
            if (arg.type == TYPE_INT) {
                result.type = TYPE_INT;
                result.int_val = abs(arg.int_val);
            } else if (arg.type == TYPE_FLOAT) {
                result.type = TYPE_FLOAT;
                result.float_val = fabs(arg.float_val);
            }
            break;
        case FUNC_SQRT:
            if (arg.type == TYPE_INT || arg.type == TYPE_FLOAT) {
                result.type = TYPE_FLOAT;
                result.float_val = sqrt(arg.type == TYPE_INT ? (double)arg.int_val : arg.float_val);
            }
            break;
        case FUNC_MOD:
            if (arg.type == TYPE_INT) {
                result.type = TYPE_INT;
                result.int_val = arg.int_val % 10;
            }
            break;
        case FUNC_POW:
            if (arg.type == TYPE_INT || arg.type == TYPE_FLOAT) {
                result.type = TYPE_FLOAT;
                result.float_val = pow(arg.type == TYPE_INT ? (double)arg.int_val : arg.float_val, 2.0);
            }
            break;
        case FUNC_ROUND:
            if (arg.type == TYPE_FLOAT) {
                result.type = TYPE_FLOAT;
                result.float_val = round(arg.float_val);
            }
            break;
        case FUNC_FLOOR:
            if (arg.type == TYPE_FLOAT) {
                result.type = TYPE_FLOAT;
                result.float_val = floor(arg.float_val);
            }
            break;
        case FUNC_CEIL:
            if (arg.type == TYPE_FLOAT) {
                result.type = TYPE_FLOAT;
                result.float_val = ceil(arg.float_val);
            }
            break;
        default:
            break;
    }

    return result;
}

static Value eval_scalar_function(const Expr* expr, const Row* row, const TableDef* schema) {
    Value result = eval_math_function(expr, row, schema);
    if (result.type != TYPE_ERROR) {
        return result;
    }

    return eval_string_function(expr, row, schema);
}

static Value eval_select_expression(Expr* expr, const Row* row, const TableDef* schema) {
    Value result = {0};
    result.type = TYPE_NULL;
    result.char_val = NULL;

    if (!expr) return result;

    switch (expr->type) {
        case EXPR_COLUMN:
            return get_column_value(row, schema, expr->column_name);
        case EXPR_VALUE:
            return copy_string_value(&expr->value);
        case EXPR_BINARY_OP: {
            Value left = eval_select_expression(expr->binary.left, row, schema);
            Value right = eval_select_expression(expr->binary.right, row, schema);
            bool cmp_result = eval_comparison(left, right, expr->binary.op);

            switch (expr->binary.op) {
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
                case OP_EQUALS:
                case OP_NOT_EQUALS:
                case OP_LESS:
                case OP_LESS_EQUAL:
                case OP_GREATER:
                case OP_GREATER_EQUAL:
                case OP_LIKE:
                case OP_AND:
                case OP_OR:
                    result.type = TYPE_INT;
                    result.int_val = cmp_result ? 1 : 0;
                    break;
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
            log_msg(LOG_ERROR, "eval_select_expression: Cannot evaluate aggregate in non-aggregate context");
            break;
        case EXPR_SCALAR_FUNC:
            return eval_scalar_function(expr, row, schema);
        case EXPR_SUBQUERY:
            log_msg(LOG_ERROR, "eval_select_expression: Subquery evaluation NOT IMPLEMENTED");
            break;
        default:
            log_msg(LOG_WARN, "eval_select_expression: Unknown expression type %d", expr->type);
    }

    return result;
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

static void exec_insert_row_ast(ASTNode* ast) {
    InsertNode* ins = &ast->insert;
    Table* table = get_table_by_id(ins->table_id);
    if (!table) {
        log_msg(LOG_ERROR, "Table with ID %d not found", ins->table_id);
        return;
    }

    int value_count = alist_length(&ins->values);
    if (value_count == 0) return;

    Row* row = (Row*)alist_append(&table->rows);
    if (!row) return;

    row->value_count = value_count;
    row->values = malloc(value_count * sizeof(Value));

    for (int i = 0; i < value_count; i++) {
        ColumnValue* cv = (ColumnValue*)alist_get(&ins->values, i);
        if (cv) {
            row->values[i] = copy_string_value(&cv->value);
        }
    }

    log_msg(LOG_INFO, "Inserted 1 row into table '%s'", table->name);
}

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

    snprintf(result_table->name, MAX_TABLE_NAME_LEN, "_join_%d_%d", select->table_id, select->join_table_id);
    result_table->table_id = result_id;
    alist_init(&result_table->rows, sizeof(Row), free_row_contents);
    alist_init(&result_table->schema.columns, sizeof(ColumnDef), NULL);
    result_table->schema.strict = false;

    int left_cols = alist_length(&left_table->schema.columns);
    int right_cols = alist_length(&right_table->schema.columns);

    for (int i = 0; i < left_cols; i++) {
        ColumnDef* col = (ColumnDef*)alist_get(&left_table->schema.columns, i);
        if (col) {
            ColumnDef* new_col = (ColumnDef*)alist_append(&result_table->schema.columns);
            if (new_col) *new_col = *col;
        }
    }
    for (int i = 0; i < right_cols; i++) {
        ColumnDef* col = (ColumnDef*)alist_get(&right_table->schema.columns, i);
        if (col) {
            ColumnDef* new_col = (ColumnDef*)alist_append(&result_table->schema.columns);
            if (new_col) *new_col = *col;
        }
    }

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
                match = eval_expression_for_join(select->join_condition, left_row,
                                                  &left_table->schema, &right_table->schema, left_cols);
            }

            if (match) {
                had_match = true;
                Row* new_row = (Row*)alist_append(&result_table->rows);
                if (new_row) {
                    new_row->value_count = left_row->value_count + right_row->value_count;
                    new_row->values = malloc(new_row->value_count * sizeof(Value));
                    memcpy(new_row->values, left_row->values, left_row->value_count * sizeof(Value));
                    memcpy(new_row->values + left_row->value_count, right_row->values,
                           right_row->value_count * sizeof(Value));
                }
            }
        }

        if (!had_match && select->join_type == JOIN_LEFT) {
            Row* new_row = (Row*)alist_append(&result_table->rows);
            if (new_row) {
                new_row->value_count = left_row->value_count + right_cols;
                new_row->values = malloc(new_row->value_count * sizeof(Value));
                memcpy(new_row->values, left_row->values, left_row->value_count * sizeof(Value));
                for (int k = 0; k < right_cols; k++) {
                    new_row->values[left_row->value_count + k].type = TYPE_NULL;
                }
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
            case FUNC_COUNT: {
                if (count_all) {
                    result.int_val = loop_count;
                } else if (operand && operand->type == EXPR_COLUMN) {
                    int null_count = 0;
                    for (int idx = 0; idx < loop_count; idx++) {
                        int i = select->where_clause ? *(int*)alist_get(&filtered_indices, idx) : idx;
                        Row* row = (Row*)alist_get(&table->rows, i);
                        if (row) {
                            Value val = get_column_value(row, &table->schema, operand->column_name);
                            if (is_null(&val)) null_count++;
                        }
                    }
                    result.int_val = loop_count - null_count;
                } else {
                    result.int_val = loop_count;
                }
                result.type = TYPE_INT;
                break;
            }
            case FUNC_SUM: {
                double sum = 0.0;
                for (int idx = 0; idx < loop_count; idx++) {
                    int i = select->where_clause ? *(int*)alist_get(&filtered_indices, idx) : idx;
                    Row* row = (Row*)alist_get(&table->rows, i);
                    if (row && operand && operand->type == EXPR_COLUMN) {
                        Value val = get_column_value(row, &table->schema, operand->column_name);
                        if (val.type == TYPE_INT) sum += val.int_val;
                        else if (val.type == TYPE_FLOAT) sum += val.float_val;
                    }
                }
                result.float_val = sum;
                break;
            }
            case FUNC_AVG: {
                double sum = 0.0;
                int count = 0;
                for (int idx = 0; idx < loop_count; idx++) {
                    int i = select->where_clause ? *(int*)alist_get(&filtered_indices, idx) : idx;
                    Row* row = (Row*)alist_get(&table->rows, i);
                    if (row && operand && operand->type == EXPR_COLUMN) {
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
                result.float_val = count > 0 ? sum / count : 0;
                break;
            }
            case FUNC_MIN: {
                double min_val = INFINITY;
                for (int idx = 0; idx < loop_count; idx++) {
                    int i = select->where_clause ? *(int*)alist_get(&filtered_indices, idx) : idx;
                    Row* row = (Row*)alist_get(&table->rows, i);
                    if (row && operand && operand->type == EXPR_COLUMN) {
                        Value val = get_column_value(row, &table->schema, operand->column_name);
                        if (val.type == TYPE_INT && val.int_val < min_val) min_val = val.int_val;
                        else if (val.type == TYPE_FLOAT && val.float_val < min_val) min_val = val.float_val;
                    }
                }
                result.float_val = min_val == INFINITY ? 0 : min_val;
                break;
            }
            case FUNC_MAX: {
                double max_val = -INFINITY;
                for (int idx = 0; idx < loop_count; idx++) {
                    int i = select->where_clause ? *(int*)alist_get(&filtered_indices, idx) : idx;
                    Row* row = (Row*)alist_get(&table->rows, i);
                    if (row && operand && operand->type == EXPR_COLUMN) {
                        Value val = get_column_value(row, &table->schema, operand->column_name);
                        if (val.type == TYPE_INT && val.int_val > max_val) max_val = val.int_val;
                        else if (val.type == TYPE_FLOAT && val.float_val > max_val) max_val = val.float_val;
                    }
                }
                result.float_val = max_val == -INFINITY ? 0 : max_val;
                break;
            }
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

static void print_result_row(const Value* values, int col_count) {
    for (int j = 0; j < col_count; j++) {
        Value* val = (Value*)&values[j];
        printf("%-20s", val->type == TYPE_NULL ? "NULL" : repr(val));
    }
    printf("\n");
}

static void print_column_headers(SelectNode* select, Table* table, bool is_select_star, int col_count) {
    if (is_select_star) {
        for (int j = 0; j < col_count; j++) {
            ColumnDef* col = (ColumnDef*)alist_get(&table->schema.columns, j);
            printf("%-20s", col ? col->name : "unknown");
        }
    } else {
        for (int i = 0; i < col_count; i++) {
            Expr** expr = (Expr**)alist_get(&select->expressions, i);
            const char* alias = expr[0]->alias;
            if (alias[0]) {
                printf("%-20s", alias);
            } else if (expr[0]->type == EXPR_COLUMN) {
                printf("%-20s", expr[0]->column_name);
            } else {
                printf("%-20s", "expr");
            }
        }
    }
    printf("\n");
}

static void exec_project_ast(ASTNode* ast, uint8_t table_id) {
    SelectNode* select = &ast->select;
    Table* table = get_table_by_id(table_id);
    if (!table) return;

    int expr_count = alist_length(&select->expressions);
    if (expr_count == 0) return;

    Expr** first_expr = (Expr**)alist_get(&select->expressions, 0);
    bool is_select_star = (first_expr && first_expr[0]->type == EXPR_VALUE &&
                           first_expr[0]->value.char_val &&
                           strcmp(first_expr[0]->value.char_val, "*") == 0);

    int col_count = is_select_star ? alist_length(&table->schema.columns) : expr_count;
    if (col_count <= 0) return;

    free_query_result(g_last_result);
    g_last_result = NULL;

    g_last_result = malloc(sizeof(QueryResult));
    if (!g_last_result) return;

    memset(g_last_result, 0, sizeof(QueryResult));
    g_last_result->col_count = col_count;
    g_last_result->column_names = malloc(col_count * sizeof(char*));
    for (int i = 0; i < col_count; i++) {
        if (is_select_star) {
            ColumnDef* col = (ColumnDef*)alist_get(&table->schema.columns, i);
            const char* name = col ? col->name : "unknown";
            g_last_result->column_names[i] = malloc(strlen(name) + 1);
            strcpy(g_last_result->column_names[i], name);
        } else {
            Expr** expr = (Expr**)alist_get(&select->expressions, i);
            const char* alias = expr[0]->alias;
            const char* name = alias[0] ? alias :
                (expr[0]->type == EXPR_COLUMN ? expr[0]->column_name : "expr");
            g_last_result->column_names[i] = malloc(strlen(name) + 1);
            strcpy(g_last_result->column_names[i], name);
        }
    }

    int capacity = 16;
    g_last_result->values = malloc(capacity * sizeof(Value));
    g_last_result->rows = malloc(capacity * sizeof(int));
    g_last_result->row_count = 0;

    if (g_in_aggregate_context) {
        int agg_count = alist_length(&g_aggregate_results);
        int limit = select->limit > 0 ? select->limit : 1;

        for (int i = 0; i < 1 && i < agg_count && g_last_result->row_count < limit; i++) {
            if (g_last_result->row_count * col_count >= capacity) {
                capacity *= 2;
                g_last_result->values = realloc(g_last_result->values, capacity * sizeof(Value));
                g_last_result->rows = realloc(g_last_result->rows, capacity * sizeof(int));
            }

            for (int j = 0; j < col_count && j < agg_count; j++) {
                Value* agg_val = (Value*)alist_get(&g_aggregate_results, j);
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
                g_last_result->values[g_last_result->row_count * col_count + j] = val;
            }
            g_last_result->rows[g_last_result->row_count] = i;
            g_last_result->row_count++;
        }
    } else {
        int row_count = alist_length(&table->rows);
        int limit = select->limit > 0 ? select->limit : row_count;

        for (int i = 0; i < row_count && g_last_result->row_count < limit; i++) {
            Row* row = (Row*)alist_get(&table->rows, i);
            if (!row || row->value_count == 0) continue;

            if (select->where_clause) {
                if (!eval_expression(select->where_clause, row, &table->schema)) {
                    continue;
                }
            }

            if (g_last_result->row_count * col_count >= capacity) {
                capacity *= 2;
                g_last_result->values = realloc(g_last_result->values, capacity * sizeof(Value));
                g_last_result->rows = realloc(g_last_result->rows, capacity * sizeof(int));
            }

            for (int j = 0; j < col_count; j++) {
                Value val;
                if (is_select_star) {
                    val = copy_string_value(&row->values[j]);
                } else {
                    Expr** expr = (Expr**)alist_get(&select->expressions, j);
                    val = eval_select_expression(expr[0], row, &table->schema);
                }
                g_last_result->values[g_last_result->row_count * col_count + j] = val;
            }
            g_last_result->rows[g_last_result->row_count] = i;
            g_last_result->row_count++;
        }
    }

    printf("\n");
    print_column_headers(select, table, is_select_star, col_count);
    for (int i = 0; i < col_count * 20; i++) printf("-");
    printf("\n");

    for (int i = 0; i < g_last_result->row_count; i++) {
        print_result_row(&g_last_result->values[i * col_count], col_count);
    }

    log_msg(LOG_INFO, "Projected %d rows from table '%s'", g_last_result->row_count, table->name);
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
