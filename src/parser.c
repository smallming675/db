#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "db.h"
#include "logger.h"

static Token* current_token;
static int token_index;

static void advance(void);
static Expr* parse_primary(void);
static Expr* parse_unary_expr(void);
static Expr* parse_comparison_expr(void);
static Expr* parse_and_expr(void);
static Expr* parse_or_expr(void);
static Expr* parse_where_clause(void);
static Expr* parse_aggregate_func(void);
static bool match(TokenType type);
static bool consume(TokenType type);
static bool expect(TokenType type);
static DataType parse_data_type(void);
static bool parse_column_def(ColumnDef* col);
static Value parse_value(void);
static ASTNode* parse_update(void);
static ASTNode* parse_delete(void);

static const char* token_type_name(TokenType type) {
    switch (type) {
        case TOKEN_KEYWORD:
            return "KEYWORD";
        case TOKEN_IDENTIFIER:
            return "IDENTIFIER";
        case TOKEN_STRING:
            return "STRING";
        case TOKEN_NUMBER:
            return "NUMBER";
        case TOKEN_OPERATOR:
            return "OPERATOR";
        case TOKEN_COMMA:
            return "COMMA";
        case TOKEN_SEMICOLON:
            return "SEMICOLON";
        case TOKEN_LPAREN:
            return "LPAREN";
        case TOKEN_RPAREN:
            return "RPAREN";
        case TOKEN_EOF:
            return "EOF";
        case TOKEN_ERROR:
            return "ERROR";
        case TOKEN_EQUALS:
            return "EQUALS";
        case TOKEN_NOT_EQUALS:
            return "NOT_EQUALS";
        case TOKEN_LESS:
            return "LESS";
        case TOKEN_LESS_EQUAL:
            return "LESS_EQUAL";
        case TOKEN_GREATER:
            return "GREATER";
        case TOKEN_GREATER_EQUAL:
            return "GREATER_EQUAL";
        case TOKEN_AND:
            return "AND";
        case TOKEN_OR:
            return "OR";
        case TOKEN_NOT:
            return "NOT";
        case TOKEN_LIKE:
            return "LIKE";
        default:
            return "UNKNOWN";
    }
}

static void advance(void) {
    current_token++;
    token_index++;
}

static bool match(TokenType type) { return current_token->type == type; }
static bool consume(TokenType type) {
    if (match(type)) {
        advance();
        return true;
    }
    return false;
}

static bool expect(TokenType type) {
    if (match(type)) {
        advance();
        return true;
    }
    log_msg(LOG_ERROR, "expect: Expected %s, got %s\n", token_type_name(type),
            token_type_name(current_token->type));
    return false;
}

static DataType parse_data_type(void) {
    if (consume(TOKEN_KEYWORD)) {
        if (strcmp(current_token[-1].value, "INT") == 0) return TYPE_INT;
        if (strcmp(current_token[-1].value, "STRING") == 0) return TYPE_STRING;
        if (strcmp(current_token[-1].value, "FLOAT") == 0) return TYPE_FLOAT;
    }
    return TYPE_INT;
}

static bool parse_column_def(ColumnDef* col) {
    if (!match(TOKEN_IDENTIFIER)) return false;

    strcpy(col->name, current_token->value);
    col->name[MAX_COLUMN_NAME_LEN - 1] = '\0';
    advance();

    col->type = parse_data_type();
    col->nullable = true;

    return true;
}

static ASTNode* parse_create_table(void) {
    if (!expect(TOKEN_IDENTIFIER)) {
        log_msg(LOG_ERROR, "parse_create_table: CREATE TABLE parse failed: missing table name");
        return NULL;
    }

    ASTNode* node = malloc(sizeof(ASTNode));
    node->type = AST_CREATE_TABLE;
    node->next = NULL;

    strncpy(node->create_table.table_def.name, current_token[-1].value, MAX_TABLE_NAME_LEN - 1);
    node->create_table.table_def.name[MAX_TABLE_NAME_LEN - 1] = '\0';
    log_msg(LOG_DEBUG, "parse_create_table: Parsing CREATE TABLE for table: %s",
            node->create_table.table_def.name);

    if (!expect(TOKEN_LPAREN)) {
        free(node);
        return NULL;
    }

    int col_count = 0;
    while (!match(TOKEN_RPAREN) && col_count < MAX_COLUMNS) {
        if (!parse_column_def(&node->create_table.table_def.columns[col_count])) {
            free(node);
            return NULL;
        }
        col_count++;
        if (!consume(TOKEN_COMMA)) break;
    }

    if (!expect(TOKEN_RPAREN)) {
        free(node);
        return NULL;
    }

    node->create_table.table_def.column_count = col_count;
    return node;
}

static Value parse_value(void) {
    Value val;

    if (match(TOKEN_STRING)) {
        val.type = TYPE_STRING;
        val.char_val = malloc(strlen(current_token->value) + 1);
        strcpy(val.char_val, current_token->value);

        advance();
        return val;
    } else if (match(TOKEN_NUMBER)) {
        if (strchr(current_token->value, '.')) {
            val.type = TYPE_FLOAT;
            val.float_val = atof(current_token->value);
        } else {
            val.type = TYPE_INT;
            val.int_val = atoi(current_token->value);
        }
        advance();
        return val;
    } else if (match(TOKEN_KEYWORD) && strcmp(current_token->value, "NULL") == 0) {
        val.type = TYPE_NULL;
        advance();
        return val;
    }

    // Default case
    val.type = TYPE_STRING;
    val.char_val = malloc(1);
    strcpy(val.char_val, "");
    return val;
}

static Expr* parse_aggregate_func(void) {
    Expr* expr = malloc(sizeof(Expr));
    if (!expr) return NULL;

    expr->type = EXPR_AGGREGATE_FUNC;

    if (strcmp(current_token->value, "COUNT") == 0) {
        expr->aggregate.func_type = FUNC_COUNT;
    } else if (strcmp(current_token->value, "SUM") == 0) {
        expr->aggregate.func_type = FUNC_SUM;
    } else if (strcmp(current_token->value, "AVG") == 0) {
        expr->aggregate.func_type = FUNC_AVG;
    } else if (strcmp(current_token->value, "MIN") == 0) {
        expr->aggregate.func_type = FUNC_MIN;
    } else if (strcmp(current_token->value, "MAX") == 0) {
        expr->aggregate.func_type = FUNC_MAX;
    } else {
        expr->aggregate.func_type = FUNC_COUNT;  
    }
    advance();

    if (!expect(TOKEN_LPAREN)) {
        free(expr);
        return NULL;
    }

    if (expr->aggregate.func_type == FUNC_COUNT && match(TOKEN_OPERATOR) &&
        strcmp(current_token->value, "*") == 0) {
        expr->aggregate.count_all = true;
        expr->aggregate.distinct = false;
        expr->aggregate.operand = NULL;
        advance();
    } else {
        if (match(TOKEN_DISTINCT)) {
            expr->aggregate.distinct = true;
            advance();
        } else {
            expr->aggregate.distinct = false;
        }

        expr->aggregate.operand = parse_or_expr();
        if (!expr->aggregate.operand) {
            free(expr);
            return NULL;
        }
        expr->aggregate.count_all = false;
    }

    if (!expect(TOKEN_RPAREN)) {
        if (expr->aggregate.operand) {
            free(expr->aggregate.operand);
        }
        free(expr);
        return NULL;
    }

    return expr;
}

static Expr* parse_primary(void) {
    Expr* expr = malloc(sizeof(Expr));
    if (!expr) return NULL;

    if (match(TOKEN_IDENTIFIER)) {
        expr->type = EXPR_COLUMN;
        strncpy(expr->column_name, current_token->value, MAX_COLUMN_NAME_LEN - 1);
        expr->column_name[MAX_COLUMN_NAME_LEN - 1] = '\0';
        advance();
    } else if (match(TOKEN_STRING) || match(TOKEN_NUMBER)) {
        expr->type = EXPR_VALUE;
        if (match(TOKEN_STRING)) {
            expr->value.char_val = malloc(strlen(current_token->value) + 1);
            strcpy(expr->value.char_val, current_token->value);
        } else {
            if (strchr(current_token->value, '.')) {
                expr->value.float_val = atof(current_token->value);
            } else {
                expr->value.int_val = atoi(current_token->value);
            }
        }
        advance();
    } else if (match(TOKEN_KEYWORD) && strcmp(current_token->value, "NULL") == 0) {
        expr->type = EXPR_VALUE;
        expr->value.char_val = malloc(5);
        strcpy(expr->value.char_val, "NULL");
        advance();
    } else if (match(TOKEN_AGGREGATE_FUNC)) {
        free(expr);
        return parse_aggregate_func();
    } else if (match(TOKEN_LPAREN)) {
        advance();
        expr = parse_or_expr();
        if (!expect(TOKEN_RPAREN)) {
            free(expr);
            return NULL;
        }
    } else {
        free(expr);
        return NULL;
    }

    return expr;
}

static Expr* parse_unary_expr(void) {
    if (match(TOKEN_NOT)) {
        Expr* expr = malloc(sizeof(Expr));
        expr->type = EXPR_UNARY_OP;
        expr->unary.op = OP_NOT;
        advance();
        expr->unary.operand = parse_unary_expr();
        return expr;
    }
    return parse_primary();
}

static Expr* parse_comparison_expr(void) {
    Expr* left = parse_unary_expr();

    while (match(TOKEN_EQUALS) || match(TOKEN_NOT_EQUALS) || match(TOKEN_LESS) ||
           match(TOKEN_LESS_EQUAL) || match(TOKEN_GREATER) || match(TOKEN_GREATER_EQUAL) ||
           match(TOKEN_LIKE)) {
        Expr* expr = malloc(sizeof(Expr));
        expr->type = EXPR_BINARY_OP;

        if (match(TOKEN_EQUALS)) {
            expr->binary.op = OP_EQUALS;
            advance();
        } else if (match(TOKEN_NOT_EQUALS)) {
            expr->binary.op = OP_NOT_EQUALS;
            advance();
        } else if (match(TOKEN_LESS)) {
            expr->binary.op = OP_LESS;
            advance();
        } else if (match(TOKEN_LESS_EQUAL)) {
            expr->binary.op = OP_LESS_EQUAL;
            advance();
        } else if (match(TOKEN_GREATER)) {
            expr->binary.op = OP_GREATER;
            advance();
        } else if (match(TOKEN_GREATER_EQUAL)) {
            expr->binary.op = OP_GREATER_EQUAL;
            advance();
        } else if (match(TOKEN_LIKE)) {
            expr->binary.op = OP_LIKE;
            advance();
        }

        expr->binary.left = left;
        expr->binary.right = parse_unary_expr();
        left = expr;
    }

    return left;
}

static Expr* parse_and_expr(void) {
    Expr* left = parse_comparison_expr();

    while (match(TOKEN_AND)) {
        Expr* expr = malloc(sizeof(Expr));
        expr->type = EXPR_BINARY_OP;
        expr->binary.op = OP_AND;
        advance();
        expr->binary.left = left;
        expr->binary.right = parse_comparison_expr();
        left = expr;
    }

    return left;
}

static Expr* parse_or_expr(void) {
    Expr* left = parse_and_expr();

    while (match(TOKEN_OR)) {
        Expr* expr = malloc(sizeof(Expr));
        expr->type = EXPR_BINARY_OP;
        expr->binary.op = OP_OR;
        advance();
        expr->binary.left = left;
        expr->binary.right = parse_and_expr();
        left = expr;
    }

    return left;
}

static Expr* parse_where_clause(void) {
    if (match(TOKEN_KEYWORD) && strcmp(current_token->value, "WHERE") == 0) {
        advance();
        return parse_or_expr();
    }
    return NULL;
}

static ASTNode* parse_insert(void) {
    if (!expect(TOKEN_KEYWORD) || strcmp(current_token[-1].value, "INTO") != 0) return NULL;
    if (!expect(TOKEN_IDENTIFIER)) return NULL;

    ASTNode* node = malloc(sizeof(ASTNode));
    node->type = AST_INSERT_ROW;
    node->next = NULL;

    strncpy(node->insert.table_name, current_token[-1].value, MAX_TABLE_NAME_LEN - 1);
    node->insert.table_name[MAX_TABLE_NAME_LEN - 1] = '\0';

    if (!expect(TOKEN_LPAREN)) {
        free(node);
        return NULL;
    }

    int val_count = 0;
    while (!match(TOKEN_RPAREN) && val_count < MAX_COLUMNS) {
        node->insert.values[val_count].value = parse_value();
        strcpy(node->insert.values[val_count].column_name, "");
        val_count++;
        if (!consume(TOKEN_COMMA)) break;
    }

    if (!expect(TOKEN_RPAREN)) {
        free(node);
        return NULL;
    }

    node->insert.value_count = val_count;
    return node;
}

static ASTNode* parse_select(void) {
    ASTNode* node = malloc(sizeof(ASTNode));
    if (!node) return NULL;
    node->type = AST_SELECT;
    node->next = NULL;

    int expr_count = 0;
    if (match(TOKEN_OPERATOR) && strcmp(current_token->value, "*") == 0) {
        Expr* star_expr = malloc(sizeof(Expr));
        if (star_expr) {
            star_expr->type = EXPR_VALUE;
            star_expr->value.char_val = malloc(2);
            strcpy(star_expr->value.char_val, "*");
        }
        node->select.expressions[expr_count] = star_expr;
        advance();
        expr_count++;
    } else {
        while (expr_count < MAX_COLUMNS) {
            if (match(TOKEN_IDENTIFIER) ||
                (match(TOKEN_KEYWORD) && strcmp(current_token->value, "FROM") != 0) ||
                match(TOKEN_AGGREGATE_FUNC)) {
                Expr* expr = parse_or_expr();
                if (!expr) {
                    for (int i = 0; i < expr_count; i++) {
                        if (node->select.expressions[i]) {
                            free(node->select.expressions[i]);
                        }
                    }
                    free(node);
                    return NULL;
                }
                node->select.expressions[expr_count] = expr;
                expr_count++;
            } else {
                break;
            }
            if (!consume(TOKEN_COMMA)) {
                break;
            }
        }
    }

    if (!expect(TOKEN_KEYWORD) || strcmp(current_token[-1].value, "FROM") != 0) {
        for (int i = 0; i < expr_count; i++) {
            if (node->select.expressions[i]) {
                free(node->select.expressions[i]);
            }
        }
        free(node);
        return NULL;
    }

    if (!expect(TOKEN_IDENTIFIER)) {
        for (int i = 0; i < expr_count; i++) {
            if (node->select.expressions[i]) {
                free(node->select.expressions[i]);
            }
        }
        free(node);
        return NULL;
    }

    strncpy(node->select.table_name, current_token[-1].value, MAX_TABLE_NAME_LEN - 1);
    node->select.table_name[MAX_TABLE_NAME_LEN - 1] = '\0';

    node->select.where_clause = parse_where_clause();
    node->select.expression_count = expr_count;

    return node;
}

static ASTNode* parse_drop_table(void) {
    if (!match(TOKEN_IDENTIFIER)) return NULL;

    ASTNode* node = malloc(sizeof(ASTNode));
    node->type = AST_DROP_TABLE;
    node->next = NULL;

    strncpy(node->drop_table.table_name, current_token->value, MAX_TABLE_NAME_LEN - 1);
    node->drop_table.table_name[MAX_TABLE_NAME_LEN - 1] = '\0';
    advance();

    return node;
}

static ASTNode* parse_update(void) {
    if (!expect(TOKEN_IDENTIFIER)) {
        log_msg(LOG_DEBUG, "parse_update: Expected identifier");
        return NULL;
    }

    ASTNode* node = malloc(sizeof(ASTNode));
    node->type = AST_UPDATE_ROW;
    node->next = NULL;

    strncpy(node->update.table_name, current_token[-1].value, MAX_TABLE_NAME_LEN - 1);
    node->update.table_name[MAX_TABLE_NAME_LEN - 1] = '\0';

    if (!expect(TOKEN_KEYWORD) || strcmp(current_token[-1].value, "SET") != 0) {
        free(node);
        return NULL;
    }

    int val_count = 0;
    while (val_count < MAX_COLUMNS) {
        if (match(TOKEN_KEYWORD) && strcmp(current_token->value, "WHERE") == 0) {
            break;
        }
        if (!match(TOKEN_IDENTIFIER)) {
            free(node);
            return NULL;
        }
        size_t len = strlen(current_token->value);
        if (len >= MAX_COLUMN_NAME_LEN) len = MAX_COLUMN_NAME_LEN - 1;
        memcpy(node->update.values[val_count].column_name, current_token->value, len);
        node->update.values[val_count].column_name[len] = '\0';
        advance();

        if (!expect(TOKEN_EQUALS)) {
            free(node);
            return NULL;
        }

        node->update.values[val_count].value = parse_value();
        val_count++;

        if (!consume(TOKEN_COMMA)) {
            break;
        }
    }

    node->update.value_count = val_count;
    if (match(TOKEN_KEYWORD) && strcmp(current_token->value, "WHERE") == 0) {
        advance();
        node->update.where_clause = parse_or_expr();
    } else {
        node->update.where_clause = NULL;
    }

    expect(TOKEN_SEMICOLON);

    return node;
}

static ASTNode* parse_delete(void) {
    if (!expect(TOKEN_KEYWORD) || strcmp(current_token[-1].value, "FROM") != 0) return NULL;
    if (!expect(TOKEN_IDENTIFIER)) return NULL;

    ASTNode* node = malloc(sizeof(ASTNode));
    node->type = AST_DELETE_ROW;
    node->next = NULL;

    strncpy(node->delete.table_name, current_token[-1].value, MAX_TABLE_NAME_LEN - 1);
    node->delete.table_name[MAX_TABLE_NAME_LEN - 1] = '\0';

    if (match(TOKEN_KEYWORD) && strcmp(current_token->value, "WHERE") == 0) {
        advance();
        node->delete.where_clause = parse_or_expr();
    } else {
        node->delete.where_clause = NULL;
    }

    expect(TOKEN_SEMICOLON);

    return node;
}

ASTNode* parse(Token* tokens) {
    if (!tokens) {
        log_msg(LOG_WARN, "parse: Parse called with NULL tokens");
        return NULL;
    }

    log_msg(LOG_DEBUG, "parse: Starting parse");
    current_token = tokens;
    token_index = 0;

    if (match(TOKEN_KEYWORD)) {
        if (strcasecmp(current_token->value, "CREATE") == 0) {
            advance();
            if (strcasecmp(current_token->value, "TABLE") == 0) {
                advance();
                log_msg(LOG_DEBUG, "parse: Parsing CREATE TABLE statement");
                return parse_create_table();
            }
        } else if (strcasecmp(current_token->value, "INSERT") == 0) {
            advance();
            log_msg(LOG_DEBUG, "parse: Parsing INSERT statement");
            return parse_insert();
        } else if (strcasecmp(current_token->value, "SELECT") == 0) {
            advance();
            log_msg(LOG_DEBUG, "parse: Parsing SELECT statement");
            return parse_select();
        } else if (strcasecmp(current_token->value, "UPDATE") == 0) {
            advance();
            log_msg(LOG_DEBUG, "parse: Parsing UPDATE statement");
            return parse_update();
        } else if (strcasecmp(current_token->value, "DELETE") == 0) {
            advance();
            log_msg(LOG_DEBUG, "parse: Parsing DELETE statement");
            return parse_delete();
        } else if (strcmp(current_token->value, "DROP") == 0) {
            advance();
            if (match(TOKEN_KEYWORD) && strcmp(current_token->value, "TABLE") == 0) {
                advance();
                log_msg(LOG_DEBUG, "parse: Parsing DROP TABLE statement");
                return parse_drop_table();
            }
        }
    }

    log_msg(LOG_ERROR, "parse: Failed to parse tokens: invalid syntax");
    return NULL;
}

void free_ast(ASTNode* ast) {
    if (!ast) return;
    free_ast(ast->next);
    free(ast);
}
