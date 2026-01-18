#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "db.h"
#include "logger.h"
#include "table.h"

static unsigned int make_time(int hour, int minute, int second) { return ((hour & 0xFF) << 12) | ((minute & 0x3F) << 6) | (second & 0x3F); }
static unsigned int make_date(int year, int month, int day) { return ((year & 0x3FFFFF) << 9) | ((month & 0xF) << 5) | (day & 0x1F); }

#define COLOR_RESET "\x1b[0m"
#define COLOR_RED "\x1b[31m"
#define COLOR_GREEN "\x1b[32m"
#define COLOR_YELLOW "\x1b[33m"
#define COLOR_BLUE "\x1b[34m"
#define COLOR_MAGENTA "\x1b[35m"
#define COLOR_CYAN "\x1b[36m"
#define COLOR_BOLD "\x1b[1m"
#define COLOR_DIM "\x1b[2m"

static int levenshtein_distance(const char* s1, const char* s2) {
    int len1 = strlen(s1);
    int len2 = strlen(s2);
    int* prev = malloc((len2 + 1) * sizeof(int));
    int* curr = malloc((len2 + 1) * sizeof(int));
    if (!prev || !curr) {
        if (prev) free(prev);
        if (curr) free(curr);
        return len1 + len2;
    }

    for (int j = 0; j <= len2; j++) {
        prev[j] = j;
    }

    for (int i = 1; i <= len1; i++) {
        curr[0] = i;
        for (int j = 1; j <= len2; j++) {
            int cost = (s1[i - 1] == s2[j - 1]) ? 0 : 1;
            curr[j] = (curr[j - 1] + 1 < prev[j] + 1 ? curr[j - 1] + 1 : prev[j] + 1) < prev[j - 1] + cost ?
                      (curr[j - 1] + 1 < prev[j] + 1 ? curr[j - 1] + 1 : prev[j] + 1) : prev[j - 1] + cost;
        }
        int* temp = prev;
        prev = curr;
        curr = temp;
    }

    int dist = prev[len2];
    free(prev);
    free(curr);
    return dist;
}

static void suggest_similar_table(ParseContext* ctx, const char* requested_name) {
    const char* best_match = NULL;
    int best_dist = 3;

    for (int i = 0; i < alist_length(&tables); i++) {
        Table* table = (Table*)alist_get(&tables, i);
        if (table) {
            int dist = levenshtein_distance(requested_name, table->name);
            if (dist < best_dist) {
                best_dist = dist;
                best_match = table->name;
            }
        }
    }

    if (best_match) {
        snprintf(ctx->error.suggestion, sizeof(ctx->error.suggestion), "Did you mean '%s'?", best_match);
    }
}

static Token* current_token;
static int token_index;
static int column_position;
static const char* input_buffer;
static ParseContext g_parse_context;

static void advance(void);
static Expr* parse_primary(ParseContext* ctx);
static Expr* parse_subquery(ParseContext* ctx);
static Expr* parse_unary_expr(ParseContext* ctx);
static Expr* parse_comparison_expr(ParseContext* ctx);
static Expr* parse_and_expr(ParseContext* ctx);
static Expr* parse_or_expr(ParseContext* ctx);
static Expr* parse_where_clause(ParseContext* ctx);
static Expr* parse_aggregate_func(ParseContext* ctx);
static Expr* parse_scalar_func(ParseContext* ctx);
static bool match(TokenType type);
static bool consume(ParseContext* ctx, TokenType type);
static bool expect(ParseContext* ctx, TokenType type, const char* context);

static DataType parse_data_type(ParseContext* ctx);
static bool parse_column_def(ParseContext* ctx, ColumnDef* col);
static Value parse_value(ParseContext* ctx);
static ASTNode* parse_update(ParseContext* ctx);
static ASTNode* parse_delete(ParseContext* ctx);
static ASTNode* parse_select(ParseContext* ctx);
static ASTNode* parse_create_index(ParseContext* ctx);
static ASTNode* parse_drop_index(ParseContext* ctx);

static void free_column_value(void* ptr) {
    ColumnValue* cv = (ColumnValue*)ptr;
    if (!cv) return;
    if (cv->value.type == TYPE_STRING && cv->value.char_val) {
        free(cv->value.char_val);
        cv->value.char_val = NULL;
    }
    cv->value.type = TYPE_NULL;
}

static void free_expr_contents(Expr* expr) {
    if (!expr) return;
    switch (expr->type) {
        case EXPR_BINARY_OP:
            if (expr->binary.left) free_expr_contents(expr->binary.left);
            if (expr->binary.right) free_expr_contents(expr->binary.right);
            break;
        case EXPR_UNARY_OP:
            if (expr->unary.operand) free_expr_contents(expr->unary.operand);
            break;
        case EXPR_AGGREGATE_FUNC:
            if (expr->aggregate.operand) free_expr_contents(expr->aggregate.operand);
            break;
        case EXPR_SCALAR_FUNC:
            for (int i = 0; i < expr->scalar.arg_count; i++) {
                if (expr->scalar.args[i]) free_expr_contents(expr->scalar.args[i]);
            }
            break;
        case EXPR_SUBQUERY:
            if (expr->subquery.subquery) free_ast(expr->subquery.subquery);
            break;
        default:
            break;
    }
}

const char* token_type_name(TokenType type) {
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
        case TOKEN_AGGREGATE_FUNC:
            return "AGGREGATE_FUNC";
        case TOKEN_SCALAR_FUNC:
            return "SCALAR_FUNC";
        case TOKEN_DISTINCT:
            return "DISTINCT";
        case TOKEN_TIME:
            return "TIME";
        case TOKEN_DATE:
            return "DATE";
        case TOKEN_ORDER:
            return "ORDER";
        case TOKEN_BY:
            return "BY";
        case TOKEN_AS:
            return "AS";
        case TOKEN_EXISTS:
            return "EXISTS";
        case TOKEN_IN:
            return "IN";
        case TOKEN_PRIMARY:
            return "PRIMARY";
        case TOKEN_KEY:
            return "KEY";
        case TOKEN_REFERENCES:
            return "REFERENCES";
        case TOKEN_NULL:
            return "NULL";
        case TOKEN_UNIQUE:
            return "UNIQUE";
        case TOKEN_JOIN:
            return "JOIN";
        case TOKEN_INNER:
            return "INNER";
        case TOKEN_LEFT:
            return "LEFT";
        default:
            return "UNKNOWN";
    }
}

static const char* get_token_type_desc(TokenType type) {
    switch (type) {
        case TOKEN_KEYWORD:
            return "a keyword (CREATE, INSERT, SELECT, etc.)";
        case TOKEN_IDENTIFIER:
            return "an identifier (table or column name)";
        case TOKEN_STRING:
            return "a string literal (e.g., 'value')";
        case TOKEN_NUMBER:
            return "a number";
        case TOKEN_OPERATOR:
            return "an operator (=, !=, <, >, etc.)";
        case TOKEN_COMMA:
            return "a comma (,)";
        case TOKEN_SEMICOLON:
            return "a semicolon (;)";
        case TOKEN_LPAREN:
            return "an opening parenthesis (()";
        case TOKEN_RPAREN:
            return "a closing parenthesis ())";
        case TOKEN_EOF:
            return "end of input";
        case TOKEN_ERROR:
            return "an invalid character";
        default:
            return "a token";
    }
}

static const char* get_context_suggestion(const char* context, TokenType expected) {
    static char suggestion[512];

    if (strcmp(context, "CREATE TABLE") == 0) {
        if (expected == TOKEN_IDENTIFIER) {
            snprintf(suggestion, sizeof(suggestion),
                     "CREATE TABLE syntax:\n"
                     "  CREATE TABLE table_name (column1 type1, column2 type2, ...)\n"
                     "Example: CREATE TABLE users (id INT, name STRING, age INT)");
        } else if (expected == TOKEN_LPAREN) {
            snprintf(suggestion, sizeof(suggestion),
                     "After table name, expect opening parenthesis '('\n"
                     "  CREATE TABLE table_name ( ... )");
        } else if (expected == TOKEN_RPAREN) {
            snprintf(suggestion, sizeof(suggestion),
                     "Column definitions should end with closing parenthesis ')'\n"
                     "  CREATE TABLE users (id INT, name STRING)");
        } else {
            snprintf(suggestion, sizeof(suggestion), "Check CREATE TABLE syntax");
        }
    } else if (strcmp(context, "INSERT") == 0) {
        if (expected == TOKEN_KEYWORD && strcmp(current_token[-1].value, "INTO") == 0) {
            snprintf(suggestion, sizeof(suggestion),
                     "INSERT syntax:\n"
                     "  INSERT INTO table_name VALUES (value1, value2, ...)\n"
                     "Example: INSERT INTO users VALUES ('John', 25)");
        } else if (expected == TOKEN_LPAREN) {
            snprintf(suggestion, sizeof(suggestion),
                     "After VALUES keyword, expect opening parenthesis '('\n"
                     "  INSERT INTO table_name VALUES ( ... )");
        } else if (expected == TOKEN_RPAREN) {
            snprintf(suggestion, sizeof(suggestion),
                     "Values should end with closing parenthesis ')'\n"
                     "  INSERT INTO users VALUES ('John', 25)");
        } else {
            snprintf(suggestion, sizeof(suggestion), "Check INSERT syntax");
        }
    } else if (strcmp(context, "SELECT") == 0) {
        if (expected == TOKEN_KEYWORD && strcmp(current_token[-1].value, "FROM") == 0) {
            snprintf(suggestion, sizeof(suggestion),
                     "SELECT syntax:\n"
                     "  SELECT columns FROM table_name [WHERE condition]\n"
                     "Examples:\n"
                     "  SELECT * FROM users\n"
                     "  SELECT name, age FROM users WHERE age > 18");
        } else if (expected == TOKEN_IDENTIFIER) {
            snprintf(suggestion, sizeof(suggestion),
                     "After FROM keyword, expect table name\n"
                     "  SELECT * FROM table_name");
        } else {
            snprintf(suggestion, sizeof(suggestion), "Check SELECT syntax");
        }
    } else if (strcmp(context, "UPDATE") == 0) {
        if (expected == TOKEN_IDENTIFIER) {
            snprintf(suggestion, sizeof(suggestion),
                     "UPDATE syntax:\n"
                     "  UPDATE table_name SET column1=value1, column2=value2 [WHERE condition]\n"
                     "Example: UPDATE users SET age=30 WHERE name='John'");
        } else if (expected == TOKEN_KEYWORD && strcmp(current_token[-1].value, "SET") == 0) {
            snprintf(suggestion, sizeof(suggestion),
                     "After table name, expect SET keyword\n"
                     "  UPDATE table_name SET column=value");
        } else {
            snprintf(suggestion, sizeof(suggestion), "Check UPDATE syntax");
        }
    } else if (strcmp(context, "DELETE") == 0) {
        if (expected == TOKEN_KEYWORD && strcmp(current_token[-1].value, "FROM") == 0) {
            snprintf(suggestion, sizeof(suggestion),
                     "DELETE syntax:\n"
                     "  DELETE FROM table_name [WHERE condition]\n"
                     "Examples:\n"
                     "  DELETE FROM users WHERE age < 18");
        } else {
            snprintf(suggestion, sizeof(suggestion), "Check DELETE syntax");
        }
    } else if (strcmp(context, "aggregate function") == 0) {
        snprintf(suggestion, sizeof(suggestion),
                 "Aggregate function syntax:\n"
                 "  COUNT([DISTINCT] column | *)\n"
                 "  SUM(column), AVG(column), MIN(column), MAX(column)\n"
                 "Examples:\n"
                 "  COUNT(*)\n"
                 "  COUNT(DISTINCT name)\n"
                 "  SUM(price)");
    } else if (strcmp(context, "expression") == 0) {
        snprintf(suggestion, sizeof(suggestion),
                 "Expression should be:\n"
                 "  - A column name (identifier)\n"
                 "  - A value (string, number, date, time)\n"
                 "  - An aggregate function (COUNT, SUM, AVG, MIN, MAX)\n"
                 "  - A parenthesized expression (expr)");
    } else {
        snprintf(suggestion, sizeof(suggestion), "Check syntax near '%s'", context);
    }

    return suggestion;
}

void parse_error_init(ParseContext* ctx, const char* input, Token* tokens, int token_count) {
    ctx->tokens = tokens;
    ctx->token_count = token_count;
    ctx->current_token_index = 0;
    ctx->error_occurred = false;
    memset(&ctx->error, 0, sizeof(ParseError));
    if (input) {
        strncpy(ctx->input, input, sizeof(ctx->input) - 1);
        ctx->input[sizeof(ctx->input) - 1] = '\0';
    } else {
        ctx->input[0] = '\0';
    }
}

void parse_error_set(ParseContext* ctx, ParseErrorCode code, const char* message,
                     const char* expected, const char* found, const char* suggestion) {
    ctx->error_occurred = true;
    ctx->error.code = code;

    if (message) {
        strncpy(ctx->error.message, message, sizeof(ctx->error.message) - 1);
        ctx->error.message[sizeof(ctx->error.message) - 1] = '\0';
    }

    if (expected) {
        strncpy(ctx->error.expected, expected, sizeof(ctx->error.expected) - 1);
        ctx->error.expected[sizeof(ctx->error.expected) - 1] = '\0';
    }

    if (found) {
        strncpy(ctx->error.found, found, sizeof(ctx->error.found) - 1);
        ctx->error.found[sizeof(ctx->error.found) - 1] = '\0';
    }

    if (suggestion) {
        strncpy(ctx->error.suggestion, suggestion, sizeof(ctx->error.suggestion) - 1);
        ctx->error.suggestion[sizeof(ctx->error.suggestion) - 1] = '\0';
    }

    ctx->error.token_index = token_index;

    int pos = 0;
    for (int i = 0; i < token_index && i < ctx->token_count; i++) {
        pos += strlen(ctx->tokens[i].value);
        if (i < token_index - 1 && i + 1 < ctx->token_count) {
            pos++;
        }
    }
    ctx->error.column = pos + 1;
}

static int calculate_line_number(const char* input, int target_pos) {
    if (!input || target_pos < 0) return 1;

    int line = 1;
    for (int i = 0; i < target_pos && input[i]; i++) {
        if (input[i] == '\n') line++;
    }
    return line;
}

void parse_error_report(ParseContext* ctx) {
    if (!ctx->error_occurred) return;

    fprintf(stderr, COLOR_RESET COLOR_RED "%s" COLOR_RESET ": ",
            parse_error_code_str(ctx->error.code));

    ctx->error.line = calculate_line_number(ctx->input, 0);
    fprintf(stderr, "%s\n", ctx->error.message);

    fprintf(stderr, "Input:" COLOR_RESET);
    fprintf(stderr, " " COLOR_YELLOW "%s" COLOR_RESET "\n", ctx->input);

    fprintf(stderr, "Expected:" COLOR_RESET);
    fprintf(stderr, " " COLOR_GREEN "%s" COLOR_RESET "\n", ctx->error.expected);

    fprintf(stderr, "Found:" COLOR_RESET);
    fprintf(stderr, " " COLOR_RED "%s" COLOR_RESET "\n", ctx->error.found);

    if (ctx->error.token_index >= 0 && ctx->error.token_index < ctx->token_count) {
        fprintf(stderr, "Error location (token %d):\n", ctx->error.token_index);
        for (int i = 0; i < ctx->token_count && i < 20; i++) {
            Token* t = &ctx->tokens[i];
            if (i == ctx->error.token_index) {
                fprintf(stderr,
                        "  " COLOR_DIM "[%d] " COLOR_RESET COLOR_YELLOW "%-20s" COLOR_RESET
                        " " COLOR_CYAN "'%s'" COLOR_RESET " " COLOR_RED "<-- ERROR HERE" COLOR_RESET
                        "\n",
                        i, token_type_name(t->type), t->value);
            } else if (i > ctx->error.token_index - 3 && i < ctx->error.token_index + 3) {
                fprintf(stderr,
                        "  " COLOR_DIM "[%d] " COLOR_RESET COLOR_YELLOW "%-20s" COLOR_RESET
                        " " COLOR_CYAN "'%s'" COLOR_RESET "\n",
                        i, token_type_name(t->type), t->value);
            }
        }
        fprintf(stderr, "\n");

        Token* err_tok = &ctx->tokens[ctx->error.token_index];
        const char* token_val = err_tok->value;

        int token_pos = -1;
        const char* found = ctx->input;
        while (*found) {
            if (strncmp(found, token_val, strlen(token_val)) == 0) {
                token_pos = found - ctx->input;
                break;
            }
            found++;
        }

        if (token_pos < 0) {
            int pos = 0;
            for (int i = 0; i <= ctx->error.token_index && i < ctx->token_count; i++) {
                if (i == ctx->error.token_index) {
                    break;
                }
                pos += strlen(ctx->tokens[i].value) + 1;
            }
            token_pos = pos;
        }

        fprintf(stderr, "Nearby context in input:\n");
        int context_start = token_pos > 20 ? token_pos - 20 : 0;
        int context_end = token_pos + strlen(token_val) + 20;
        int total_len = strlen(ctx->input);
        if (context_end > total_len) context_end = total_len;

        fprintf(stderr, "  " COLOR_DIM);
        if (context_start > 0) fprintf(stderr, "...");
        fprintf(stderr, COLOR_RESET);
        for (int i = context_start; i < context_end && ctx->input[i]; i++) {
            fprintf(stderr, "%c", ctx->input[i]);
        }
        if (context_end < total_len) fprintf(stderr, COLOR_DIM "...");
        fprintf(stderr, COLOR_RESET "\n");

        int marker_start = 2;
        if (context_start > 0) marker_start += 3;
        marker_start += token_pos - context_start;

        for (int i = 0; i < marker_start; i++) {
            fprintf(stderr, " ");
        }

        int marker_len = strlen(token_val);
        for (int i = 0; i < marker_len; i++) {
            fprintf(stderr, COLOR_RED "^");
        }
        fprintf(stderr, "\n");
    }

    fprintf(stderr, COLOR_GREEN COLOR_BOLD "Fix:" COLOR_RESET "\n");

    size_t len = strlen(ctx->error.suggestion);
    size_t sug_start = 0;
    while (sug_start < len) {
        size_t end = sug_start;
        while (end < len && ctx->error.suggestion[end] != '\n') end++;

        fprintf(stderr, "  ");
        for (size_t i = sug_start; i < end && i < sug_start + 70; i++) {
            fprintf(stderr, "%c", ctx->error.suggestion[i]);
        }
        fprintf(stderr, "\n");

        if (end < len && ctx->error.suggestion[end] == '\n') end++;
        sug_start = end;
    }

    fprintf(stderr, "\n");
}

const char* parse_error_code_str(ParseErrorCode code) {
    switch (code) {
        case PARSE_ERROR_NONE:
            return "None";
        case PARSE_ERROR_UNEXPECTED_TOKEN:
            return "Unexpected Token";
        case PARSE_ERROR_MISSING_TOKEN:
            return "Missing Token";
        case PARSE_ERROR_INVALID_SYNTAX:
            return "Invalid Syntax";
        case PARSE_ERROR_UNTERMINATED_STRING:
            return "Unterminated String";
        case PARSE_ERROR_INVALID_NUMBER:
            return "Invalid Number";
        case PARSE_ERROR_UNEXPECTED_END:
            return "Unexpected End";
        case PARSE_ERROR_TOO_MANY_COLUMNS:
            return "Too Many Columns";
        default:
            return "Unknown";
    }
}

ParseContext* parse_get_context(void) { return &g_parse_context; }

static void advance(void) {
    current_token++;
    token_index++;
    column_position += strlen(current_token[-1].value);
}

static bool match(TokenType type) { return current_token->type == type; }

static bool consume(ParseContext* ctx, TokenType type) {
    (void)ctx;
    if (match(type)) {
        advance();
        return true;
    }
    return false;
}

static bool expect(ParseContext* ctx, TokenType type, const char* context) {
    if (match(type)) {
        advance();
        return true;
    }

    char expected[256];
    char found[256];
    char msg[512];
    char suggestion[512];

    snprintf(expected, sizeof(expected), "%s (%s)", token_type_name(type),
             get_token_type_desc(type));

    if (current_token->type == TOKEN_EOF) {
        snprintf(found, sizeof(found), "end of input");

        if (strcmp(context, "SELECT") == 0) {
            snprintf(msg, sizeof(msg), "Missing table name after FROM");
            snprintf(suggestion, sizeof(suggestion),
                     "SELECT syntax:\n"
                     "  SELECT columns FROM table_name [WHERE condition]\n\n"
                     "You need to provide:\n"
                     "  1. A table name after FROM (required)\n"
                     "  2. Optional WHERE clause to filter results\n\n"
                     "Examples:\n"
                     "  SELECT * FROM users\n"
                     "  SELECT name, age FROM users\n"
                     "  SELECT * FROM users WHERE age > 18");
        } else if (strcmp(context, "CREATE TABLE") == 0) {
            snprintf(msg, sizeof(msg), "Incomplete CREATE TABLE statement");
            snprintf(suggestion, sizeof(suggestion),
                     "CREATE TABLE syntax:\n"
                     "  CREATE TABLE table_name (column1 type1, column2 type2, ...)\n\n"
                     "You need to provide:\n"
                     "  1. A table name (required)\n"
                     "  2. Column definitions inside parentheses (required)\n\n"
                     "Examples:\n"
                     "  CREATE TABLE users (id INT, name STRING)\n"
                     "  CREATE TABLE products (id INT, name STRING, price FLOAT)");
        } else if (strcmp(context, "INSERT") == 0) {
            snprintf(msg, sizeof(msg), "Incomplete INSERT statement");
            snprintf(suggestion, sizeof(suggestion),
                     "INSERT syntax:\n"
                     "  INSERT INTO table_name VALUES (value1, value2, ...)\n\n"
                     "You need to provide:\n"
                     "  1. Table name after INTO (required)\n"
                     "  2. VALUES keyword (required)\n"
                     "  3. Values inside parentheses (required)\n\n"
                     "Examples:\n"
                     "  INSERT INTO users VALUES ('John', 25)\n"
                     "  INSERT INTO products VALUES ('Widget', 19.99)");
        } else if (strcmp(context, "UPDATE") == 0) {
            snprintf(msg, sizeof(msg), "Incomplete UPDATE statement");
            snprintf(suggestion, sizeof(suggestion),
                     "UPDATE syntax:\n"
                     "  UPDATE table_name SET column1=value1, ... [WHERE condition]\n\n"
                     "You need to provide:\n"
                     "  1. A table name (required)\n"
                     "  2. SET keyword followed by assignments (required)\n"
                     "  3. Optional WHERE clause to filter updates\n\n"
                     "Examples:\n"
                     "  UPDATE users SET age=30\n"
                     "  UPDATE users SET age=30 WHERE name='John'");
        } else if (strcmp(context, "DELETE") == 0) {
            snprintf(msg, sizeof(msg), "Incomplete DELETE statement");
            snprintf(suggestion, sizeof(suggestion),
                     "DELETE syntax:\n"
                     "  DELETE FROM table_name [WHERE condition]\n\n"
                     "You need to provide:\n"
                     "  1. A table name after FROM (required)\n"
                     "  2. Optional WHERE clause to filter deletions\n\n"
                     "Examples:\n"
                     "  DELETE FROM users\n"
                     "  DELETE FROM users WHERE age < 18");
        } else if (strcmp(context, "aggregate function") == 0) {
            snprintf(msg, sizeof(msg),
                     "Missing closing parenthesis or argument in aggregate function");
            snprintf(suggestion, sizeof(suggestion),
                     "Aggregate function syntax:\n"
                     "  COUNT([DISTINCT] column | *)\n"
                     "  SUM(column), AVG(column), MIN(column), MAX(column)\n\n"
                     "Examples:\n"
                     "  COUNT(*)\n"
                     "  COUNT(DISTINCT name)\n"
                     "  SUM(price)");
        } else if (strcmp(context, "expression") == 0) {
            snprintf(msg, sizeof(msg), "Unexpected end of expression");
            snprintf(suggestion, sizeof(suggestion),
                     "Expression should contain:\n"
                     "  - Column names or values\n"
                     "  - Operators (=, !=, <, >, AND, OR)\n"
                     "  - Aggregate functions\n\n"
                     "Examples:\n"
                     "  age > 18\n"
                     "  name = 'John' AND age >= 21\n"
                     "  price * quantity > 100");
        } else {
            snprintf(msg, sizeof(msg), "Unexpected end of input while parsing %s", context);
            snprintf(suggestion, sizeof(suggestion), "Check the syntax for %s statement", context);
        }
    } else {
        const char* type_name = token_type_name(current_token->type);
        size_t type_len = strlen(type_name);
        size_t val_len = strlen(current_token->value);
        if (type_len > 100) type_len = 100;
        if (val_len > 100) val_len = 100;
        snprintf(found, sizeof(found), "%.*s '%.*s'", (int)type_len, type_name, (int)val_len,
                 current_token->value);
        snprintf(msg, sizeof(msg), "Unexpected token while parsing %s", context);
        snprintf(suggestion, sizeof(suggestion), "%s", get_context_suggestion(context, type));
    }

    parse_error_set(ctx, PARSE_ERROR_MISSING_TOKEN, msg, expected, found, suggestion);

    return false;
}

static DataType parse_data_type(ParseContext* ctx) {
    if (consume(ctx, TOKEN_KEYWORD)) {
        if (strcasecmp(current_token[-1].value, "INT") == 0) return TYPE_INT;
        if (strcasecmp(current_token[-1].value, "INTEGER") == 0) return TYPE_INT;
        if (strcasecmp(current_token[-1].value, "STRING") == 0) return TYPE_STRING;
        if (strcasecmp(current_token[-1].value, "TEXT") == 0) return TYPE_STRING;
        if (strcasecmp(current_token[-1].value, "FLOAT") == 0) return TYPE_FLOAT;
    } else if (consume(ctx, TOKEN_DATE)) {
        return TYPE_DATE;
    } else if (consume(ctx, TOKEN_TIME)) {
        return TYPE_TIME;
    }
    return TYPE_INT;
}

static bool parse_column_def(ParseContext* ctx, ColumnDef* col) {
    if (!match(TOKEN_IDENTIFIER)) {
        char expected[256];
        snprintf(expected, sizeof(expected), "column name (IDENTIFIER)");
        parse_error_set(ctx, PARSE_ERROR_UNEXPECTED_TOKEN,
                        "Expected column name in column definition", expected,
                        current_token->type == TOKEN_EOF ? "end of input"
                                                         : token_type_name(current_token->type),
                        "Column definition syntax: column_name TYPE [constraints]\n"
                        "Example: id INT PRIMARY KEY, name STRING NOT NULL, price FLOAT UNIQUE");
        return false;
    }

    strcpy(col->name, current_token->value);
    col->name[MAX_COLUMN_NAME_LEN - 1] = '\0';
    advance();
    col->type = parse_data_type(ctx);
    col->flags = COL_FLAG_NULLABLE;
    col->references_table[0] = '\0';
    col->references_column[0] = '\0';

    while (current_token->type != TOKEN_COMMA && current_token->type != TOKEN_RPAREN &&
           current_token->type != TOKEN_SEMICOLON) {
        if (current_token->type == TOKEN_NOT && current_token[1].type == TOKEN_NULL) {
            col->flags &= ~COL_FLAG_NULLABLE;
            log_msg(LOG_DEBUG, "parse_column_def: Column '%s' NOT NULL", col->name);
            advance();
            advance();
        } else if (current_token->type == TOKEN_UNIQUE) {
            col->flags |= COL_FLAG_UNIQUE;
            log_msg(LOG_DEBUG, "parse_column_def: Column '%s' UNIQUE", col->name);
            advance();
        } else if (current_token->type == TOKEN_PRIMARY && current_token[1].type == TOKEN_KEY) {
            col->flags |= COL_FLAG_PRIMARY_KEY | COL_FLAG_UNIQUE;
            col->flags &= ~COL_FLAG_NULLABLE;
            log_msg(LOG_DEBUG, "parse_column_def: Column '%s' PRIMARY KEY", col->name);
            advance();
            advance();
        } else if (current_token->type == TOKEN_KEY && current_token[-1].type != TOKEN_PRIMARY) {
            log_msg(LOG_WARN, "parse_column_def: KEY without PRIMARY, ignoring");
            advance();
        } else if (current_token->type == TOKEN_KEYWORD &&
                   strcasecmp(current_token->value, "FOREIGN") == 0 &&
                   current_token[1].type == TOKEN_KEY) {
            advance();
            advance();
            if (current_token->type == TOKEN_KEYWORD &&
                strcasecmp(current_token->value, "REFERENCES") == 0) {
                advance();
                if (current_token->type == TOKEN_IDENTIFIER) {
                    size_t table_name_len = strlen(current_token->value);
                    size_t copy_len = table_name_len < MAX_TABLE_NAME_LEN - 1
                                          ? table_name_len
                                          : MAX_TABLE_NAME_LEN - 1;
                    memcpy(col->references_table, current_token->value, copy_len);
                    col->references_table[copy_len] = '\0';
                    advance();
                    if (current_token->type == TOKEN_LPAREN) {
                        advance();
                        if (current_token->type == TOKEN_IDENTIFIER) {
                            size_t col_name_len = strlen(current_token->value);
                            size_t copy_len = col_name_len < MAX_COLUMN_NAME_LEN - 1
                                                  ? col_name_len
                                                  : MAX_COLUMN_NAME_LEN - 1;
                            memcpy(col->references_column, current_token->value, copy_len);
                            col->references_column[copy_len] = '\0';
                            advance();
                            if (!expect(ctx, TOKEN_RPAREN, "FOREIGN KEY REFERENCES")) {
                                return false;
                            }
                        } else {
                            parse_error_set(ctx, PARSE_ERROR_UNEXPECTED_TOKEN,
                                            "Expected column name in FOREIGN KEY REFERENCES",
                                            "column name", token_type_name(current_token->type),
                                            NULL);
                            return false;
                        }
                    }
                    col->flags |= COL_FLAG_FOREIGN_KEY;
                    log_msg(LOG_DEBUG, "parse_column_def: Column '%s' FOREIGN KEY REFERENCES %s.%s",
                            col->name, col->references_table, col->references_column);
                } else {
                    parse_error_set(ctx, PARSE_ERROR_UNEXPECTED_TOKEN,
                                    "Expected table name in FOREIGN KEY REFERENCES", "table name",
                                    token_type_name(current_token->type), NULL);
                    return false;
                }
            } else {
                parse_error_set(ctx, PARSE_ERROR_UNEXPECTED_TOKEN,
                                "Expected REFERENCES after FOREIGN KEY", "REFERENCES",
                                token_type_name(current_token->type), NULL);
                return false;
            }
        } else if (current_token->type == TOKEN_KEYWORD) {
            log_msg(LOG_WARN, "parse_column_def: Unknown keyword '%s', skipping",
                    current_token->value);
            advance();
        } else {
            break;
        }
    }

    log_msg(LOG_DEBUG, "parse_column_def: Done with column '%s'", col->name);

    return true;
}

static ASTNode* parse_create_table(ParseContext* ctx) {
    log_msg(LOG_DEBUG, "parse_create_table: Starting CREATE TABLE parsing");

    if (!expect(ctx, TOKEN_IDENTIFIER, "CREATE TABLE")) {
        return NULL;
    }

    /* Allocate an AST node for the CREATE TABLE statement.
     * This node will be freed by free_ast() after IR generation.
     * memset ensures all union members and pointers are NULL/zero. */
    ASTNode* node = malloc(sizeof(ASTNode));
    if (!node) {
        parse_error_set(ctx, PARSE_ERROR_INVALID_SYNTAX,
                        "Memory allocation failed for CREATE TABLE node", "memory", "NULL",
                        "Try again or reduce query complexity");
        return NULL;
    }

    memset(node, 0, sizeof(ASTNode));
    node->type = AST_CREATE_TABLE;
    node->next = NULL;

    char* dest = node->create_table.table_name;
    const char* src = current_token[-1].value;
    size_t copy_len = strlen(src);
    if (copy_len >= MAX_TABLE_NAME_LEN) copy_len = MAX_TABLE_NAME_LEN - 1;
    memcpy(dest, src, copy_len);
    dest[copy_len] = '\0';

    log_msg(LOG_DEBUG, "parse_create_table: Table name = '%s'", node->create_table.table_name);

    if (!expect(ctx, TOKEN_LPAREN, "CREATE TABLE")) {
        free(node);
        return NULL;
    }

    log_msg(LOG_DEBUG, "parse_create_table: Starting column parsing");

    alist_init(&node->create_table.columns, sizeof(ColumnDef), NULL);

    int col_count = 0;
    while (!match(TOKEN_RPAREN)) {
        ColumnDef* col = (ColumnDef*)alist_append(&node->create_table.columns);
        if (!col) {
            parse_error_set(ctx, PARSE_ERROR_INVALID_SYNTAX, "Memory allocation failed for columns",
                            "memory", "NULL", NULL);
            free(node);
            return NULL;
        }

        if (!parse_column_def(ctx, col)) {
            log_msg(LOG_ERROR, "parse_create_table: Failed to column %d", col_count);
            free(node);
            return NULL;
        }
        col_count++;

        if (!consume(ctx, TOKEN_COMMA)) {
            break;
        }
    }

    if (!expect(ctx, TOKEN_RPAREN, "CREATE TABLE")) {
        free(node);
        return NULL;
    }

    node->create_table.strict = false;
    if (match(TOKEN_STRICT)) {
        node->create_table.strict = true;
        log_msg(LOG_DEBUG, "parse_create_table: STRICT mode enabled");
    }

    log_msg(LOG_DEBUG, "parse_create_table: Successfully parsed CREATE TABLE with %d columns",
            col_count);
    return node;
}

static Value parse_value(ParseContext* ctx) {
    (void)ctx;
    Value val;
    val.type = TYPE_ERROR;

    if (match(TOKEN_STRING)) {
        log_msg(LOG_DEBUG, "parse_value: Parsing string value '%s'", current_token->value);
        val.type = TYPE_STRING;
        val.char_val = malloc(strlen(current_token->value) + 1);
        strcpy(val.char_val, current_token->value);
        advance();
        return val;
    } else if (match(TOKEN_NUMBER)) {
        log_msg(LOG_DEBUG, "parse_value: Parsing number value '%s'", current_token->value);
        if (strchr(current_token->value, '.')) {
            val.type = TYPE_FLOAT;
            val.float_val = atof(current_token->value);
        } else {
            val.type = TYPE_INT;
            val.int_val = atoi(current_token->value);
        }
        advance();
        return val;
    } else if (match(TOKEN_NULL)) {
        log_msg(LOG_DEBUG, "parse_value: Parsing NULL value");
        val.type = TYPE_NULL;
        advance();
        return val;
    } else if (match(TOKEN_KEYWORD) && strcasecmp(current_token->value, "NULL") == 0) {
        log_msg(LOG_DEBUG, "parse_value: Parsing NULL value");
        val.type = TYPE_NULL;
        advance();
        return val;
    } else if (match(TOKEN_DATE)) {
        log_msg(LOG_DEBUG, "parse_value: Parsing date value '%s'", current_token->value);
        val.type = TYPE_DATE;
        int year, month, day;
        if (sscanf(current_token->value, "%d-%d-%d", &year, &month, &day) == 3) {
            val.date_val.date_val = make_date(year, month, day);
        } else {
            val.date_val.date_val = 0;
        }
        advance();
        return val;
    } else if (match(TOKEN_TIME)) {
        log_msg(LOG_DEBUG, "parse_value: Parsing time value '%s'", current_token->value);
        val.type = TYPE_TIME;
        int hour, minute, second;
        if (sscanf(current_token->value, "%d:%d:%d", &hour, &minute, &second) == 3) {
            val.time_val.time_val = make_time(hour, minute, second);
        } else {
            val.time_val.time_val = 0;
        }
        advance();
        return val;
    }

    log_msg(LOG_WARN, "parse_value: Unknown value type, defaulting to empty string");
    val.type = TYPE_STRING;
    val.char_val = malloc(1);
    strcpy(val.char_val, "");
    return val;
}

static Expr* parse_aggregate_func(ParseContext* ctx) {
    log_msg(LOG_DEBUG, "parse_aggregate_func: Parsing '%s'", current_token->value);

    Expr* expr = malloc(sizeof(Expr));
    if (!expr) {
        parse_error_set(ctx, PARSE_ERROR_INVALID_SYNTAX,
                        "Memory allocation failed for aggregate function", "expression", "NULL",
                        "Try again or simplify your query");
        return NULL;
    }

    expr->type = EXPR_AGGREGATE_FUNC;

    if (strcasecmp(current_token->value, "COUNT") == 0) {
        expr->aggregate.func_type = FUNC_COUNT;
    } else if (strcasecmp(current_token->value, "SUM") == 0) {
        expr->aggregate.func_type = FUNC_SUM;
    } else if (strcasecmp(current_token->value, "AVG") == 0) {
        expr->aggregate.func_type = FUNC_AVG;
    } else if (strcasecmp(current_token->value, "MIN") == 0) {
        expr->aggregate.func_type = FUNC_MIN;
    } else if (strcasecmp(current_token->value, "MAX") == 0) {
        expr->aggregate.func_type = FUNC_MAX;
    } else {
        expr->aggregate.func_type = FUNC_COUNT;
    }
    advance();

    if (!expect(ctx, TOKEN_LPAREN, "aggregate function")) {
        free(expr);
        return NULL;
    }

    if (expr->aggregate.func_type == FUNC_COUNT && match(TOKEN_OPERATOR) &&
        strcmp(current_token->value, "*") == 0) {
        log_msg(LOG_DEBUG, "parse_aggregate_func: COUNT(*) detected");
        expr->aggregate.count_all = true;
        expr->aggregate.distinct = false;
        expr->aggregate.operand = NULL;
        advance();
    } else {
        if (match(TOKEN_DISTINCT)) {
            log_msg(LOG_DEBUG, "parse_aggregate_func: DISTINCT keyword detected");
            expr->aggregate.distinct = true;
            advance();
        } else {
            expr->aggregate.distinct = false;
        }

        log_msg(LOG_DEBUG, "parse_aggregate_func: Parsing aggregate operand");
        expr->aggregate.operand = parse_or_expr(ctx);
        if (!expr->aggregate.operand) {
            parse_error_set(ctx, PARSE_ERROR_INVALID_SYNTAX,
                            "Failed to parse aggregate function argument",
                            "column name, expression, or *",
                            current_token->type == TOKEN_EOF ? "end of input"
                                                             : token_type_name(current_token->type),
                            get_context_suggestion("aggregate function", TOKEN_ERROR));
            free(expr);
            return NULL;
        }
        expr->aggregate.count_all = false;
    }

    if (!expect(ctx, TOKEN_RPAREN, "aggregate function")) {
        if (expr->aggregate.operand) {
            free(expr->aggregate.operand);
        }
        free(expr);
        return NULL;
    }

    log_msg(LOG_DEBUG, "parse_scalar_func: Successfully parsed aggregate function");
    return expr;
}

static Expr* parse_scalar_func(ParseContext* ctx) {
    Expr* expr = malloc(sizeof(Expr));
    if (!expr) {
        parse_error_set(ctx, PARSE_ERROR_INVALID_SYNTAX,
                        "Memory allocation failed for scalar function", "expression", "NULL",
                        "Try again or simplify your query");
        return NULL;
    }

    memset(expr, 0, sizeof(Expr));
    expr->type = EXPR_SCALAR_FUNC;

    if (strcasecmp(current_token->value, "ABS") == 0) {
        expr->scalar.func_type = FUNC_ABS;
    } else if (strcasecmp(current_token->value, "SQRT") == 0) {
        expr->scalar.func_type = FUNC_SQRT;
    } else if (strcasecmp(current_token->value, "MOD") == 0) {
        expr->scalar.func_type = FUNC_MOD;
    } else if (strcasecmp(current_token->value, "POWER") == 0) {
        expr->scalar.func_type = FUNC_POW;
    } else if (strcasecmp(current_token->value, "ROUND") == 0) {
        expr->scalar.func_type = FUNC_ROUND;
    } else if (strcasecmp(current_token->value, "FLOOR") == 0) {
        expr->scalar.func_type = FUNC_FLOOR;
    } else if (strcasecmp(current_token->value, "CEIL") == 0 ||
               strcasecmp(current_token->value, "CEILING") == 0) {
        expr->scalar.func_type = FUNC_CEIL;
    } else if (strcasecmp(current_token->value, "UPPER") == 0) {
        expr->scalar.func_type = FUNC_UPPER;
    } else if (strcasecmp(current_token->value, "LOWER") == 0) {
        expr->scalar.func_type = FUNC_LOWER;
    } else if (strcasecmp(current_token->value, "LENGTH") == 0 ||
               strcasecmp(current_token->value, "LEN") == 0) {
        expr->scalar.func_type = FUNC_LEN;
    } else if (strcasecmp(current_token->value, "MID") == 0 ||
               strcasecmp(current_token->value, "SUBSTRING") == 0) {
        expr->scalar.func_type = FUNC_MID;
    } else if (strcasecmp(current_token->value, "LEFT") == 0) {
        expr->scalar.func_type = FUNC_LEFT;
    } else if (strcasecmp(current_token->value, "RIGHT") == 0) {
        expr->scalar.func_type = FUNC_RIGHT;
    } else if (strcasecmp(current_token->value, "CONCAT") == 0) {
        expr->scalar.func_type = FUNC_CONCAT;
    } else {
        log_msg(LOG_DEBUG, "parse_scalar_func: Unknown scalar function '%s'", current_token->value);
        free(expr);
        return NULL;
    }

    advance();

    if (!expect(ctx, TOKEN_LPAREN, "scalar function")) {
        free(expr);
        return NULL;
    }

    expr->scalar.arg_count = 0;

    if (match(TOKEN_RPAREN)) {
        log_msg(LOG_DEBUG, "parse_scalar_func: Empty arguments for function");
        return expr;
    }

    while (expr->scalar.arg_count < 3 && !match(TOKEN_RPAREN)) {
        if (expr->scalar.arg_count > 0) {
            if (!expect(ctx, TOKEN_COMMA, "scalar function arguments")) {
                free(expr);
                return NULL;
            }
        }
        Expr* arg = parse_or_expr(ctx);
        if (!arg) {
            free(expr);
            return NULL;
        }
        expr->scalar.args[expr->scalar.arg_count++] = arg;
    }

    if (!expect(ctx, TOKEN_RPAREN, "scalar function")) {
        free(expr);
        return NULL;
    }

    return expr;
}

static Expr* parse_primary(ParseContext* ctx) {
    Expr* expr = malloc(sizeof(Expr));
    if (!expr) {
        parse_error_set(ctx, PARSE_ERROR_INVALID_SYNTAX,
                        "Memory allocation failed for primary expression", "expression", "NULL",
                        "Try again or simplify your query");
        return NULL;
    }

    if (match(TOKEN_IDENTIFIER)) {
        log_msg(LOG_DEBUG, "parse_primary: Parsing identifier '%s'", current_token->value);
        expr->type = EXPR_COLUMN;
        strncpy(expr->column_name, current_token->value, MAX_COLUMN_NAME_LEN - 1);
        expr->column_name[MAX_COLUMN_NAME_LEN - 1] = '\0';
        advance();
    } else if (match(TOKEN_STRING) || match(TOKEN_NUMBER) || match(TOKEN_DATE) ||
               match(TOKEN_TIME)) {
        log_msg(LOG_DEBUG, "parse_primary: Parsing literal value '%s'", current_token->value);
        expr->type = EXPR_VALUE;
        if (match(TOKEN_DATE)) {
            expr->value.type = TYPE_DATE;
            int year, month, day;
            if (sscanf(current_token->value, "%d-%d-%d", &year, &month, &day) == 3) {
                expr->value.date_val.date_val = make_date(year, month, day);
            } else {
                expr->value.date_val.date_val = 0;
            }
        } else if (match(TOKEN_TIME)) {
            expr->value.type = TYPE_TIME;
            int hour, minute, second;
            if (sscanf(current_token->value, "%d:%d:%d", &hour, &minute, &second) == 3) {
                expr->value.time_val.time_val = make_time(hour, minute, second);
            } else {
                expr->value.time_val.time_val = 0;
            }
        } else if (match(TOKEN_STRING)) {
            expr->value.type = TYPE_STRING;
            expr->value.char_val = malloc(strlen(current_token->value) + 1);
            strcpy(expr->value.char_val, current_token->value);
        } else {
            if (strchr(current_token->value, '.')) {
                expr->value.type = TYPE_FLOAT;
                expr->value.float_val = atof(current_token->value);
            } else {
                expr->value.type = TYPE_INT;
                expr->value.int_val = atoi(current_token->value);
            }
        }
        advance();
    } else if (match(TOKEN_KEYWORD) && strcasecmp(current_token->value, "NULL") == 0) {
        log_msg(LOG_DEBUG, "parse_primary: Parsing NULL keyword");
        expr->type = EXPR_VALUE;
        expr->value.char_val = malloc(5);
        strcpy(expr->value.char_val, "NULL");
        advance();
    } else if (match(TOKEN_AGGREGATE_FUNC)) {
        free(expr);
        return parse_aggregate_func(ctx);
    } else if (match(TOKEN_SCALAR_FUNC) || match(TOKEN_LEFT)) {
        free(expr);
        return parse_scalar_func(ctx);
    } else if (match(TOKEN_LPAREN)) {
        log_msg(LOG_DEBUG, "parse_primary: Parsing parenthesized expression");
        advance();
        expr = parse_or_expr(ctx);
        if (!expr) {
            parse_error_set(ctx, PARSE_ERROR_INVALID_SYNTAX,
                            "Failed to parse expression inside parentheses", "valid expression",
                            current_token->type == TOKEN_EOF ? "end of input"
                                                             : token_type_name(current_token->type),
                            get_context_suggestion("expression", TOKEN_ERROR));
            return NULL;
        }
        if (!expect(ctx, TOKEN_RPAREN, "expression")) {
            free(expr);
            return NULL;
        }
    } else {
        log_msg(LOG_WARN, "parse_primary: Unknown token type %d", current_token->type);
        free(expr);

        char expected[256];
        snprintf(expected, sizeof(expected),
                 "identifier, string, number, date, time, NULL, or aggregate function");
        parse_error_set(ctx, PARSE_ERROR_UNEXPECTED_TOKEN, "Expected a value or column name",
                        expected,
                        current_token->type == TOKEN_EOF ? "end of input"
                                                         : token_type_name(current_token->type),
                        get_context_suggestion("expression", TOKEN_ERROR));
        return NULL;
    }

    return expr;
}

static Expr* parse_unary_expr(ParseContext* ctx) {
    if (match(TOKEN_NOT)) {
        log_msg(LOG_DEBUG, "parse_unary_expr: Parsing NOT expression");
        Expr* expr = malloc(sizeof(Expr));
        expr->type = EXPR_UNARY_OP;
        expr->unary.op = OP_NOT;
        advance();
        expr->unary.operand = parse_unary_expr(ctx);
        if (!expr->unary.operand) {
            free(expr);
            return NULL;
        }
        return expr;
    }
    if (match(TOKEN_EXISTS)) {
        log_msg(LOG_DEBUG, "parse_unary_expr: Parsing EXISTS expression");
        Expr* expr = malloc(sizeof(Expr));
        expr->type = EXPR_SUBQUERY;
        advance();
        if (!expect(ctx, TOKEN_LPAREN, "EXISTS")) {
            free(expr);
            return NULL;
        }
        Expr* subquery_expr = parse_subquery(ctx);
        if (!subquery_expr) {
            free(expr);
            return NULL;
        }
        expr->subquery.subquery = subquery_expr->subquery.subquery;
        free(subquery_expr);
        if (!expect(ctx, TOKEN_RPAREN, "EXISTS")) {
            free(expr);
            return NULL;
        }
        return expr;
    }
    return parse_primary(ctx);
}

static Expr* parse_subquery(ParseContext* ctx) {
    log_msg(LOG_DEBUG, "parse_subquery: Starting subquery parsing");

    if (!match(TOKEN_KEYWORD) || strcasecmp(current_token->value, "SELECT") != 0) {
        log_msg(LOG_WARN, "parse_subquery: Expected SELECT keyword");
        parse_error_set(ctx, PARSE_ERROR_UNEXPECTED_TOKEN, "Expected SELECT in subquery", "SELECT",
                        current_token->value, "subquery");
        return NULL;
    }

    advance();
    ASTNode* subquery_ast = parse_select(ctx);
    if (!subquery_ast) {
        log_msg(LOG_WARN, "parse_subquery: Failed to parse subquery AST");
        return NULL;
    }

    if (subquery_ast->type != AST_SELECT) {
        log_msg(LOG_WARN, "parse_subquery: Subquery must be a SELECT statement");
        free_ast(subquery_ast);
        return NULL;
    }

    Expr* expr = malloc(sizeof(Expr));
    if (!expr) {
        log_msg(LOG_ERROR, "parse_subquery: Memory allocation failed");
        free_ast(subquery_ast);
        return NULL;
    }

    expr->type = EXPR_SUBQUERY;
    expr->subquery.subquery = subquery_ast;

    log_msg(LOG_DEBUG, "parse_subquery: Subquery parsed successfully");
    return expr;
}

static Expr* parse_comparison_expr(ParseContext* ctx) {
    log_msg(LOG_DEBUG, "parse_comparison_expr: Starting comparison expression parsing");

    Expr* left = parse_unary_expr(ctx);
    if (!left) return NULL;

    while (match(TOKEN_EQUALS) || match(TOKEN_NOT_EQUALS) || match(TOKEN_LESS) ||
           match(TOKEN_LESS_EQUAL) || match(TOKEN_GREATER) || match(TOKEN_GREATER_EQUAL) ||
           match(TOKEN_LIKE)) {
        Expr* expr = malloc(sizeof(Expr));
        expr->type = EXPR_BINARY_OP;
        OperatorType op;

        if (match(TOKEN_EQUALS)) {
            op = OP_EQUALS;
            log_msg(LOG_DEBUG, "parse_comparison_expr: Found = operator");
            advance();
        } else if (match(TOKEN_NOT_EQUALS)) {
            op = OP_NOT_EQUALS;
            log_msg(LOG_DEBUG, "parse_comparison_expr: Found != operator");
            advance();
        } else if (match(TOKEN_LESS)) {
            op = OP_LESS;
            log_msg(LOG_DEBUG, "parse_comparison_expr: Found < operator");
            advance();
        } else if (match(TOKEN_LESS_EQUAL)) {
            op = OP_LESS_EQUAL;
            log_msg(LOG_DEBUG, "parse_comparison_expr: Found <= operator");
            advance();
        } else if (match(TOKEN_GREATER)) {
            op = OP_GREATER;
            log_msg(LOG_DEBUG, "parse_comparison_expr: Found > operator");
            advance();
        } else if (match(TOKEN_GREATER_EQUAL)) {
            op = OP_GREATER_EQUAL;
            log_msg(LOG_DEBUG, "parse_comparison_expr: Found >= operator");
            advance();
        } else {
            op = OP_LIKE;
            log_msg(LOG_DEBUG, "parse_comparison_expr: Found LIKE operator");
            advance();
        }

        expr->binary.op = op;
        expr->binary.left = left;
        expr->binary.right = parse_unary_expr(ctx);
        if (!expr->binary.right) {
            free(expr);
            free(left);
            return NULL;
        }
        left = expr;
    }

    return left;
}

static Expr* parse_and_expr(ParseContext* ctx) {
    log_msg(LOG_DEBUG, "parse_and_expr: Starting AND expression parsing");

    Expr* left = parse_comparison_expr(ctx);
    if (!left) return NULL;

    while (match(TOKEN_AND)) {
        log_msg(LOG_DEBUG, "parse_and_expr: Found AND operator");
        Expr* expr = malloc(sizeof(Expr));
        expr->type = EXPR_BINARY_OP;
        expr->binary.op = OP_AND;
        advance();
        expr->binary.left = left;
        expr->binary.right = parse_comparison_expr(ctx);
        if (!expr->binary.right) {
            free(expr);
            free(left);
            return NULL;
        }
        left = expr;
    }

    return left;
}

static Expr* parse_or_expr(ParseContext* ctx) {
    log_msg(LOG_DEBUG, "parse_or_expr: Starting OR expression parsing");

    Expr* left = parse_and_expr(ctx);
    if (!left) return NULL;

    while (match(TOKEN_OR)) {
        log_msg(LOG_DEBUG, "parse_or_expr: Found OR operator");
        Expr* expr = malloc(sizeof(Expr));
        expr->type = EXPR_BINARY_OP;
        expr->binary.op = OP_OR;
        advance();
        expr->binary.left = left;
        expr->binary.right = parse_and_expr(ctx);
        if (!expr->binary.right) {
            free(expr);
            free(left);
            return NULL;
        }
        left = expr;
    }

    return left;
}

static Expr* parse_where_clause(ParseContext* ctx) {
    if (match(TOKEN_KEYWORD) && strcasecmp(current_token->value, "WHERE") == 0) {
        log_msg(LOG_DEBUG, "parse_where_clause: Found WHERE keyword");
        advance();
        return parse_or_expr(ctx);
    }
    return NULL;
}

static ASTNode* parse_insert(ParseContext* ctx) {
    log_msg(LOG_DEBUG, "parse_insert: Starting INSERT parsing");

    if (!expect(ctx, TOKEN_KEYWORD, "INSERT") || strcasecmp(current_token[-1].value, "INTO") != 0) {
        parse_error_set(ctx, PARSE_ERROR_UNEXPECTED_TOKEN, "Expected 'INTO' keyword after INSERT",
                        "INTO keyword",
                        current_token->type == TOKEN_EOF ? "end of input" : current_token->value,
                        "INSERT syntax: INSERT INTO table_name VALUES (...)\n"
                        "Example: INSERT INTO users VALUES ('John', 25)");
        return NULL;
    }

    if (!expect(ctx, TOKEN_IDENTIFIER, "INSERT")) {
        return NULL;
    }

    ASTNode* node = malloc(sizeof(ASTNode));
    if (!node) {
        parse_error_set(ctx, PARSE_ERROR_INVALID_SYNTAX, "Memory allocation failed for INSERT node",
                        "node", "NULL", "Try again");
        return NULL;
    }

    memset(node, 0, sizeof(ASTNode));
    node->type = AST_INSERT_ROW;
    node->next = NULL;

    const char* table_name = current_token[-1].value;
    Table* table = find_table(table_name);
    node->insert.table_id = table ? table->table_id : -1;

    log_msg(LOG_DEBUG, "parse_insert: Table name = '%s', table_id = %d", table_name, node->insert.table_id);

    if (match(TOKEN_KEYWORD) && strcasecmp(current_token->value, "VALUES") == 0) {
        log_msg(LOG_DEBUG, "parse_insert: Found VALUES keyword");
        advance();
    }

    if (!expect(ctx, TOKEN_LPAREN, "INSERT")) {
        free(node);
        return NULL;
    }

    log_msg(LOG_DEBUG, "parse_insert: Starting value parsing");

    alist_init(&node->insert.values, sizeof(ColumnValue), free_column_value);

    while (!match(TOKEN_RPAREN)) {
        ColumnValue* cv = (ColumnValue*)alist_append(&node->insert.values);
        if (!cv) {
            parse_error_set(ctx, PARSE_ERROR_INVALID_SYNTAX, "Memory allocation failed for values",
                            "memory", "NULL", NULL);
            free(node);
            return NULL;
        }
        cv->value = parse_value(ctx);
        cv->column_name[0] = '\0';

        if (!consume(ctx, TOKEN_COMMA)) {
            log_msg(LOG_DEBUG, "parse_insert: No more commas");
            break;
        }
    }

    if (!expect(ctx, TOKEN_RPAREN, "INSERT")) {
        free(node);
        return NULL;
    }

    log_msg(LOG_DEBUG, "parse_insert: Successfully parsed INSERT with %d values",
            alist_length(&node->insert.values));
    return node;
}

static ASTNode* parse_select(ParseContext* ctx) {
    log_msg(LOG_DEBUG, "parse_select: Starting SELECT parsing");

    ASTNode* node = malloc(sizeof(ASTNode));
    if (!node) {
        parse_error_set(ctx, PARSE_ERROR_INVALID_SYNTAX, "Memory allocation failed for SELECT node",
                        "node", "NULL", "Try again");
        return NULL;
    }

    memset(node, 0, sizeof(ASTNode));
    node->type = AST_SELECT;
    node->next = NULL;

    alist_init(&node->select.expressions, sizeof(Expr*), NULL);

    if (match(TOKEN_OPERATOR) && strcmp(current_token->value, "*") == 0) {
        log_msg(LOG_DEBUG, "parse_select: Found * (all columns)");
        Expr* star_expr = malloc(sizeof(Expr));
        if (star_expr) {
            star_expr->type = EXPR_VALUE;
            star_expr->alias[0] = '\0';
            star_expr->value.char_val = malloc(2);
            strcpy(star_expr->value.char_val, "*");
        }
        if (match(TOKEN_AS)) {
            advance();
            if (match(TOKEN_IDENTIFIER)) {
                strncpy(star_expr->alias, current_token->value, MAX_COLUMN_NAME_LEN - 1);
                star_expr->alias[MAX_COLUMN_NAME_LEN - 1] = '\0';
                advance();
            }
        }
        Expr** expr_ptr = (Expr**)alist_append(&node->select.expressions);
        if (expr_ptr) *expr_ptr = star_expr;
        advance();
    } else {
        while (true) {
            if (match(TOKEN_IDENTIFIER) ||
                (match(TOKEN_KEYWORD) && strcasecmp(current_token->value, "FROM") != 0) ||
                match(TOKEN_AGGREGATE_FUNC) || match(TOKEN_SCALAR_FUNC) || match(TOKEN_LEFT)) {
                Expr* expr = parse_or_expr(ctx);
                if (!expr) {
                    log_msg(LOG_ERROR, "parse_select: Failed to parse expression");
                    free(node);
                    return NULL;
                }

                if (match(TOKEN_AS) ||
                    (match(TOKEN_KEYWORD) && strcasecmp(current_token->value, "AS") == 0)) {
                    advance();
                    if (match(TOKEN_IDENTIFIER)) {
                        strncpy(expr->alias, current_token->value, MAX_COLUMN_NAME_LEN - 1);
                        expr->alias[MAX_COLUMN_NAME_LEN - 1] = '\0';
                        advance();
                    }
                }

                Expr** expr_ptr = (Expr**)alist_append(&node->select.expressions);
                if (expr_ptr) *expr_ptr = expr;
            } else {
                log_msg(LOG_DEBUG, "parse_select: No more expressions");
                break;
            }

            if (!consume(ctx, TOKEN_COMMA)) {
                log_msg(LOG_DEBUG, "parse_select: No more commas");
                break;
            }
        }
    }

    if (alist_length(&node->select.expressions) == 0) {
        parse_error_set(ctx, PARSE_ERROR_INVALID_SYNTAX,
                        "Expected at least one column or * in SELECT",
                        "column name, *, or aggregate function",
                        current_token->type == TOKEN_EOF ? "end of input"
                                                         : token_type_name(current_token->type),
                        "SELECT syntax: SELECT columns FROM table\n"
                        "Examples:\n"
                        "  SELECT * FROM users\n"
                        "  SELECT name, age FROM users");
        free(node);
        return NULL;
    }

    if (!expect(ctx, TOKEN_KEYWORD, "SELECT") || strcasecmp(current_token[-1].value, "FROM") != 0) {
        if (current_token->type == TOKEN_EOF) {
            parse_error_set(ctx, PARSE_ERROR_UNEXPECTED_END,
                            "Missing 'FROM' keyword after column list", "FROM keyword",
                            "end of input",
                            "SELECT syntax:\n"
                            "  SELECT columns FROM table_name [WHERE condition]\n\n"
                            "After listing columns, you need:\n"
                            "  FROM keyword (required)\n"
                            "  Table name (required)\n"
                            "  Optional WHERE clause to filter results\n\n"
                            "Examples:\n"
                            "  SELECT * FROM users\n"
                            "  SELECT name, age FROM users\n"
                            "  SELECT * FROM users WHERE age > 18");
        } else {
            parse_error_set(ctx, PARSE_ERROR_UNEXPECTED_TOKEN,
                            "Expected 'FROM' keyword after column list", "FROM keyword",
                            current_token->value,
                            "Did you forget the FROM keyword?\n"
                            "  SELECT columns FROM table_name");
        }
        free(node);
        return NULL;
    }

    if (!expect(ctx, TOKEN_IDENTIFIER, "SELECT")) {
        if (current_token->type == TOKEN_EOF) {
            parse_error_set(ctx, PARSE_ERROR_UNEXPECTED_END, "Missing table name after FROM",
                            "table name (IDENTIFIER)", "end of input",
                            "SELECT syntax:\n"
                            "  SELECT columns FROM table_name [WHERE condition]\n\n"
                            "You need to provide:\n"
                            "  1. A table name after FROM (required)\n"
                            "  2. Optional WHERE clause to filter results\n\n"
                            "Examples:\n"
                            "  SELECT * FROM users\n"
                            "  SELECT name, age FROM users\n"
                            "  SELECT * FROM users WHERE age > 18");
        }
        free(node);
        return NULL;
    }

    const char* table_name = current_token[-1].value;
    Table* table = find_table(table_name);
    if (!table) {
        suggest_similar_table(ctx, table_name);
        parse_error_set(ctx, PARSE_ERROR_UNEXPECTED_TOKEN, "Table not found",
                        "table name", table_name,
                        ctx->error.suggestion[0] ? ctx->error.suggestion : "No tables found with that name, try creating a table with CREATE TABLE.");
        free(node);
        return NULL;
    }
    node->select.table_id = table->table_id;
    node->select.join_type = JOIN_NONE;
    node->select.join_table_id = -1;
    node->select.join_condition = NULL;

    log_msg(LOG_DEBUG, "parse_select: Table name = '%s', table_id = %d", table_name, node->select.table_id);

    if (match(TOKEN_JOIN)) {
        log_msg(LOG_DEBUG, "parse_select: Found JOIN keyword");
        advance();

        node->select.join_type = JOIN_INNER;
        if (match(TOKEN_LEFT)) {
            node->select.join_type = JOIN_LEFT;
            advance();
        } else if (match(TOKEN_INNER)) {
            advance();
        }

        if (!match(TOKEN_IDENTIFIER)) {
            parse_error_set(ctx, PARSE_ERROR_UNEXPECTED_TOKEN, "Expected table name after JOIN",
                            "table name (IDENTIFIER)",
                            current_token->type == TOKEN_EOF ? "end of input" : token_type_name(current_token->type),
                            "Use: SELECT ... FROM table1 JOIN table2 ON condition\n"
                            "     SELECT ... FROM table1 INNER JOIN table2 ON condition\n"
                            "     SELECT ... FROM table1 LEFT JOIN table2 ON condition");
            free(node);
            return NULL;
        }

        const char* join_table_name = current_token->value;
        Table* join_table = find_table(join_table_name);
        if (!join_table) {
            suggest_similar_table(ctx, join_table_name);
            parse_error_set(ctx, PARSE_ERROR_UNEXPECTED_TOKEN, "Table not found in JOIN",
                            "table name", join_table_name,
                            ctx->error.suggestion[0] ? ctx->error.suggestion : "No tables found with that name");
            free(node);
            return NULL;
        }
        node->select.join_table_id = join_table->table_id;
        size_t len = strlen(join_table_name);
        if (len >= MAX_TABLE_NAME_LEN) len = MAX_TABLE_NAME_LEN - 1;
        memcpy(node->select.join_table_name, join_table_name, len);
        node->select.join_table_name[len] = '\0';
        advance();

        if (!match(TOKEN_KEYWORD) || strcasecmp(current_token->value, "ON") != 0) {
            parse_error_set(ctx, PARSE_ERROR_UNEXPECTED_TOKEN, "Expected 'ON' keyword after JOIN table",
                            "ON keyword",
                            current_token->type == TOKEN_EOF ? "end of input" : token_type_name(current_token->type),
                            "Use: SELECT ... FROM table1 JOIN table2 ON condition");
            free(node);
            return NULL;
        }
        advance();

        node->select.join_condition = parse_or_expr(ctx);
        if (!node->select.join_condition) {
            log_msg(LOG_ERROR, "parse_select: Failed to parse JOIN condition");
            free(node);
            return NULL;
        }
    }

    node->select.where_clause = parse_where_clause(ctx);
    node->select.order_by_count = 0;
    node->select.limit = 0;

    alist_init(&node->select.order_by, sizeof(Expr*), NULL);
    alist_init(&node->select.order_by_desc, sizeof(bool), NULL);

    if (current_token->type == TOKEN_ORDER) {
        advance();
        if (current_token->type != TOKEN_BY) {
            parse_error_set(
                ctx, PARSE_ERROR_UNEXPECTED_TOKEN, "Expected 'BY' after 'ORDER'", "BY keyword",
                current_token->type == TOKEN_EOF ? "end of input" : current_token->value,
                "Use ORDER BY column_name [ASC|DESC]");
            free(node);
            return NULL;
        }
        advance();

        while (!match(TOKEN_KEYWORD) || strcasecmp(current_token->value, "LIMIT") != 0) {
            if (match(TOKEN_IDENTIFIER)) {
                Expr* order_expr = parse_primary(ctx);
                if (!order_expr) {
                    free(node);
                    return NULL;
                }
                Expr** expr_ptr = (Expr**)alist_append(&node->select.order_by);
                if (expr_ptr) *expr_ptr = order_expr;
                bool desc = false;
                if (match(TOKEN_KEYWORD) && strcasecmp(current_token->value, "DESC") == 0) {
                    desc = true;
                    advance();
                } else if (match(TOKEN_KEYWORD) && strcasecmp(current_token->value, "ASC") == 0) {
                    advance();
                }
                bool* desc_ptr = (bool*)alist_append(&node->select.order_by_desc);
                if (desc_ptr) *desc_ptr = desc;

                if (!consume(ctx, TOKEN_COMMA)) {
                    break;
                }
            } else {
                break;
            }
        }
    }

    if (match(TOKEN_KEYWORD) && strcasecmp(current_token->value, "LIMIT") == 0) {
        advance();
        if (!match(TOKEN_NUMBER)) {
            parse_error_set(ctx, PARSE_ERROR_UNEXPECTED_TOKEN, "Expected number after LIMIT",
                            "number",
                            current_token->type == TOKEN_EOF ? "end of input"
                                                             : token_type_name(current_token->type),
                            "Use LIMIT n where n is a positive integer");
            free(node);
            return NULL;
        }
        node->select.limit = atoi(current_token->value);
        advance();
    }

    node->select.order_by_count = alist_length(&node->select.order_by);

    log_msg(LOG_DEBUG, "parse_select: Successfully parsed SELECT with %d expressions",
            alist_length(&node->select.expressions));
    return node;
}

static ASTNode* parse_drop_table(ParseContext* ctx) {
    log_msg(LOG_DEBUG, "parse_drop_table: Starting DROP TABLE parsing");

    if (!match(TOKEN_IDENTIFIER)) {
        parse_error_set(ctx, PARSE_ERROR_UNEXPECTED_TOKEN, "Expected table name after DROP TABLE",
                        "table name (IDENTIFIER)",
                        current_token->type == TOKEN_EOF ? "end of input"
                                                         : token_type_name(current_token->type),
                        "DROP TABLE syntax: DROP TABLE table_name\n"
                        "Example: DROP TABLE users");
        return NULL;
    }

    ASTNode* node = malloc(sizeof(ASTNode));
    if (!node) {
        parse_error_set(ctx, PARSE_ERROR_INVALID_SYNTAX,
                        "Memory allocation failed for DROP TABLE node", "node", "NULL",
                        "Try again");
        return NULL;
    }

    memset(node, 0, sizeof(ASTNode));
    node->type = AST_DROP_TABLE;
    node->next = NULL;

    const char* table_name = current_token->value;
    Table* table = find_table(table_name);
    node->drop_table.table_id = table ? table->table_id : -1;

    log_msg(LOG_DEBUG, "parse_drop_table: Table name = '%s', table_id = %d", table_name, node->drop_table.table_id);
    advance();

    return node;
}

static ASTNode* parse_update(ParseContext* ctx) {
    log_msg(LOG_DEBUG, "parse_update: Starting UPDATE parsing");

    if (!expect(ctx, TOKEN_IDENTIFIER, "UPDATE")) {
        return NULL;
    }

    ASTNode* node = malloc(sizeof(ASTNode));
    if (!node) {
        parse_error_set(ctx, PARSE_ERROR_INVALID_SYNTAX, "Memory allocation failed for UPDATE node",
                        "node", "NULL", "Try again");
        return NULL;
    }

    memset(node, 0, sizeof(ASTNode));
    node->type = AST_UPDATE_ROW;
    node->next = NULL;

    const char* table_name = current_token[-1].value;
    Table* table = find_table(table_name);
    node->update.table_id = table ? table->table_id : -1;

    log_msg(LOG_DEBUG, "parse_update: Table name = '%s', table_id = %d", table_name, node->update.table_id);

    if (!expect(ctx, TOKEN_KEYWORD, "UPDATE") || strcasecmp(current_token[-1].value, "SET") != 0) {
        parse_error_set(ctx, PARSE_ERROR_UNEXPECTED_TOKEN,
                        "Expected 'SET' keyword after table name", "SET keyword",
                        current_token->type == TOKEN_EOF ? "end of input" : current_token->value,
                        get_context_suggestion("UPDATE", TOKEN_ERROR));
        free(node);
        return NULL;
    }

    log_msg(LOG_DEBUG, "parse_update: Starting SET clause parsing");

    alist_init(&node->update.values, sizeof(ColumnValue), free_column_value);

    while (true) {
        if (match(TOKEN_KEYWORD) && strcasecmp(current_token->value, "WHERE") == 0) {
            log_msg(LOG_DEBUG, "parse_update: Found WHERE keyword, stopping SET parsing");
            break;
        }

        if (!match(TOKEN_IDENTIFIER)) {
            parse_error_set(ctx, PARSE_ERROR_UNEXPECTED_TOKEN, "Expected column name in SET clause",
                            "column name (IDENTIFIER)",
                            current_token->type == TOKEN_EOF ? "end of input"
                                                             : token_type_name(current_token->type),
                            get_context_suggestion("UPDATE", TOKEN_ERROR));
            free(node);
            return NULL;
        }

        ColumnValue* cv = (ColumnValue*)alist_append(&node->update.values);
        if (!cv) {
            parse_error_set(ctx, PARSE_ERROR_INVALID_SYNTAX, "Memory allocation failed for values",
                            "memory", "NULL", NULL);
            free(node);
            return NULL;
        }

        size_t len = strlen(current_token->value);
        if (len >= MAX_COLUMN_NAME_LEN) len = MAX_COLUMN_NAME_LEN - 1;
        memcpy(cv->column_name, current_token->value, len);
        cv->column_name[len] = '\0';

        log_msg(LOG_DEBUG, "parse_update: Parsing assignment for column '%s'", cv->column_name);
        advance();

        if (!expect(ctx, TOKEN_EQUALS, "UPDATE")) {
            free(node);
            return NULL;
        }

        cv->value = parse_value(ctx);

        if (!consume(ctx, TOKEN_COMMA)) {
            log_msg(LOG_DEBUG, "parse_update: No more commas");
            break;
        }
    }

    if (match(TOKEN_KEYWORD) && strcasecmp(current_token->value, "WHERE") == 0) {
        log_msg(LOG_DEBUG, "parse_update: Found WHERE clause");
        advance();
        node->update.where_clause = parse_or_expr(ctx);
        if (!node->update.where_clause) {
            log_msg(LOG_WARN, "parse_update: Failed to parse WHERE clause");
            node->update.where_clause = NULL;
        }
    } else {
        node->update.where_clause = NULL;
    }

    log_msg(LOG_DEBUG, "parse_update: Successfully parsed UPDATE with %d assignments",
            alist_length(&node->update.values));
    return node;
}

static ASTNode* parse_delete(ParseContext* ctx) {
    log_msg(LOG_DEBUG, "parse_delete: Starting DELETE parsing");

    if (!expect(ctx, TOKEN_KEYWORD, "DELETE") || strcasecmp(current_token[-1].value, "FROM") != 0) {
        parse_error_set(ctx, PARSE_ERROR_UNEXPECTED_TOKEN, "Expected 'FROM' keyword after DELETE",
                        "FROM keyword",
                        current_token->type == TOKEN_EOF ? "end of input" : current_token->value,
                        get_context_suggestion("DELETE", TOKEN_ERROR));
        return NULL;
    }

    if (!expect(ctx, TOKEN_IDENTIFIER, "DELETE")) {
        return NULL;
    }

    ASTNode* node = malloc(sizeof(ASTNode));
    if (!node) {
        parse_error_set(ctx, PARSE_ERROR_INVALID_SYNTAX, "Memory allocation failed for DELETE node",
                        "node", "NULL", "Try again");
        return NULL;
    }

    memset(node, 0, sizeof(ASTNode));
    node->type = AST_DELETE_ROW;
    node->next = NULL;

    const char* table_name = current_token[-1].value;
    Table* table = find_table(table_name);
    node->delete.table_id = table ? table->table_id : -1;

    log_msg(LOG_DEBUG, "parse_delete: Table name = '%s', table_id = %d", table_name, node->delete.table_id);

    if (match(TOKEN_KEYWORD) && strcasecmp(current_token->value, "WHERE") == 0) {
        log_msg(LOG_DEBUG, "parse_delete: Found WHERE clause");
        advance();
        node->delete.where_clause = parse_or_expr(ctx);
        if (!node->delete.where_clause) {
            log_msg(LOG_WARN, "parse_delete: Failed to parse WHERE clause");
            node->delete.where_clause = NULL;
        }
    } else {
        node->delete.where_clause = NULL;
    }

    log_msg(LOG_DEBUG, "parse_delete: Successfully parsed DELETE");
    return node;
}

static ASTNode* parse_create_index(ParseContext* ctx) {
    log_msg(LOG_DEBUG, "parse_create_index: Starting CREATE INDEX parsing");

    /* Note: parse_with_context has already advanced past 'INDEX' keyword,
     * so current_token is now at the index name */

    if (!match(TOKEN_IDENTIFIER)) {
        parse_error_set(ctx, PARSE_ERROR_UNEXPECTED_TOKEN, "Expected index name after 'INDEX'",
                        "index name (IDENTIFIER)", token_type_name(current_token->type),
                        "Syntax: CREATE INDEX index_name ON table_name (column_name)");
        return NULL;
    }

    ASTNode* node = malloc(sizeof(ASTNode));
    if (!node) {
        parse_error_set(ctx, PARSE_ERROR_INVALID_SYNTAX,
                        "Memory allocation failed for CREATE INDEX node", "memory", "NULL",
                        "Try again or reduce query complexity");
        return NULL;
    }

    memset(node, 0, sizeof(ASTNode));
    node->type = AST_CREATE_INDEX;
    node->next = NULL;

    char* dest = node->create_index.index_name;
    const char* src = current_token->value;
    size_t copy_len = strlen(src);
    if (copy_len >= MAX_TABLE_NAME_LEN) copy_len = MAX_TABLE_NAME_LEN - 1;
    memcpy(dest, src, copy_len);
    dest[copy_len] = '\0';

    log_msg(LOG_DEBUG, "parse_create_index: Index name = '%s'", node->create_index.index_name);
    advance();

    if (!match(TOKEN_KEYWORD) || strcasecmp(current_token->value, "ON") != 0) {
        parse_error_set(ctx, PARSE_ERROR_UNEXPECTED_TOKEN, "Expected 'ON' keyword after index name",
                        "ON keyword", current_token->value,
                        "Syntax: CREATE INDEX index_name ON table_name (column_name)");
        free(node);
        return NULL;
    }

    advance(); /* Move past ON keyword */

    if (!match(TOKEN_IDENTIFIER)) {
        parse_error_set(ctx, PARSE_ERROR_UNEXPECTED_TOKEN, "Expected table name after 'ON'",
                        "table name (IDENTIFIER)", token_type_name(current_token->type),
                        "Syntax: CREATE INDEX index_name ON table_name (column_name)");
        free(node);
        return NULL;
    }

    const char* table_name = current_token->value;
    Table* table = find_table(table_name);
    node->create_index.table_id = table ? table->table_id : -1;

    log_msg(LOG_DEBUG, "parse_create_index: Table name = '%s', table_id = %d", table_name, node->create_index.table_id);
    advance();

    if (!match(TOKEN_LPAREN)) {
        parse_error_set(ctx, PARSE_ERROR_UNEXPECTED_TOKEN, "Expected '(' after table name",
                        "LPAREN", token_type_name(current_token->type),
                        "Syntax: CREATE INDEX index_name ON table_name (column_name)");
        free(node);
        return NULL;
    }

    advance(); /* Move past '(' */

    if (!match(TOKEN_IDENTIFIER)) {
        parse_error_set(ctx, PARSE_ERROR_UNEXPECTED_TOKEN,
                        "Expected column name in index definition", "column name (IDENTIFIER)",
                        current_token->value,
                        "Syntax: CREATE INDEX index_name ON table_name (column_name)");
        free(node);
        return NULL;
    }

    char* col_dest = node->create_index.column_name;
    const char* col_src = current_token->value;
    size_t col_copy_len = strlen(col_src);
    if (col_copy_len >= MAX_COLUMN_NAME_LEN) col_copy_len = MAX_COLUMN_NAME_LEN - 1;
    memcpy(col_dest, col_src, col_copy_len);
    col_dest[col_copy_len] = '\0';

    log_msg(LOG_DEBUG, "parse_create_index: Column name = '%s'", node->create_index.column_name);
    advance();

    if (!match(TOKEN_RPAREN)) {
        parse_error_set(ctx, PARSE_ERROR_UNEXPECTED_TOKEN, "Expected ')' after column name",
                        "RPAREN", token_type_name(current_token->type),
                        "Make sure all parentheses are balanced");
        free(node);
        return NULL;
    }

    log_msg(LOG_DEBUG, "parse_create_index: Successfully parsed CREATE INDEX");
    return node;
}

static ASTNode* parse_drop_index(ParseContext* ctx) {
    log_msg(LOG_DEBUG, "parse_drop_index: Starting DROP INDEX parsing");

    if (!match(TOKEN_IDENTIFIER)) {
        parse_error_set(ctx, PARSE_ERROR_UNEXPECTED_TOKEN, "Expected index name after 'DROP INDEX'",
                        "index name (IDENTIFIER)", token_type_name(current_token->type),
                        "Syntax: DROP INDEX index_name");
        return NULL;
    }

    ASTNode* node = malloc(sizeof(ASTNode));
    if (!node) {
        parse_error_set(ctx, PARSE_ERROR_INVALID_SYNTAX,
                        "Memory allocation failed for DROP INDEX node", "memory", "NULL",
                        "Try again or reduce query complexity");
        return NULL;
    }

    memset(node, 0, sizeof(ASTNode));
    node->type = AST_DROP_INDEX;
    node->next = NULL;

    char* dest = node->drop_index.index_name;
    const char* src = current_token->value;
    size_t copy_len = strlen(src);
    if (copy_len >= MAX_TABLE_NAME_LEN) copy_len = MAX_TABLE_NAME_LEN - 1;
    memcpy(dest, src, copy_len);
    dest[copy_len] = '\0';

    log_msg(LOG_DEBUG, "parse_drop_index: Index name = '%s'", node->drop_index.index_name);
    advance();

    log_msg(LOG_DEBUG, "parse_drop_index: Successfully parsed DROP INDEX");
    return node;
}

ASTNode* parse_with_context(ParseContext* ctx, Token* tokens) {
    if (!tokens) {
        parse_error_set(ctx, PARSE_ERROR_INVALID_SYNTAX, "Parse called with NULL tokens",
                        "valid token stream", "NULL",
                        "Ensure tokenization succeeded before parsing");
        log_msg(LOG_WARN, "parse: Parse called with NULL tokens");
        return NULL;
    }

    log_msg(LOG_DEBUG, "parse: Starting parse");
    current_token = tokens;
    token_index = 0;
    column_position = 0;
    input_buffer = ctx->input;

    if (match(TOKEN_KEYWORD)) {
        if (strcasecmp(current_token->value, "CREATE") == 0) {
            log_msg(LOG_DEBUG, "parse: Detected CREATE statement");
            advance();
            if (strcasecmp(current_token->value, "TABLE") == 0) {
                advance();
                log_msg(LOG_DEBUG, "parse: Parsing CREATE TABLE statement");
                return parse_create_table(ctx);
            } else if (strcasecmp(current_token->value, "INDEX") == 0) {
                advance();
                log_msg(LOG_DEBUG, "parse: Parsing CREATE INDEX statement");
                return parse_create_index(ctx);
            } else {
                parse_error_set(ctx, PARSE_ERROR_UNEXPECTED_TOKEN,
                                "Expected 'TABLE' or 'INDEX' keyword after 'CREATE'",
                                "TABLE or INDEX keyword", current_token->value,
                                "Did you mean: CREATE TABLE table_name (...) ?");
                return NULL;
            }
        } else if (strcasecmp(current_token->value, "INSERT") == 0) {
            log_msg(LOG_DEBUG, "parse: Parsing INSERT statement");
            advance();
            return parse_insert(ctx);
        } else if (strcasecmp(current_token->value, "SELECT") == 0) {
            log_msg(LOG_DEBUG, "parse: Parsing SELECT statement");
            advance();
            return parse_select(ctx);
        } else if (strcasecmp(current_token->value, "UPDATE") == 0) {
            log_msg(LOG_DEBUG, "parse: Parsing UPDATE statement");
            advance();
            return parse_update(ctx);
        } else if (strcasecmp(current_token->value, "DELETE") == 0) {
            log_msg(LOG_DEBUG, "parse: Parsing DELETE statement");
            advance();
            return parse_delete(ctx);
        } else if (strcasecmp(current_token->value, "DROP") == 0) {
            log_msg(LOG_DEBUG, "parse: Detected DROP statement");
            advance();
            if (match(TOKEN_KEYWORD) && strcasecmp(current_token->value, "TABLE") == 0) {
                advance();
                log_msg(LOG_DEBUG, "parse: Parsing DROP TABLE statement");
                return parse_drop_table(ctx);
            } else if (match(TOKEN_KEYWORD) && strcasecmp(current_token->value, "INDEX") == 0) {
                advance();
                log_msg(LOG_DEBUG, "parse: Parsing DROP INDEX statement");
                return parse_drop_index(ctx);
            } else {
                parse_error_set(
                    ctx, PARSE_ERROR_UNEXPECTED_TOKEN,
                    "Expected 'TABLE' or 'INDEX' keyword after 'DROP'", "TABLE or INDEX keyword",
                    current_token->type == TOKEN_EOF ? "end of input" : current_token->value,
                    "Did you mean: DROP TABLE table_name ?");
                return NULL;
            }
        } else {
            parse_error_set(ctx, PARSE_ERROR_UNEXPECTED_TOKEN, "Expected a SQL keyword",
                            "CREATE, INSERT, SELECT, UPDATE, DELETE, or DROP", current_token->value,
                            "Check SQL syntax. Common statements:\n"
                            "  CREATE TABLE - Create a new table\n"
                            "  INSERT INTO - Insert rows\n"
                            "  SELECT - Query data\n"
                            "  UPDATE - Modify data\n"
                            "  DELETE - Remove data\n"
                            "  DROP TABLE - Delete a table");
            return NULL;
        }
    }

    parse_error_set(
        ctx, PARSE_ERROR_INVALID_SYNTAX, "Expected a SQL statement",
        "CREATE, INSERT, SELECT, UPDATE, DELETE, or DROP",
        current_token->type == TOKEN_EOF ? "end of input" : token_type_name(current_token->type),
        "SQL statements must start with a keyword like:\n"
        "  CREATE TABLE users (id INT, name STRING)\n"
        "  INSERT INTO users VALUES ('John', 25)\n"
        "  SELECT * FROM users\n"
        "  UPDATE users SET age=30 WHERE name='John'\n"
        "  DELETE FROM users WHERE age < 18\n"
        "  DROP TABLE users");
    return NULL;
}

ASTNode* parse_ex(const char* input, Token* tokens) {
    parse_error_init(&g_parse_context, input, tokens, 0);

    Token* count_tokens = tokens;
    int count = 0;
    if (tokens) {
        while (count_tokens[count].type != TOKEN_EOF && count < 10000) {
            count++;
        }
        count++;
    }
    parse_error_init(&g_parse_context, input, tokens, count);

    ASTNode* result = parse_with_context(&g_parse_context, tokens);

    if (g_parse_context.error_occurred && !result) {
        log_msg(LOG_ERROR, "Parse failed: %s", g_parse_context.error.message);
    }

    return result;
}

ASTNode* parse(Token* tokens) { return parse_ex(NULL, tokens); }

void free_ast(ASTNode* ast) {
    if (!ast) return;
    free_ast(ast->next);

    switch (ast->type) {
        case AST_INSERT_ROW:
            alist_destroy(&ast->insert.values);
            break;
        case AST_CREATE_TABLE:
            alist_destroy(&ast->create_table.columns);
            break;
        case AST_UPDATE_ROW:
            alist_destroy(&ast->update.values);
            break;
        case AST_DELETE_ROW:
            break;
        case AST_SELECT: {
            int expr_count = alist_length(&ast->select.expressions);
            for (int i = 0; i < expr_count; i++) {
                Expr** expr_ptr = (Expr**)alist_get(&ast->select.expressions, i);
                if (expr_ptr && *expr_ptr) {
                    free_expr_contents(*expr_ptr);
                    free(*expr_ptr);
                }
            }
            alist_destroy(&ast->select.expressions);
            int order_by_count = alist_length(&ast->select.order_by);
            for (int i = 0; i < order_by_count; i++) {
                Expr** expr_ptr = (Expr**)alist_get(&ast->select.order_by, i);
                if (expr_ptr && *expr_ptr) {
                    free_expr_contents(*expr_ptr);
                    free(*expr_ptr);
                }
            }
            alist_destroy(&ast->select.order_by);
            alist_destroy(&ast->select.order_by_desc);
            break;
        }
        default:
            break;
    }

    free(ast);
}
