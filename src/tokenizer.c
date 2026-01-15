#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <ctype.h>
#include <time.h>

#include "db.h"
#include "logger.h"

static bool is_operator_char(char c) {
    return c == '=' || c == '!' || c == '<' || c == '>' || c == '+' || 
           c == '-' || c == '*' || c == '/' || c == '%';
}

typedef struct {
    const char* name;
    TokenType type;
} KeywordMap;

typedef struct {
    const char* op;
    TokenType type;
} OperatorMap;

static const KeywordMap keywords[] = {
    {"CREATE", TOKEN_KEYWORD}, {"TABLE", TOKEN_KEYWORD}, {"INSERT", TOKEN_KEYWORD},
    {"INTO", TOKEN_KEYWORD}, {"VALUES", TOKEN_KEYWORD}, {"SELECT", TOKEN_KEYWORD},
    {"FROM", TOKEN_KEYWORD}, {"DROP", TOKEN_KEYWORD}, {"INT", TOKEN_KEYWORD},
    {"STRING", TOKEN_KEYWORD}, {"FLOAT", TOKEN_KEYWORD}, {"NULL", TOKEN_KEYWORD},
    {"WHERE", TOKEN_KEYWORD}, {"UPDATE", TOKEN_KEYWORD}, {"SET", TOKEN_KEYWORD},
    {"DELETE", TOKEN_KEYWORD}, {"DISTINCT", TOKEN_KEYWORD}, {"TIME", TOKEN_TIME},
    {"DATE", TOKEN_DATE}, {"AND", TOKEN_AND}, {"OR", TOKEN_OR},
    {"NOT", TOKEN_NOT}, {"LIKE", TOKEN_LIKE}, {"SUM", TOKEN_AGGREGATE_FUNC},
    {"COUNT", TOKEN_AGGREGATE_FUNC}, {"AVG", TOKEN_AGGREGATE_FUNC}, {"MIN", TOKEN_AGGREGATE_FUNC},
    {"MAX", TOKEN_AGGREGATE_FUNC}, {"ABS", TOKEN_SCALAR_FUNC}, {"MID", TOKEN_SCALAR_FUNC},
    {"LEFT", TOKEN_SCALAR_FUNC}, {"RIGHT", TOKEN_SCALAR_FUNC}, {"UPPER", TOKEN_SCALAR_FUNC},
    {"LOWER", TOKEN_SCALAR_FUNC}, {"LENGTH", TOKEN_SCALAR_FUNC}, {"ROUND", TOKEN_SCALAR_FUNC},
    {"FLOOR", TOKEN_SCALAR_FUNC}, {"CEIL", TOKEN_SCALAR_FUNC}, {"SQRT", TOKEN_SCALAR_FUNC},
    {"MOD", TOKEN_SCALAR_FUNC}, {"POWER", TOKEN_SCALAR_FUNC}, {"SUBSTRING", TOKEN_SCALAR_FUNC},
    {"CONCAT", TOKEN_SCALAR_FUNC}
};

static const OperatorMap operators[] = {
    {"==", TOKEN_EQUALS}, {"!=", TOKEN_NOT_EQUALS}, {"<=", TOKEN_LESS_EQUAL},
    {">=", TOKEN_GREATER_EQUAL}, {"<", TOKEN_LESS}, {">", TOKEN_GREATER},
    {"=", TOKEN_EQUALS}, {"LIKE", TOKEN_LIKE}
};

static TokenType lookup_keyword(const char* str) {
    for (size_t i = 0; i < sizeof(keywords) / sizeof(keywords[0]); i++) {
        if (strcasecmp(str, keywords[i].name) == 0) {
            return keywords[i].type;
        }
    }
    return TOKEN_IDENTIFIER;
}

static void add_token(Token* tokens, int* token_count, TokenType type, const char* value) {
    tokens[*token_count].type = type;
    strncpy(tokens[*token_count].value, value, MAX_TOKEN_LEN - 1);
    tokens[*token_count].value[MAX_TOKEN_LEN - 1] = '\0';
    (*token_count)++;
}

static Token* resize_tokens(Token* tokens, int* capacity) {
    *capacity *= 2;
    Token* new_tokens = realloc(tokens, *capacity * sizeof(Token));
    if (!new_tokens) {
        free(tokens);
        return NULL;
    }
    return new_tokens;
}

static int tokenize_string(const char* input, int i, Token* tokens, int* token_count) {
    char quote = input[i++];
    int j = 0;
    int len = strlen(input);
    
    while (i < len && input[i] != quote && j < MAX_TOKEN_LEN - 1) {
        tokens[*token_count].value[j++] = input[i++];
    }
    if (i < len && input[i] == quote) i++;
    
    tokens[*token_count].value[j] = '\0';
    tokens[*token_count].type = TOKEN_STRING;
    (*token_count)++;
    return i;
}

static int tokenize_number(const char* input, int i, Token* tokens, int* token_count) {
    int j = 0;
    bool has_dot = false;
    int len = strlen(input);
    
    while (i < len && (isdigit(input[i]) || (input[i] == '.' && !has_dot)) && j < MAX_TOKEN_LEN - 1) {
        if (input[i] == '.') has_dot = true;
        tokens[*token_count].value[j++] = input[i++];
    }
    
    tokens[*token_count].value[j] = '\0';
    tokens[*token_count].type = TOKEN_NUMBER;
    (*token_count)++;
    return i;
}

static int tokenize_identifier(const char* input, int i, Token* tokens, int* token_count) {
    int j = 0;
    int len = strlen(input);
    
    while (i < len && (isalpha(input[i]) || isdigit(input[i]) || input[i] == '_') && j < MAX_TOKEN_LEN - 1) {
        tokens[*token_count].value[j++] = input[i++];
    }
    
    tokens[*token_count].value[j] = '\0';
    tokens[*token_count].type = lookup_keyword(tokens[*token_count].value);
    (*token_count)++;
    return i;
}

static int tokenize_operator(const char* input, int i, Token* tokens, int* token_count) {
    int len = strlen(input);
    
    for (size_t k = 0; k < sizeof(operators) / sizeof(operators[0]); k++) {
        int op_len = strlen(operators[k].op);
        if (strncmp(&input[i], operators[k].op, op_len) == 0) {
            if (op_len == 2 && i + 1 < len && isalnum(input[i + 2])) continue;
            add_token(tokens, token_count, operators[k].type, operators[k].op);
            return i + op_len;
        }
    }
    
    int j = 0;
    while (i < len && is_operator_char(input[i]) && j < MAX_TOKEN_LEN - 1) {
        tokens[*token_count].value[j++] = input[i++];
    }
    tokens[*token_count].value[j] = '\0';
    tokens[*token_count].type = TOKEN_OPERATOR;
    (*token_count)++;
    return i;
}

Token* tokenize(const char* input) {
    if (!input) {
        log_msg(LOG_WARN, "tokenize: Called with NULL input");
        return NULL;
    }

    log_msg(LOG_DEBUG, "tokenize: Tokenizing input: '%s'", input);

    int capacity = 100;
    Token* tokens = malloc(capacity * sizeof(Token));
    if (!tokens) {
        log_msg(LOG_ERROR, "tokenize: Failed to allocate memory for tokens");
        return NULL;
    }

    int token_count = 0;
    int i = 0;
    int len = strlen(input);

    while (i < len) {
        if (isspace(input[i])) {
            i++;
            continue;
        }

        if (token_count >= capacity - 1) {
            Token* new_tokens = resize_tokens(tokens, &capacity);
            if (!new_tokens) return NULL;
            tokens = new_tokens;
        }

        if (input[i] == '(') {
            add_token(tokens, &token_count, TOKEN_LPAREN, "(");
            i++;
        } else if (input[i] == ')') {
            add_token(tokens, &token_count, TOKEN_RPAREN, ")");
            i++;
        } else if (input[i] == ',') {
            add_token(tokens, &token_count, TOKEN_COMMA, ",");
            i++;
        } else if (input[i] == ';') {
            add_token(tokens, &token_count, TOKEN_SEMICOLON, ";");
            i++;
        } else if (input[i] == '\'' || input[i] == '"') {
            i = tokenize_string(input, i, tokens, &token_count);
        } else if (isdigit(input[i]) || (input[i] == '.' && isdigit(input[i + 1]))) {
            i = tokenize_number(input, i, tokens, &token_count);
        } else if (is_operator_char(input[i])) {
            i = tokenize_operator(input, i, tokens, &token_count);
        } else if (isalpha(input[i]) || input[i] == '_') {
            i = tokenize_identifier(input, i, tokens, &token_count);
        } else {
            tokens[token_count].type = TOKEN_ERROR;
            tokens[token_count].value[0] = input[i];
            tokens[token_count].value[1] = '\0';
            token_count++;
            i++;
        }
    }

    if (token_count >= capacity) {
        Token* new_tokens = resize_tokens(tokens, &capacity);
        if (!new_tokens) return NULL;
        tokens = new_tokens;
    }

    tokens[token_count].type = TOKEN_EOF;
    strcpy(tokens[token_count].value, "");

    log_msg(LOG_DEBUG, "tokenize: Tokenization completed: %d tokens generated", token_count);
    return tokens;
}

void free_tokens(Token* tokens) {
    if (tokens) free(tokens);
}
