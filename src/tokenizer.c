#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>

#include "arraylist.h"
#include "db.h"
#include "logger.h"

static bool is_operator_char(char c) {
    return c == '=' || c == '!' || c == '<' || c == '>' || c == '+' || c == '-' || c == '*' ||
           c == '/' || c == '%';
}

typedef struct {
    const char* name;
    TokenType type;
} KeywordMap;

typedef struct {
    const char* op;
    TokenType type;
} OperatorMap;

static const KeywordMap keywords[] = {{"CREATE", TOKEN_KEYWORD},
                                      {"TABLE", TOKEN_KEYWORD},
                                      {"INDEX", TOKEN_KEYWORD},
                                      {"ON", TOKEN_KEYWORD},
                                      {"INSERT", TOKEN_KEYWORD},
                                      {"INTO", TOKEN_KEYWORD},
                                      {"VALUES", TOKEN_KEYWORD},
                                      {"SELECT", TOKEN_KEYWORD},
                                      {"FROM", TOKEN_KEYWORD},
                                      {"DROP", TOKEN_KEYWORD},
                                      {"EXIT", TOKEN_KEYWORD},
                                      {"INT", TOKEN_KEYWORD},
                                      {"STRING", TOKEN_KEYWORD},
                                      {"FLOAT", TOKEN_KEYWORD},
                                      {"NULL", TOKEN_KEYWORD},
                                      {"WHERE", TOKEN_KEYWORD},
                                      {"UPDATE", TOKEN_KEYWORD},
                                      {"SET", TOKEN_KEYWORD},
                                      {"DELETE", TOKEN_KEYWORD},
                                      {"DISTINCT", TOKEN_DISTINCT},
                                      {"TIME", TOKEN_TIME},
                                      {"DATE", TOKEN_DATE},
                                      {"AND", TOKEN_AND},
                                      {"OR", TOKEN_OR},
                                      {"NOT", TOKEN_NOT},
                                      {"LIKE", TOKEN_LIKE},
                                      {"ORDER", TOKEN_ORDER},
                                      {"BY", TOKEN_BY},
                                      {"LIMIT", TOKEN_KEYWORD},
                                      {"ASC", TOKEN_KEYWORD},
                                      {"DESC", TOKEN_KEYWORD},
                                      {"SUM", TOKEN_AGGREGATE_FUNC},
                                      {"COUNT", TOKEN_AGGREGATE_FUNC},
                                      {"AVG", TOKEN_AGGREGATE_FUNC},
                                      {"MIN", TOKEN_AGGREGATE_FUNC},
                                      {"MAX", TOKEN_AGGREGATE_FUNC},
                                      {"ABS", TOKEN_SCALAR_FUNC},
                                      {"MID", TOKEN_SCALAR_FUNC},
                                      {"LEFT", TOKEN_SCALAR_FUNC},
                                      {"RIGHT", TOKEN_SCALAR_FUNC},
                                      {"UPPER", TOKEN_SCALAR_FUNC},
                                      {"LOWER", TOKEN_SCALAR_FUNC},
                                      {"LENGTH", TOKEN_SCALAR_FUNC},
                                      {"ROUND", TOKEN_SCALAR_FUNC},
                                      {"FLOOR", TOKEN_SCALAR_FUNC},
                                      {"CEIL", TOKEN_SCALAR_FUNC},
                                      {"SQRT", TOKEN_SCALAR_FUNC},
                                      {"MOD", TOKEN_SCALAR_FUNC},
                                      {"POWER", TOKEN_SCALAR_FUNC},
                                       {"SUBSTRING", TOKEN_SCALAR_FUNC},
                                       {"CONCAT", TOKEN_SCALAR_FUNC},
                                       {"AS", TOKEN_AS},
                                       {"EXISTS", TOKEN_EXISTS},
                                       {"IN", TOKEN_IN},
                                       {"PRIMARY", TOKEN_PRIMARY},
                                       {"KEY", TOKEN_KEY},
                                       {"REFERENCES", TOKEN_REFERENCES},
                                       {"NULL", TOKEN_NULL},
                                       {"UNIQUE", TOKEN_UNIQUE},
                                       {"FOREIGN", TOKEN_KEYWORD} };

static const OperatorMap operators[] = {{"==", TOKEN_EQUALS},     {"!=", TOKEN_NOT_EQUALS},
                                        {"<=", TOKEN_LESS_EQUAL}, {">=", TOKEN_GREATER_EQUAL},
                                        {"<", TOKEN_LESS},        {">", TOKEN_GREATER},
                                        {"=", TOKEN_EQUALS},      {"LIKE", TOKEN_LIKE}};

static TokenType lookup_keyword(const char* str) {
    for (size_t i = 0; i < sizeof(keywords) / sizeof(keywords[0]); i++) {
        if (strcasecmp(str, keywords[i].name) == 0) {
            return keywords[i].type;
        }
    }
    return TOKEN_IDENTIFIER;
}

static void add_token(ArrayList* tokens, TokenType type, const char* value) {
    Token* token = (Token*)alist_append(tokens);
    token->type = type;
    strncpy(token->value, value, MAX_TOKEN_LEN - 1);
    token->value[MAX_TOKEN_LEN - 1] = '\0';
}

static int tokenize_string(const char* input, int i, ArrayList* tokens) {
    char quote = input[i++];
    int j = 0;
    int len = strlen(input);

    Token* token = (Token*)alist_append(tokens);

    while (i < len && input[i] != quote && j < MAX_TOKEN_LEN - 1) {
        token->value[j++] = input[i++];
    }
    if (i < len && input[i] == quote) i++;

    token->value[j] = '\0';

    if (j == 10 && token->value[4] == '-' && token->value[7] == '-') {
        bool is_date = true;
        for (int k = 0; k < 10; k++) {
            if (k == 4 || k == 7) continue;
            if (!isdigit(token->value[k])) {
                is_date = false;
                break;
            }
        }
        if (is_date) {
            token->type = TOKEN_DATE;
            return i;
        }
    }

    if (j == 8 && token->value[2] == ':' && token->value[5] == ':') {
        bool is_time = true;
        for (int k = 0; k < 8; k++) {
            if (k == 2 || k == 5) continue;
            if (!isdigit(token->value[k])) {
                is_time = false;
                break;
            }
        }
        if (is_time) {
            token->type = TOKEN_TIME;
            return i;
        }
    }

    token->type = TOKEN_STRING;
    return i;
}

static int tokenize_number(const char* input, int i, ArrayList* tokens) {
    int j = 0;
    bool has_dot = false;
    int len = strlen(input);

    Token* token = (Token*)alist_append(tokens);

    while (i < len && (isdigit(input[i]) || (input[i] == '.' && !has_dot)) &&
           j < MAX_TOKEN_LEN - 1) {
        if (input[i] == '.') has_dot = true;
        token->value[j++] = input[i++];
    }

    token->value[j] = '\0';
    token->type = TOKEN_NUMBER;
    return i;
}

static int tokenize_identifier(const char* input, int i, ArrayList* tokens) {
    int j = 0;
    int len = strlen(input);

    Token* token = (Token*)alist_append(tokens);

    while (i < len && (isalpha(input[i]) || isdigit(input[i]) || input[i] == '_') &&
           j < MAX_TOKEN_LEN - 1) {
        token->value[j++] = input[i++];
    }

    token->value[j] = '\0';
    token->type = lookup_keyword(token->value);
    return i;
}

static int tokenize_operator(const char* input, int i, ArrayList* tokens) {
    int len = strlen(input);

    for (size_t k = 0; k < sizeof(operators) / sizeof(operators[0]); k++) {
        int op_len = strlen(operators[k].op);
        if (strncmp(&input[i], operators[k].op, op_len) == 0) {
            if (op_len == 2 && i + 1 < len && isalnum(input[i + 2])) continue;
            add_token(tokens, operators[k].type, operators[k].op);
            return i + op_len;
        }
    }

    int j = 0;
    Token* token = (Token*)alist_append(tokens);
    while (i < len && is_operator_char(input[i]) && j < MAX_TOKEN_LEN - 1) {
        token->value[j++] = input[i++];
    }
    token->value[j] = '\0';
    token->type = TOKEN_OPERATOR;
    return i;
}

Token* tokenize(const char* input) {
    if (!input) {
        log_msg(LOG_WARN, "tokenize: Called with NULL input");
        return NULL;
    }

    log_msg(LOG_DEBUG, "tokenize: Tokenizing input: '%s'", input);

    ArrayList tokens;
    alist_init(&tokens, sizeof(Token), NULL);

    int i = 0;
    int len = strlen(input);

    while (i < len) {
        if (isspace(input[i])) {
            i++;
            continue;
        }

        if (input[i] == '(') {
            add_token(&tokens, TOKEN_LPAREN, "(");
            i++;
        } else if (input[i] == ')') {
            add_token(&tokens, TOKEN_RPAREN, ")");
            i++;
        } else if (input[i] == ',') {
            add_token(&tokens, TOKEN_COMMA, ",");
            i++;
        } else if (input[i] == ';') {
            add_token(&tokens, TOKEN_SEMICOLON, ";");
            i++;
        } else if (input[i] == '\'' || input[i] == '"') {
            i = tokenize_string(input, i, &tokens);
        } else if (input[i] == '-' &&
                   (isdigit(input[i + 1]) || (input[i + 1] == '.' && isdigit(input[i + 2])))) {
            char neg_value[32];
            int j = 1;
            int len = strlen(input);
            bool has_dot = false;

            neg_value[0] = '-';
            i++;

            while (i < len && (isdigit(input[i]) || (input[i] == '.' && !has_dot)) && j < 30) {
                if (input[i] == '.') has_dot = true;
                neg_value[j++] = input[i++];
            }
            neg_value[j] = '\0';

            Token* token = (Token*)alist_append(&tokens);
            strcpy(token->value, neg_value);
            token->type = TOKEN_NUMBER;
        } else if (isdigit(input[i]) || (input[i] == '.' && isdigit(input[i + 1]))) {
            i = tokenize_number(input, i, &tokens);
        } else if (is_operator_char(input[i])) {
            i = tokenize_operator(input, i, &tokens);
        } else if (isalpha(input[i]) || input[i] == '_') {
            i = tokenize_identifier(input, i, &tokens);
        } else {
            Token* token = (Token*)alist_append(&tokens);
            token->type = TOKEN_ERROR;
            token->value[0] = input[i];
            token->value[1] = '\0';
            i++;
        }
    }

    add_token(&tokens, TOKEN_EOF, "");

    log_msg(LOG_DEBUG, "tokenize: Tokenization completed: %d tokens generated",
            alist_length(&tokens));

    int token_count = alist_length(&tokens);
    Token* result = malloc(token_count * sizeof(Token));
    if (result) {
        memcpy(result, tokens.data, token_count * sizeof(Token));
    }
    alist_destroy(&tokens);
    return result;
}

void free_tokens(Token* tokens) {
    if (tokens) free(tokens);
}
