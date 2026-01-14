#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <ctype.h>

#include "db.h"
#include "logger.h"

static bool is_keyword(const char* str) {
    static const char* keywords[] = {
        "CREATE", "TABLE", "INSERT", "INTO", "VALUES", "SELECT", "FROM", "DROP", 
        "INT", "STRING", "FLOAT", "NULL", "WHERE", "AND", "OR", "NOT", 
        "UPDATE", "SET", "DELETE", "SUM", "COUNT", "AVG", "MIN", "MAX", "DISTINCT"
    };
    static const int keyword_count = sizeof(keywords) / sizeof(keywords[0]);
    
    for (int i = 0; i < keyword_count; i++) {
        if (strcasecmp(str, keywords[i]) == 0) {
            return true;
        }
    }
    return false;
}

static bool is_operator_char(char c) {
    return c == '=' || c == '<' || c == '>' || c == '!' || c == '+' || c == '*' ||
           c == '/';
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

        if (input[i] == '(') {
            tokens[token_count].type = TOKEN_LPAREN;
            strcpy(tokens[token_count].value, "(");
            token_count++;
            i++;
        } else if (input[i] == ')') {
            tokens[token_count].type = TOKEN_RPAREN;
            strcpy(tokens[token_count].value, ")");
            token_count++;
            i++;
        } else if (input[i] == ',') {
            tokens[token_count].type = TOKEN_COMMA;
            strcpy(tokens[token_count].value, ",");
            token_count++;
            i++;
        } else if (input[i] == ';') {
            tokens[token_count].type = TOKEN_SEMICOLON;
            strcpy(tokens[token_count].value, ";");
            token_count++;
            i++;
        } else if (input[i] == '\'' || input[i] == '"') {
            char quote = input[i];
            i++;
            int j = 0;
            while (i < len && input[i] != quote && j < MAX_TOKEN_LEN - 1) {
                tokens[token_count].value[j++] = input[i++];
            }
            if (i < len && input[i] == quote) i++;
            tokens[token_count].value[j] = '\0';
            tokens[token_count].type = TOKEN_STRING;
            token_count++;
        } else if (isdigit(input[i]) || (input[i] == '.' && isdigit(input[i + 1]))) {
            int j = 0;
            bool has_dot = false;
            while (i < len && (isdigit(input[i]) || (input[i] == '.' && !has_dot)) &&
                   j < MAX_TOKEN_LEN - 1) {
                if (input[i] == '.') has_dot = true;
                tokens[token_count].value[j++] = input[i++];
            }
            tokens[token_count].value[j] = '\0';
            tokens[token_count].type = TOKEN_NUMBER;
            token_count++;
        } else if (is_operator_char(input[i])) {
            if (input[i] == '=' && i + 1 < len && input[i + 1] == '=') {
                strcpy(tokens[token_count].value, "==");
                tokens[token_count].type = TOKEN_EQUALS;
                token_count++;
                i += 2;
            } else if (input[i] == '!' && i + 1 < len && input[i + 1] == '=') {
                strcpy(tokens[token_count].value, "!=");
                tokens[token_count].type = TOKEN_NOT_EQUALS;
                token_count++;
                i += 2;
            } else if (input[i] == '<' && i + 1 < len && input[i + 1] == '=') {
                strcpy(tokens[token_count].value, "<=");
                tokens[token_count].type = TOKEN_LESS_EQUAL;
                token_count++;
                i += 2;
            } else if (input[i] == '>' && i + 1 < len && input[i + 1] == '=') {
                strcpy(tokens[token_count].value, ">=");
                tokens[token_count].type = TOKEN_GREATER_EQUAL;
                token_count++;
                i += 2;
            } else if (input[i] == '<') {
                strcpy(tokens[token_count].value, "<");
                tokens[token_count].type = TOKEN_LESS;
                token_count++;
                i++;
            } else if (input[i] == '>') {
                strcpy(tokens[token_count].value, ">");
                tokens[token_count].type = TOKEN_GREATER;
                token_count++;
                i++;
            } else if (input[i] == '=') {
                strcpy(tokens[token_count].value, "=");
                tokens[token_count].type = TOKEN_EQUALS;
                token_count++;
                i++;
            } else if (strncmp(&input[i], "LIKE", 4) == 0 &&
                       (i + 4 == len || !isalnum(input[i + 4]))) {
                strcpy(tokens[token_count].value, "LIKE");
                tokens[token_count].type = TOKEN_LIKE;
                token_count++;
                i += 4;
            } else {
                int j = 0;
                while (i < len && is_operator_char(input[i]) && j < MAX_TOKEN_LEN - 1) {
                    tokens[token_count].value[j++] = input[i++];
                }
                tokens[token_count].value[j] = '\0';
                tokens[token_count].type = TOKEN_OPERATOR;
                token_count++;
            }
        } else if (isalpha(input[i]) || input[i] == '_') {
            int j = 0;
            while (i < len && (isalpha(input[i]) || isdigit(input[i]) || input[i] == '_') &&
                   j < MAX_TOKEN_LEN - 1) {
                tokens[token_count].value[j++] = input[i++];
            }
            tokens[token_count].value[j] = '\0';

            if (strcasecmp(tokens[token_count].value, "AND") == 0) {
                tokens[token_count].type = TOKEN_AND;
            } else if (strcasecmp(tokens[token_count].value, "OR") == 0) {
                tokens[token_count].type = TOKEN_OR;
            } else if (strcasecmp(tokens[token_count].value, "NOT") == 0) {
                tokens[token_count].type = TOKEN_NOT;
            } else if (strcasecmp(tokens[token_count].value, "LIKE") == 0) {
                tokens[token_count].type = TOKEN_LIKE;
            } else if (strcasecmp(tokens[token_count].value, "SUM") == 0) {
                tokens[token_count].type = TOKEN_AGGREGATE_FUNC;
            } else if (strcasecmp(tokens[token_count].value, "COUNT") == 0) {
                tokens[token_count].type = TOKEN_AGGREGATE_FUNC;
            } else if (strcasecmp(tokens[token_count].value, "AVG") == 0) {
                tokens[token_count].type = TOKEN_AGGREGATE_FUNC;
            } else if (strcasecmp(tokens[token_count].value, "MIN") == 0) {
                tokens[token_count].type = TOKEN_AGGREGATE_FUNC;
            } else if (strcasecmp(tokens[token_count].value, "MAX") == 0) {
                tokens[token_count].type = TOKEN_AGGREGATE_FUNC;
            } else if (strcasecmp(tokens[token_count].value, "ABS") == 0) {
                tokens[token_count].type = TOKEN_SCALAR_FUNC;
            } else if (strcasecmp(tokens[token_count].value, "MID") == 0) {
                tokens[token_count].type = TOKEN_SCALAR_FUNC;
            } else if (strcasecmp(tokens[token_count].value, "LEFT") == 0) {
                tokens[token_count].type = TOKEN_SCALAR_FUNC;
            } else if (strcasecmp(tokens[token_count].value, "RIGHT") == 0) {
                tokens[token_count].type = TOKEN_SCALAR_FUNC;
            } else if (strcasecmp(tokens[token_count].value, "UPPER") == 0) {
                tokens[token_count].type = TOKEN_SCALAR_FUNC;
            } else if (strcasecmp(tokens[token_count].value, "LOWER") == 0) {
                tokens[token_count].type = TOKEN_SCALAR_FUNC;
            } else if (strcasecmp(tokens[token_count].value, "LENGTH") == 0) {
                tokens[token_count].type = TOKEN_SCALAR_FUNC;
            } else if (strcasecmp(tokens[token_count].value, "ROUND") == 0) {
                tokens[token_count].type = TOKEN_SCALAR_FUNC;
            } else if (strcasecmp(tokens[token_count].value, "FLOOR") == 0) {
                tokens[token_count].type = TOKEN_SCALAR_FUNC;
            } else if (strcasecmp(tokens[token_count].value, "CEIL") == 0) {
                tokens[token_count].type = TOKEN_SCALAR_FUNC;
            } else if (strcasecmp(tokens[token_count].value, "SQRT") == 0) {
                tokens[token_count].type = TOKEN_SCALAR_FUNC;
            } else if (strcasecmp(tokens[token_count].value, "MOD") == 0) {
                tokens[token_count].type = TOKEN_SCALAR_FUNC;
            } else if (strcasecmp(tokens[token_count].value, "POWER") == 0) {
                tokens[token_count].type = TOKEN_SCALAR_FUNC;
            } else if (strcasecmp(tokens[token_count].value, "SUBSTRING") == 0) {
                tokens[token_count].type = TOKEN_SCALAR_FUNC;
            } else if (strcasecmp(tokens[token_count].value, "CONCAT") == 0) {
                tokens[token_count].type = TOKEN_SCALAR_FUNC;
            } else if (strcasecmp(tokens[token_count].value, "DISTINCT") == 0) {
                tokens[token_count].type = TOKEN_DISTINCT;
            } else {
                tokens[token_count].type =
                    is_keyword(tokens[token_count].value) ? TOKEN_KEYWORD : TOKEN_IDENTIFIER;
            }
            token_count++;
        } else {
            tokens[token_count].type = TOKEN_ERROR;
            tokens[token_count].value[0] = input[i];
            tokens[token_count].value[1] = '\0';
            token_count++;
            i++;
        }

        if (token_count >= capacity - 1) {
            capacity *= 2;
            Token* new_tokens = realloc(tokens, capacity * sizeof(Token));
            if (!new_tokens) {
                free(tokens);
                return NULL;
            }
            tokens = new_tokens;
        }
    }

    tokens[token_count].type = TOKEN_EOF;
    strcpy(tokens[token_count].value, "");

    log_msg(LOG_DEBUG, "tokenize: Tokenization completed: %d tokens generated", token_count);
    return tokens;
}

void free_tokens(Token* tokens) {
    if (tokens) free(tokens);
}
