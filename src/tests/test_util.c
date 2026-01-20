#include "test_util.h"

#include <assert.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "arraylist.h"
#include "db.h"
#include "logger.h"
#include "table.h"

extern ArrayList tables;
extern ArrayList indexes;

static void free_index(void* ptr) {
    Index* index = (Index*)ptr;
    if (!index) return;

    if (index->buckets) {
        for (int i = 0; i < index->bucket_count; i++) {
            IndexEntry* entry = index->buckets[i];
            while (entry) {
                IndexEntry* next = entry->next;
                if (entry->key.type == TYPE_STRING && entry->key.char_val) {
                    free(entry->key.char_val);
                }
                free(entry);
                entry = next;
            }
        }
        free(index->buckets);
        index->buckets = NULL;
    }
}

void reset_database(void) {
    if (tables.data != NULL) {
        alist_destroy(&tables);
    }
    alist_init(&tables, sizeof(Table), free_table_internal);

    if (indexes.data != NULL) {
        alist_destroy(&indexes);
    }
    alist_init(&indexes, sizeof(Index), free_index);
}

bool exec(const char* sql) {
    Token* tokens = tokenize(sql);
    assert_true(tokens, "Tokenization failed for: %s", sql);

    ASTNode* ast = parse(tokens);
    assert_true(ast, "Parsing failed for: %s", sql);

    exec_ast(ast);

    free_tokens(tokens);
    free_ast(ast);
    return true;
}

Table* find_table_by_name(const char* name) { return find_table(name); }

static void format_message(char* buffer, size_t size, const char* format, va_list args) {
    vsnprintf(buffer, size, format, args);
}

void assert_true(bool condition, const char* format, ...) {
    if (!condition) {
        char message[512];
        va_list args;
        va_start(args, format);
        format_message(message, sizeof(message), format, args);
        va_end(args);
        log_msg(LOG_ERROR, "ASSERTION FAILED: %s", message);
        assert(condition);
    }
}

void assert_false(bool condition, const char* format, ...) { assert_true(!condition, format); }

void assert_int_eq(int expected, int actual, const char* format, ...) {
    if (expected != actual) {
        char message[512];
        va_list args;
        va_start(args, format);
        format_message(message, sizeof(message), format, args);
        va_end(args);
        log_msg(LOG_ERROR, "ASSERTION FAILED: %s (expected %d, got %d)", message, expected, actual);
        assert(expected == actual);
    }
}

void assert_str_eq(const char* expected, const char* actual, const char* format, ...) {
    if (expected == NULL && actual == NULL) return;
    if (expected == NULL || actual == NULL) {
        char message[512];
        va_list args;
        va_start(args, format);
        format_message(message, sizeof(message), format, args);
        va_end(args);
        log_msg(LOG_ERROR, "ASSERTION FAILED: %s (one string is NULL)", message);
        assert(false);
    }
    if (strcmp(expected, actual) != 0) {
        char message[512];
        va_list args;
        va_start(args, format);
        format_message(message, sizeof(message), format, args);
        va_end(args);
        log_msg(LOG_ERROR, "ASSERTION FAILED: %s (expected '%s', got '%s')", message, expected,
                actual);
        assert(strcmp(expected, actual) == 0);
    }
}

void assert_ptr_not_null(void* ptr, const char* format, ...) {
    if (ptr == NULL) {
        char message[512];
        va_list args;
        va_start(args, format);
        format_message(message, sizeof(message), format, args);
        va_end(args);
        log_msg(LOG_ERROR, "ASSERTION FAILED: %s (pointer is NULL)", message);
        assert(ptr != NULL);
    }
}

void assert_ptr_null(void* ptr, const char* format, ...) {
    if (ptr != NULL) {
        char message[512];
        va_list args;
        va_start(args, format);
        format_message(message, sizeof(message), format, args);
        va_end(args);
        log_msg(LOG_ERROR, "ASSERTION FAILED: %s (pointer should be NULL but isn't)", message);
        assert(ptr == NULL);
    }
}

void assert_float_eq(double expected, double actual, double epsilon, const char* format, ...) {
    if (expected == actual) return;
    double diff = expected - actual;
    if (diff < 0) diff = -diff;
    if (diff > epsilon) {
        char message[512];
        va_list args;
        va_start(args, format);
        format_message(message, sizeof(message), format, args);
        va_end(args);
        log_msg(LOG_ERROR, "ASSERTION FAILED: %s (expected %.6f, got %.6f, epsilon=%.6f)", message,
                expected, actual, epsilon);
        assert(diff <= epsilon);
    }
}
