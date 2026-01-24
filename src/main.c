#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "arraylist.h"
#include "db.h"
#include "logger.h"
#include "utils.h"
#include "table.h"

static void print_usage(void) {
    printf("Usage: db [OPTIONS]\n");
    printf("  Simple Database System\n\n");
    printf("Options:\n");
    printf("  --show-logs    Show debug and info logs\n");
    printf("  --help, -h     Show this help message\n");
}

static void parse_command_line_args(int argc, char *argv[], bool *show_logs) {
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--show-logs") == 0) {
            *show_logs = true;
        } else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            print_usage();
            exit(0);
        }
    }
}

static void init_repl_session(bool show_logs) {
    if (show_logs) {
        set_log_level(LOG_DEBUG);
    } else {
        set_log_level(LOG_NONE);
    }

    printf("Simple Database System\n");
    printf("Type '.help;' for usage, '.exit;' to quit\n\n");
    log_msg(LOG_INFO, "Database system started");
}

static bool read_input_line(char *input, size_t input_size) {
    printf("db> ");
    fflush(stdout);

    input[0] = '\0';
    char line[1024];
    bool has_content = false;

    while (fgets(line, sizeof(line), stdin)) {
        line[strcspn(line, "\r\n")] = '\0';

        if (!has_content) {
            string_copy(input, input_size, line);
            has_content = true;
        } else {
            if (strlen(input) + strlen(line) + 2 < input_size) {
                size_t input_len = strlen(input);
                string_copy(input + input_len, input_size - input_len, " ");
                input_len = strlen(input);
                string_copy(input + input_len, input_size - input_len, line);
            }
        }

        if (strchr(line, ';')) {
            break;
        }

        fflush(stdout);
    }

    if (!has_content) {
        log_msg(LOG_INFO, "Input stream ended, exiting");
        return false;
    }

    input[strcspn(input, "\r\n")] = '\0';
    return true;
}

static bool handle_meta_command(const char *command) {
    if (strcmp(command, ".EXIT;") == 0 || strcmp(command, ".exit;") == 0) {
        log_msg(LOG_INFO, "Exit command received");
        return true;
    }

    if (strcmp(command, ".HELP;") == 0 || strcmp(command, ".help;") == 0) {
        print_usage();
        return false;
    }

    if (strcmp(command, ".LIST") == 0 || strcmp(command, ".list") == 0) {
        extern ArrayList tables;
        int table_count = alist_length(&tables);
        log_msg(LOG_INFO, "Listing %d tables", table_count);
        for (int i = 0; i < table_count; i++) {
            Table *table = (Table *)alist_get(&tables, i);
            if (table) {
                int col_count = alist_length(&table->schema.columns);
                int row_count = alist_length(&table->rows);
                log_msg(LOG_DEBUG, "Table '%s': %d columns, %d rows", table->name, col_count,
                        row_count);
            }
        }
        return false;
    }

    if (command[0] == '.') {
        log_msg(LOG_ERROR, "Unknown command: %s", command);
        return false;
    }

    return false;
}

static void process_single_statement(const char *trimmed_stmt) {
    if (strcmp(trimmed_stmt, ".LIST") == 0) {
        extern ArrayList tables;
        int table_count = alist_length(&tables);
        log_msg(LOG_INFO, "Listing %d tables", table_count);
        for (int i = 0; i < table_count; i++) {
            Table *table = (Table *)alist_get(&tables, i);
            if (table) {
                int col_count = alist_length(&table->schema.columns);
                int row_count = alist_length(&table->rows);
                log_msg(LOG_DEBUG, "Table '%s': %d columns, %d rows", table->name, col_count,
                        row_count);
            }
        }
        return;
    }

    log_msg(LOG_DEBUG, "Processing statement: '%s'", trimmed_stmt);
    log_msg(LOG_DEBUG, "Tokenizing input");

    Token *tokens = tokenize(trimmed_stmt);
    if (!tokens) {
        log_msg(LOG_ERROR, "Tokenization failed for input: '%s'", trimmed_stmt);
        return;
    }

    log_msg(LOG_DEBUG, "Tokenization completed successfully");
    log_msg(LOG_DEBUG, "Parsing tokens");

    ASTNode *ast = parse_ex(trimmed_stmt, tokens);
    if (!ast) {
        log_msg(LOG_ERROR, "Parse failed for input: '%s'", trimmed_stmt);
        ParseContext *ctx = parse_get_context();
        if (ctx->error_occurred) {
            parse_error_report(ctx);
        }
        free_tokens(tokens);
        return;
    }

    log_msg(LOG_DEBUG, "Parsing completed successfully");
    log_msg(LOG_DEBUG, "Executing AST");

    exec_ast(ast);

    log_msg(LOG_DEBUG, "AST execution completed");
    free_ast(ast);
    free_tokens(tokens);
}

static void cleanup_repl_session(void) {
    log_msg(LOG_INFO, "Database system shutting down");
    alist_destroy(&tables);
    printf("\nGoodbye!\n");
}

int main(int argc, char *argv[]) {
    bool show_logs = false;

    init_tables();
    parse_command_line_args(argc, argv, &show_logs);
    init_repl_session(show_logs);

    char input[8192];
    while (read_input_line(input, sizeof(input))) {
        if (strlen(input) == 0)
            continue;

        log_msg(LOG_DEBUG, "Processing command: '%s'", input);

        if (handle_meta_command(input)) {
            break;
        }

        if (input[0] != '.') {
            size_t input_len = strlen(input);
            char *input_copy = malloc(input_len + 1);
            if (!input_copy) {
                log_msg(LOG_ERROR, "Memory allocation failed");
                continue;
            }
            string_copy(input_copy, input_len + 1, input);

            char *stmt = input_copy;
            while (true) {
                char *semicolon_pos = strchr(stmt, ';');
                if (semicolon_pos) {
                    *semicolon_pos = '\0';
                }

                char *trimmed = stmt;
                while (*trimmed == ' ' || *trimmed == '\t')
                    trimmed++;
                char *end = trimmed + strlen(trimmed) - 1;
                while (end > trimmed && (*end == ' ' || *end == '\t'))
                    end--;
                *(end + 1) = '\0';

                if (*trimmed != '\0') {
                    process_single_statement(trimmed);
                }

                if (semicolon_pos) {
                    stmt = semicolon_pos + 1;
                } else {
                    break;
                }
            }

            free(input_copy);
        }
    }

    cleanup_repl_session();
    return 0;
}
