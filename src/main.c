#include "db.h"
#include "logger.h"
#include "arraylist.h"
#include "table.h"
#include <unistd.h>

static void print_usage(void) {
    printf("Usage: db [OPTIONS]\n");
    printf("  Simple Database System\n\n");
    printf("Options:\n");
    printf("  --show-logs    Show debug and info logs\n");
    printf("  --help, -h     Show this help message\n");
}

int main(int argc, char* argv[]) {
    bool show_logs = false;

    init_tables();

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--show-logs") == 0) {
            show_logs = true;
        } else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            print_usage();
            return 0;
        }
    }

    if (show_logs) {
        set_log_level(LOG_INFO);
    } else {
        set_log_level(LOG_ERROR + 1);
    }

    char input[8192];
    printf("Simple Database System\n");
    printf("Type 'HELP' for usage, 'EXIT' to quit\n\n");
    
    log_msg(LOG_INFO, "Database system started");
    
    while (1) {
        printf("db> ");
        fflush(stdout);
        
        input[0] = '\0';
        char line[1024];
        bool has_content = false;
        
        while (fgets(line, sizeof(line), stdin)) {
            line[strcspn(line, "\r\n")] = '\0';
            
            if (!has_content) {
                strcpy(input, line);
                has_content = true;
            } else {
                if (strlen(input) + strlen(line) + 2 < sizeof(input)) {
                    strcat(input, " ");
                    strcat(input, line);
                }
            }
            
            if (strchr(line, ';')) {
                break;
            }
            
            if (has_content && strlen(line) > 0) {
                printf("  > ");
                fflush(stdout);
            } else if (!has_content) {
                printf("db> ");
                fflush(stdout);
            }
        }
        
        if (!has_content) {
            log_msg(LOG_INFO, "Input stream ended, exiting");
            break;
        }
        
        input[strcspn(input, "\r\n")] = '\0';
        
        if (strlen(input) == 0) continue;

        log_msg(LOG_DEBUG, "Processing command: '%s'", input);
        
        /* Remove trailing semicolon if present */
        size_t input_len = strlen(input);
        if (input_len > 0 && input[input_len - 1] == ';') {
            input[input_len - 1] = '\0';
        }
        
        if (strcmp(input, "EXIT") == 0 || strcmp(input, "exit") == 0) {
            log_msg(LOG_INFO, "Exit command received");
            break;
        }
        
        if (strcmp(input, "HELP") == 0 || strcmp(input, "help") == 0) {
            print_usage();
            continue;
        }
        
        if (strcmp(input, "LIST") == 0 || strcmp(input, "list") == 0) {
            int table_count = alist_length(&tables);
            extern ArrayList tables;
            log_msg(LOG_INFO, "Listing %d tables", table_count);
            printf("Tables:\n");
            for (int i = 0; i < table_count; i++) {
                Table* table = (Table*)alist_get(&tables, i);
                if (table) {
                    int col_count = alist_length(&table->schema.columns);
                    int row_count = alist_length(&table->rows);
                    printf("  '%s' (%d columns, %d rows)\n",
                           table->name, col_count, row_count);
                    log_msg(LOG_DEBUG, "Table '%s': %d columns, %d rows",
                           table->name, col_count, row_count);
                }
            }
            continue;
        }
        
        log_msg(LOG_DEBUG, "Tokenizing input");
        Token* tokens = tokenize(input);
        if (!tokens) {
            log_msg(LOG_ERROR, "Tokenization failed for input: '%s'", input);
            continue;
        }
        log_msg(LOG_DEBUG, "Tokenization completed successfully");
        
        log_msg(LOG_DEBUG, "Parsing tokens");
        ASTNode* ast = parse_ex(input, tokens);
        if (!ast) {
            log_msg(LOG_ERROR, "Parse failed for input: '%s'", input);
            ParseContext* ctx = parse_get_context();
            if (ctx->error_occurred) {
                parse_error_report(ctx);
            }
            free_tokens(tokens);
            continue;
        }
        log_msg(LOG_DEBUG, "Parsing completed successfully");
        
        log_msg(LOG_DEBUG, "Generating IR");
        IRNode* ir = ast_to_ir(ast);
        if (!ir) {
            log_msg(LOG_ERROR, "IR generation failed for input: '%s'", input);
            free_tokens(tokens);
            free_ast(ast);
            continue;
        }
        log_msg(LOG_DEBUG, "IR generation completed successfully");
        
        log_msg(LOG_DEBUG, "Executing IR");
        exec_ir(ir);
        log_msg(LOG_DEBUG, "IR execution completed");
        
        free_tokens(tokens);
        free_ast(ast);
        free_ir(ir);
    }
    
    log_msg(LOG_INFO, "Database system shutting down");
    alist_destroy(&tables);
    printf("\nGoodbye!\n");
    return 0;
}
