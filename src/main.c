#include "db.h"
#include "logger.h"

static void print_usage() {
    printf("Database System\n");
    printf("Supported SQL commands:\n");
    printf("  CREATE TABLE table_name (col1 TYPE, col2 TYPE, ...);\n");
    printf("  INSERT INTO table_name (value1, value2, ...);\n");
    printf("  SELECT col1, col2, ... FROM table_name;\n");
    printf("  DROP TABLE table_name;\n");
    printf("  EXIT\n");
    printf("\nData types: INT, STRING, FLOAT\n");
    printf("String literals: Use single quotes\n");
    printf("Example: CREATE TABLE users (id INT, name STRING, age INT);\n");
}

int main() {
    char input[1024];
    printf("Simple Database v1.0\n");
    printf("Type 'HELP' for usage, 'EXIT' to quit\n\n");
    
    log_msg(LOG_INFO, "Database system started");
    
    while (1) {
        printf("db> ");
        fflush(stdout);
        
        if (!fgets(input, sizeof(input), stdin)) {
            log_msg(LOG_INFO, "Input stream ended, exiting");
            break;
        }
        
        input[strcspn(input, "\n")] = '\0';
        
        if (strlen(input) == 0) continue;
        
        log_msg(LOG_DEBUG, "Processing command: %s", input);
        
        if (strcmp(input, "EXIT") == 0 || strcmp(input, "exit") == 0) {
            log_msg(LOG_INFO, "Exit command received");
            break;
        }
        
        if (strcmp(input, "HELP") == 0 || strcmp(input, "help") == 0) {
            print_usage();
            continue;
        }
        
        if (strcmp(input, "LIST") == 0 || strcmp(input, "list") == 0) {
            extern Table tables[];
            extern int table_count;
            log_msg(LOG_INFO, "Listing %d tables", table_count);
            printf("Tables:\n");
            for (int i = 0; i < table_count; i++) {
                printf("  %s (%d columns, %d rows)\n", 
                       tables[i].name, tables[i].schema.column_count, tables[i].row_count);
                log_msg(LOG_DEBUG, "Table %s: %d columns, %d rows", 
                       tables[i].name, tables[i].schema.column_count, tables[i].row_count);
            }
            continue;
        }
        
        log_msg(LOG_DEBUG, "Tokenizing input");
        Token* tokens = tokenize(input);
        if (!tokens) {
            printf("Error: Tokenization failed\n");
            log_msg(LOG_ERROR, "Tokenization failed for input: %s", input);
            continue;
        }
        log_msg(LOG_DEBUG, "Tokenization completed successfully");
        
        log_msg(LOG_DEBUG, "Parsing tokens");
        ASTNode* ast = parse(tokens);
        if (!ast) {
            printf("Error: Parse failed - invalid SQL syntax\n");
            log_msg(LOG_ERROR, "Parse failed for input: %s", input);
            free_tokens(tokens);
            continue;
        }
        log_msg(LOG_DEBUG, "Parsing completed successfully");
        
        log_msg(LOG_DEBUG, "Generating IR");
        IRNode* ir = ast_to_ir(ast);
        if (!ir) {
            printf("Error: IR generation failed\n");
            log_msg(LOG_ERROR, "IR generation failed for input: %s", input);
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
    printf("\nGoodbye!\n");
    return 0;
}
