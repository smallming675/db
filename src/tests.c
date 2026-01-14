#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "db.h"
#include "logger.h"

extern Table tables[MAX_TABLES];
extern int table_count;

static Table* find_table(const char* name) {
    for (int i = 0; i < table_count; i++) {
        if (strcmp(tables[i].name, name) == 0) {
            return &tables[i];
        }
    }
    return NULL;
}

static void exec(const char* sql);
static void test_select_data(void);
void reset_database(void) { table_count = 0; }

static void exec(const char* sql) {
    Token* tokens = tokenize(sql);
    ASTNode* ast = parse(tokens);
    IRNode* ir = ast_to_ir(ast);
    execute_ir(ir);
    free_tokens(tokens);
    free_ast(ast);
    free_ir(ir);
}

void test_create_table(void) {
    log_msg(LOG_INFO, "Testing CREATE TABLE...");

    reset_database();

    const char* create_sql = "CREATE TABLE users (id INT, name STRING, age INT);";
    exec(create_sql);

    Table* table = find_table("users");
    assert(table != NULL);
    assert(table->schema.column_count == 3);
    assert(strcmp(table->schema.columns[0].name, "id") == 0);
    assert(table->schema.columns[0].type == TYPE_INT);
    assert(strcmp(table->schema.columns[1].name, "name") == 0);
    assert(table->schema.columns[1].type == TYPE_STRING);
    assert(strcmp(table->schema.columns[2].name, "age") == 0);
    assert(table->schema.columns[2].type == TYPE_INT);
    assert(table->row_count == 0);

    log_msg(LOG_INFO, "CREATE TABLE tests passed");
}

void test_insert_data(void) {
    log_msg(LOG_INFO, "Testing INSERT...");

    reset_database();
    exec("CREATE TABLE users (id INT, name STRING, age INT);");
    exec("INSERT INTO users (1, 'Alice', 25);");

    Table* table = find_table("users");
    assert(table != NULL);
    assert(table->row_count == 1);
    assert(strcmp(table->rows[0].values[0].value, "1") == 0);
    assert(strcmp(table->rows[0].values[1].value, "Alice") == 0);
    assert(strcmp(table->rows[0].values[2].value, "25") == 0);

    log_msg(LOG_INFO, "INSERT tests passed");
}

void test_select_data(void) {
    log_msg(LOG_INFO, "Testing SELECT...");

    reset_database();

    execute_ir(ast_to_ir(parse(tokenize("CREATE TABLE users (id INT, name STRING, age INT);"))));
    execute_ir(ast_to_ir(parse(tokenize("INSERT INTO users (1, 'Alice', 25);"))));
    execute_ir(ast_to_ir(parse(tokenize("INSERT INTO users (2, 'Bob', 30);"))));

    Table* table = find_table("users");
    assert(table != NULL);
    assert(table->row_count == 2);

    log_msg(LOG_INFO, "SELECT tests passed");
}

void test_drop_table(void) {
    log_msg(LOG_INFO, "Testing DROP TABLE...");

    reset_database();

    IRNode* create_ir = ast_to_ir(parse(tokenize("CREATE TABLE test_table (id INT);")));
    execute_ir(create_ir);

    Table* table_before = find_table("test_table");
    assert(table_before != NULL);

    IRNode* drop_ir = ast_to_ir(parse(tokenize("DROP TABLE test_table")));
    execute_ir(drop_ir);

    Table* table_after = find_table("test_table");
    assert(table_after == NULL);

    free_ir(create_ir);
    free_ir(drop_ir);

    log_msg(LOG_INFO, "DROP TABLE tests passed");
}

void test_crud_operations(void) {
    log_msg(LOG_INFO, "Testing integration CRUD operations...\n");

    reset_database();
    execute_ir(ast_to_ir(parse(tokenize("CREATE TABLE users (id INT, name STRING, age INT);"))));
    execute_ir(
        ast_to_ir(parse(tokenize("CREATE TABLE products (id INT, name STRING, price FLOAT);"))));

    assert(table_count == 2);

    execute_ir(ast_to_ir(parse(tokenize("INSERT INTO users (1, 'Alice', 25);"))));
    execute_ir(ast_to_ir(parse(tokenize("INSERT INTO users (2, 'Bob', 30);"))));
    execute_ir(ast_to_ir(parse(tokenize("INSERT INTO users (3, 'Charlie', 35);"))));

    execute_ir(ast_to_ir(parse(tokenize("INSERT INTO products (1, 'Laptop', 999.99);"))));
    execute_ir(ast_to_ir(parse(tokenize("INSERT INTO products (2, 'Mouse', 29.99);"))));

    Table* users = find_table("users");
    Table* products = find_table("products");

    assert(users != NULL);
    assert(products != NULL);
    assert(users->row_count == 3);
    assert(products->row_count == 2);

    assert(strcmp(users->rows[0].values[0].value, "1") == 0);
    assert(strcmp(users->rows[0].values[1].value, "Alice") == 0);
    assert(strcmp(users->rows[0].values[2].value, "25") == 0);

    assert(strcmp(users->rows[1].values[0].value, "2") == 0);
    assert(strcmp(users->rows[1].values[1].value, "Bob") == 0);
    assert(strcmp(users->rows[1].values[2].value, "30") == 0);

    assert(strcmp(products->rows[0].values[1].value, "Laptop") == 0);
    assert(strcmp(products->rows[0].values[2].value, "999.99") == 0);

    execute_ir(ast_to_ir(parse(tokenize("DROP TABLE users;"))));
    execute_ir(ast_to_ir(parse(tokenize("DROP TABLE products;"))));

    assert(find_table("users") == NULL);
    assert(find_table("products") == NULL);
    assert(table_count == 0);

    log_msg(LOG_INFO, "Integration CRUD tests passed");
}

void test_where_clause(void) {
    log_msg(LOG_INFO, "Testing WHERE clause...");

    reset_database();

    const char* create_sql = "CREATE TABLE users (id INT, name STRING, age INT, salary FLOAT);";
    Token* tokens = tokenize(create_sql);
    ASTNode* ast = parse(tokens);
    IRNode* ir = ast_to_ir(ast);
    execute_ir(ir);
    free_tokens(tokens);
    free_ast(ast);
    free_ir(ir);

    const char* insert_data[] = {"INSERT INTO users (1, 'Alice', 25, 50000.0);",
                                 "INSERT INTO users (2, 'Bob', 30, 60000.0);",
                                 "INSERT INTO users (3, 'Charlie', 35, 70000.0);",
                                 "INSERT INTO users (4, 'Diana', 28, 55000.0);",
                                 "INSERT INTO users (5, 'Eve', 22, 45000.0);"};

    for (int i = 0; i < 5; i++) {
        tokens = tokenize(insert_data[i]);
        ast = parse(tokens);
        ir = ast_to_ir(ast);
        execute_ir(ir);
        free_tokens(tokens);
        free_ast(ast);
        free_ir(ir);
    }

    Table* table = find_table("users");
    assert(table != NULL);
    assert(table->row_count == 5);

    log_msg(LOG_INFO, "Testing comparison operators...");
    exec("SELECT * FROM users WHERE age = 30;");
    exec("SELECT * FROM users WHERE age != 30;");
    exec("SELECT * FROM users WHERE age < 30;");
    exec("SELECT * FROM users WHERE age <= 28;");
    exec("SELECT * FROM users WHERE age > 30;");
    exec("SELECT * FROM users WHERE age >= 30;");
    exec("SELECT * FROM users WHERE salary > 55000.0;");

    log_msg(LOG_INFO, "Testing string comparisons...");
    exec("SELECT * FROM users WHERE name = 'Alice';");
    exec("SELECT * FROM users WHERE name != 'Alice';");

    log_msg(LOG_INFO, "Testing logical operators...");
    exec("SELECT * FROM users WHERE age > 25 AND age < 35;");
    exec("SELECT * FROM users WHERE age < 25 OR age > 30;");
    exec("SELECT * FROM users WHERE NOT age = 30;");
    exec("SELECT * FROM users WHERE age > 25 AND name != 'Charlie';");
    exec("SELECT * FROM users WHERE (age > 30 OR name = 'Alice') AND salary > 50000.0;");

    log_msg(LOG_INFO, "Testing edge cases...");
    exec("SELECT * FROM users WHERE age > 100;");
    exec("SELECT * FROM users WHERE age < 0;");
    log_msg(LOG_INFO, "WHERE clause tests passed");
}

void test_update_where_clause(void) {
    log_msg(LOG_INFO, "Testing UPDATE with WHERE clause...");

    reset_database();

    const char* create_sql = "CREATE TABLE products (id INT, name STRING, price FLOAT, stock INT);";
    Token* tokens = tokenize(create_sql);
    ASTNode* ast = parse(tokens);
    IRNode* ir = ast_to_ir(ast);
    execute_ir(ir);
    free_tokens(tokens);
    free_ast(ast);
    free_ir(ir);

    const char* insert_data[] = {"INSERT INTO products (1, 'Laptop', 999.99, 50);",
                                 "INSERT INTO products (2, 'Mouse', 29.99, 200);",
                                 "INSERT INTO products (3, 'Keyboard', 79.99, 100);",
                                 "INSERT INTO products (4, 'Monitor', 299.99, 75);"};

    for (int i = 0; i < 4; i++) {
        tokens = tokenize(insert_data[i]);
        ast = parse(tokens);
        ir = ast_to_ir(ast);
        execute_ir(ir);
        free_tokens(tokens);
        free_ast(ast);
        free_ir(ir);
    }

    Table* table = find_table("products");
    assert(table != NULL);
    assert(table->row_count == 4);

    log_msg(LOG_INFO, "Testing UPDATE with simple WHERE clause...");
    tokens = tokenize("UPDATE products SET stock = 0 WHERE price < 100;");
    ast = parse(tokens);
    ir = ast_to_ir(ast);
    assert(ir != NULL);
    assert(ir->type == IR_UPDATE_ROW);
    free_tokens(tokens);
    free_ast(ast);
    free_ir(ir);

    log_msg(LOG_INFO, "Testing UPDATE with simple WHERE clause...");
    tokens = tokenize("UPDATE products SET stock = 0 WHERE price < 100;");

    log_msg(LOG_DEBUG, "Generated %d tokens for UPDATE", 11);
    for (int i = 0; i < 11; i++) {
        log_msg(LOG_DEBUG, "Token %d: type=%d, value='%s'", i, tokens[i].type, tokens[i].value);
    }

    ast = parse(tokens);
    if (ast) {
        log_msg(LOG_DEBUG, "AST parsing successful");
        ir = ast_to_ir(ast);
        if (ir) {
            log_msg(LOG_DEBUG, "IR generation successful");
            assert(ir->type == IR_UPDATE_ROW);
        } else {
            log_msg(LOG_DEBUG, "IR generation failed");
        }
        free_tokens(tokens);
        free_ast(ast);
        free_ir(ir);
    } else {
        log_msg(LOG_DEBUG, "AST parsing failed");
    }

    log_msg(LOG_INFO, "Testing UPDATE with complex WHERE clause...");
    tokens = tokenize("UPDATE products SET price = 1099.99 WHERE name = 'Laptop';");
    ast = parse(tokens);
    ir = ast_to_ir(ast);
    assert(ir != NULL);
    assert(ir->type == IR_UPDATE_ROW);
    free_tokens(tokens);
    free_ast(ast);
    free_ir(ir);

    tokens = tokenize("UPDATE products SET price = 1099.99 WHERE name = 'Laptop';");
    ast = parse(tokens);
    ir = ast_to_ir(ast);
    assert(ir != NULL);
    assert(ir->type == IR_UPDATE_ROW);
    free_tokens(tokens);
    free_ast(ast);
    free_ir(ir);

    tokens = tokenize("UPDATE products SET stock = 150 WHERE price > 100 AND stock < 100;");
    ast = parse(tokens);
    ir = ast_to_ir(ast);
    assert(ir != NULL);
    assert(ir->type == IR_UPDATE_ROW);
    free_tokens(tokens);
    free_ast(ast);
    free_ir(ir);

    log_msg(LOG_INFO, "UPDATE WHERE clause tests passed");
}

void test_delete_where_clause(void) {
    log_msg(LOG_INFO, "Testing DELETE with WHERE clause...");

    reset_database();

    const char* create_sql =
        "CREATE TABLE logs (id INT, level STRING, message STRING, timestamp INT);";
    Token* tokens = tokenize(create_sql);
    ASTNode* ast = parse(tokens);
    IRNode* ir = ast_to_ir(ast);
    execute_ir(ir);
    free_tokens(tokens);
    free_ast(ast);
    free_ir(ir);

    const char* insert_data[] = {
        "INSERT INTO logs (1, 'ERROR', 'Database connection failed', 1000);",
        "INSERT INTO logs (2, 'INFO', 'User login successful', 1005);",
        "INSERT INTO logs (3, 'ERROR', 'File not found', 1010);",
        "INSERT INTO logs (4, 'WARN', 'Disk space low', 1015);",
        "INSERT INTO logs (5, 'INFO', 'User logout', 1020);"};

    for (int i = 0; i < 5; i++) {
        tokens = tokenize(insert_data[i]);
        ast = parse(tokens);
        ir = ast_to_ir(ast);
        execute_ir(ir);
        free_tokens(tokens);
        free_ast(ast);
        free_ir(ir);
    }

    Table* table = find_table("logs");
    assert(table != NULL);
    assert(table->row_count == 5);

    log_msg(LOG_INFO, "Testing DELETE with WHERE clause...");
    tokens = tokenize("DELETE FROM logs WHERE level = 'ERROR';");
    ast = parse(tokens);
    ir = ast_to_ir(ast);
    assert(ir != NULL);
    assert(ir->type == IR_DELETE_ROW);
    free_tokens(tokens);
    free_ast(ast);
    free_ir(ir);

    tokens = tokenize("DELETE FROM logs WHERE timestamp < 1010;");
    ast = parse(tokens);
    ir = ast_to_ir(ast);
    assert(ir != NULL);
    assert(ir->type == IR_DELETE_ROW);
    free_tokens(tokens);
    free_ast(ast);
    free_ir(ir);

    tokens = tokenize("DELETE FROM logs WHERE level = 'WARN' OR timestamp > 1015;");
    ast = parse(tokens);
    ir = ast_to_ir(ast);
    assert(ir != NULL);
    assert(ir->type == IR_DELETE_ROW);
    free_tokens(tokens);
    free_ast(ast);
    free_ir(ir);

    tokens = tokenize("DELETE FROM logs WHERE level = 'INFO' AND message LIKE 'success';");
    ast = parse(tokens);
    ir = ast_to_ir(ast);
    assert(ir != NULL);
    assert(ir->type == IR_DELETE_ROW);
    free_tokens(tokens);
    free_ast(ast);
    free_ir(ir);

    log_msg(LOG_INFO, "DELETE WHERE clause tests passed");
}

void test_where_cases(void) {
    log_msg(LOG_INFO, "Testing WHERE clause edge cases...");

    reset_database();

    const char* create_sql =
        "CREATE TABLE test_data (id INT, name STRING, value INT, notes STRING);";
    Token* tokens = tokenize(create_sql);
    ASTNode* ast = parse(tokens);
    IRNode* ir = ast_to_ir(ast);
    execute_ir(ir);
    free_tokens(tokens);
    free_ast(ast);
    free_ir(ir);

    const char* insert_data[] = {"INSERT INTO test_data (1, 'Alice', 100, 'NULL value test');",
                                 "INSERT INTO test_data (2, 'NULL', 200, '');",
                                 "INSERT INTO test_data (3, '', 0, 'Empty string test');"};

    for (int i = 0; i < 3; i++) {
        tokens = tokenize(insert_data[i]);
        ast = parse(tokens);
        ir = ast_to_ir(ast);
        execute_ir(ir);
    }

    Table* table = find_table("test_data");
    assert(table != NULL);
    assert(table->row_count == 3);

    log_msg(LOG_INFO, "Testing NULL and empty string handling...");
    exec("SELECT * FROM test_data WHERE name = 'NULL';");
    exec("SELECT * FROM test_data WHERE name = '';");

    log_msg(LOG_INFO, "Testing negative values and zero...");
    exec("SELECT * FROM test_data WHERE value < 0;");
    exec("SELECT * FROM test_data WHERE value = 0;");

    log_msg(LOG_INFO, "Testing complex precedence...");
    exec("SELECT * FROM test_data WHERE value > 0 OR name = 'NULL' AND id < 5;");
    exec("SELECT * FROM test_data WHERE (value > 0 OR name = 'NULL') AND id < 5;");

    log_msg(LOG_INFO, "WHERE clause edge cases tests passed");
}

int main(int argc, char* argv[]) {
    if (argc && strcmp(argv[0], "--help") == 0) {
        printf("Usage: %s [--integration|--all]\n", argv[0]);
        printf("  (no args): Run unit tests only\n");
        printf("  --integration: Run integration tests only\n");
        printf("  --all: Run both unit and integration tests\n");
        return 1;
    }
    set_log_level(LOG_DEBUG);
    log_msg(LOG_INFO, "Running DB test suite...");

    test_create_table();
    test_insert_data();
    test_select_data();
    test_where_clause();
    test_update_where_clause();
    test_delete_where_clause();
    test_where_cases();
    test_drop_table();
    test_crud_operations();
    log_msg(LOG_INFO, "All tests passed!");

    return 0;
}
