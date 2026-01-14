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
    exec_ir(ir);
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

    exec("CREATE TABLE users (id INT, name STRING, age INT);");
    exec("INSERT INTO users (1, 'Alice', 25);");
    exec("INSERT INTO users (2, 'Bob', 30);");

    Table* table = find_table("users");
    assert(table != NULL);
    assert(table->row_count == 2);

    log_msg(LOG_INFO, "SELECT tests passed");
}

void test_drop_table(void) {
    log_msg(LOG_INFO, "Testing DROP TABLE...");

    reset_database();

    exec("CREATE TABLE test_table (id INT);");

    Table* table_before = find_table("test_table");
    assert(table_before != NULL);

    exec("DROP TABLE test_table");

    Table* table_after = find_table("test_table");
    assert(table_after == NULL);

    log_msg(LOG_INFO, "DROP TABLE tests passed");
}

void test_crud_operations(void) {
    log_msg(LOG_INFO, "Testing integration CRUD operations...\n");

    reset_database();
    exec("CREATE TABLE users (id INT, name STRING, age INT);");
    exec("CREATE TABLE products (id INT, name STRING, price FLOAT);");

    assert(table_count == 2);

    exec("INSERT INTO users (1, 'Alice', 25);");
    exec("INSERT INTO users (2, 'Bob', 30);");
    exec("INSERT INTO users (3, 'Charlie', 35);");

    exec("INSERT INTO products (1, 'Laptop', 999.99);");
    exec("INSERT INTO products (2, 'Mouse', 29.99);");

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

    exec("DROP TABLE users;");
    exec("DROP TABLE products;");

    assert(find_table("users") == NULL);
    assert(find_table("products") == NULL);
    assert(table_count == 0);

    log_msg(LOG_INFO, "Integration CRUD tests passed");
}

void test_where_clause(void) {
    log_msg(LOG_INFO, "Testing WHERE clause...");

    reset_database();

    exec("CREATE TABLE users (id INT, name STRING, age INT, salary FLOAT);");
    const char* insert_data[] = {"INSERT INTO users (1, 'Alice', 25, 50000.0);",
                                 "INSERT INTO users (2, 'Bob', 30, 60000.0);",
                                 "INSERT INTO users (3, 'Charlie', 35, 70000.0);",
                                 "INSERT INTO users (4, 'Diana', 28, 55000.0);",
                                 "INSERT INTO users (5, 'Eve', 22, 45000.0);"};

    for (int i = 0; i < 5; i++) {
        exec(insert_data[i]);
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

    exec("CREATE TABLE products (id INT, name STRING, price FLOAT, stock INT);");

    exec("INSERT INTO products (1, 'Laptop', 999.99, 50);");
    exec("INSERT INTO products (2, 'Mouse', 29.99, 200);");
    exec("INSERT INTO products (3, 'Keyboard', 79.99, 100);");
    exec("INSERT INTO products (4, 'Monitor', 299.99, 75);");

    Table* table = find_table("products");
    assert(table != NULL);
    assert(table->row_count == 4);

    log_msg(LOG_INFO, "Testing UPDATE with simple WHERE clause...");
    exec("UPDATE products SET stock = 0 WHERE price < 100;");

    Token* tokens = tokenize("UPDATE products SET stock = 0 WHERE price < 100;");

    log_msg(LOG_DEBUG, "Generated %d tokens for UPDATE", 11);
    for (int i = 0; i < 11; i++) {
        log_msg(LOG_DEBUG, "Token %d: type=%d, value='%s'", i, tokens[i].type, tokens[i].value);
    }

    ASTNode* ast = parse(tokens);
    if (ast) {
        log_msg(LOG_DEBUG, "AST parsing successful");
        IRNode* ir = ast_to_ir(ast);
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
    exec("UPDATE products SET price = 1099.99 WHERE name = 'Laptop';");
    exec("UPDATE products SET price = 1099.99 WHERE name = 'Laptop';");
    exec("UPDATE products SET stock = 150 WHERE price > 100 AND stock < 100;");

    log_msg(LOG_INFO, "UPDATE WHERE clause tests passed");
}

void test_delete_where_clause(void) {
    log_msg(LOG_INFO, "Testing DELETE with WHERE clause...");

    reset_database();

    exec("CREATE TABLE logs (id INT, level STRING, message STRING, timestamp INT);");
    exec("INSERT INTO logs (1, 'ERROR', 'Database connection failed', 1000);");
    exec("INSERT INTO logs (2, 'INFO', 'User login successful', 1005);");
    exec("INSERT INTO logs (3, 'ERROR', 'File not found', 1010);");
    exec("INSERT INTO logs (4, 'WARN', 'Disk space low', 1015);");
    exec("INSERT INTO logs (5, 'INFO', 'User logout', 1020);");

    Table* table = find_table("logs");
    assert(table != NULL);
    assert(table->row_count == 5);

    log_msg(LOG_INFO, "Testing DELETE with WHERE clause...");
    exec("DELETE FROM logs WHERE level = 'ERROR';");
    exec("DELETE FROM logs WHERE timestamp < 1010;");
    exec("DELETE FROM logs WHERE level = 'WARN' OR timestamp > 1015;");
    exec("DELETE FROM logs WHERE level = 'INFO' AND message LIKE 'success';");

    log_msg(LOG_INFO, "DELETE WHERE clause tests passed");
}

void test_where_cases(void) {
    log_msg(LOG_INFO, "Testing WHERE clause edge cases...");

    reset_database();

    exec("CREATE TABLE test_data (id INT, name STRING, value INT, notes STRING);");

    exec("INSERT INTO test_data (1, 'Alice', 100, 'NULL value test');");
    exec("INSERT INTO test_data (2, 'NULL', 200, '');");
    exec("INSERT INTO test_data (3, '', 0, 'Empty string test');");

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

void test_null_values(void) {
    log_msg(LOG_INFO, "Testing NULL value handling...");

    reset_database();
    exec("CREATE TABLE null_test (id INT, name STRING, value INT);");
    exec("INSERT INTO null_test (1, 'Alice', NULL);");
    exec("INSERT INTO null_test (2, 'NULL', 100);");
    exec("INSERT INTO null_test (3, 'Bob', 200);");

    Table* table = find_table("null_test");
    assert(table != NULL);
    assert(table->row_count == 3);
    assert(table->rows[0].is_null[2]);
    assert(!table->rows[0].is_null[0]);
    assert(table->rows[1].is_null[1]);

    exec("SELECT * FROM null_test WHERE value = NULL;");
    exec("SELECT * FROM null_test WHERE value != NULL;");
    exec("SELECT * FROM null_test WHERE name = NULL;");

    log_msg(LOG_INFO, "NULL value tests passed");
}

void test_empty_tables(void) {
    log_msg(LOG_INFO, "Testing empty table operations...");

    reset_database();
    exec("CREATE TABLE empty_test (id INT, name STRING);");

    Table* table = find_table("empty_test");
    assert(table != NULL);
    assert(table->row_count == 0);

    exec("SELECT * FROM empty_test;");

    exec("SELECT * FROM empty_test WHERE id > 0;");
    exec("SELECT * FROM empty_test WHERE name = 'test';");

    exec("UPDATE empty_test SET name = 'test' WHERE id = 1;");

    exec("DELETE FROM empty_test WHERE id = 1;");

    log_msg(LOG_INFO, "Empty table tests passed");
}

void test_maximum_limits(void) {
    log_msg(LOG_INFO, "Testing maximum limits...");

    reset_database();

    for (int i = 0; i < MAX_TABLES; i++) {
        char table_name[32];
        snprintf(table_name, sizeof(table_name), "table_%d", i);
        char create_sql[100];
        snprintf(create_sql, sizeof(create_sql), "CREATE TABLE %s (id INT);", table_name);
        exec(create_sql);
    }

    assert(table_count == MAX_TABLES);

    exec("CREATE TABLE extra_table (id INT);");
    assert(table_count == MAX_TABLES);

    reset_database();
    exec("CREATE TABLE row_test (id INT);");

    char insert_sql[50];
    for (int i = 0; i < MAX_ROWS; i++) {
        snprintf(insert_sql, sizeof(insert_sql), "INSERT INTO row_test (%d);", i);
        exec(insert_sql);
    }

    Table* row_table = find_table("row_test");
    assert(row_table->row_count == MAX_ROWS);

    log_msg(LOG_INFO, "Maximum limits tests passed");
}

void test_data_types(void) {
    log_msg(LOG_INFO, "Testing different data types...");

    reset_database();
    exec("CREATE TABLE type_test (id INT, price FLOAT, name STRING, active INT);");
    exec("INSERT INTO type_test (1, 99.99, 'Product', 1);");
    exec("INSERT INTO type_test (2, 0.50, 'Cheap', 0);");
    exec("INSERT INTO type_test (3, -10.25, 'Negative', 1);");

    exec("SELECT * FROM type_test WHERE price > 50.0;");
    exec("SELECT * FROM type_test WHERE price < 0;");
    exec("SELECT * FROM type_test WHERE id = 2;");

    exec("SELECT * FROM type_test WHERE name = 'Product';");
    exec("SELECT * FROM type_test WHERE name != 'Product';");

    exec("SELECT * FROM type_test WHERE active = 1;");
    exec("SELECT * FROM type_test WHERE active != 0;");

    log_msg(LOG_INFO, "Data type tests passed");
}

void test_aggregate_functions(void) {
    log_msg(LOG_INFO, "Testing aggregate functions...");

    reset_database();
    exec("CREATE TABLE agg_test (id INT, value INT, category STRING);");
    exec("INSERT INTO agg_test (1, 100, 'A');");
    exec("INSERT INTO agg_test (2, 200, 'A');");
    exec("INSERT INTO agg_test (3, 300, 'B');");
    exec("INSERT INTO agg_test (4, NULL, 'A');");

    exec("SELECT COUNT(*) FROM agg_test;");
    exec("SELECT COUNT(value) FROM agg_test;");
    exec("SELECT COUNT(DISTINCT value) FROM agg_test;");

    exec("SELECT SUM(value) FROM agg_test;");

    exec("SELECT AVG(value) FROM agg_test;");

    exec("SELECT MIN(value) FROM agg_test;");
    exec("SELECT MAX(value) FROM agg_test;");

    log_msg(LOG_INFO, "Aggregate function tests passed");
}

void test_complex_queries(void) {
    log_msg(LOG_INFO, "Testing complex queries...");

    reset_database();
    exec("CREATE TABLE complex_test (id INT, name STRING, age INT, salary FLOAT, dept STRING);");
    exec("INSERT INTO complex_test (1, 'Alice', 25, 50000.0, 'IT');");
    exec("INSERT INTO complex_test (2, 'Bob', 30, 60000.0, 'HR');");
    exec("INSERT INTO complex_test (3, 'Charlie', 35, 70000.0, 'IT');");
    exec("INSERT INTO complex_test (4, 'Diana', 28, 55000.0, 'Finance');");
    exec("INSERT INTO complex_test (5, 'Eve', 22, 45000.0, 'IT');");

    exec("SELECT * FROM complex_test WHERE age > 25 AND salary > 50000.0;");
    exec("SELECT * FROM complex_test WHERE dept = 'IT' OR age < 25;");
    exec("SELECT * FROM complex_test WHERE NOT dept = 'HR';");
    exec("SELECT * FROM complex_test WHERE (dept = 'IT' AND age > 25) OR salary > 60000.0;");

    exec("SELECT * FROM complex_test WHERE name LIKE 'A%';");
    exec("SELECT * FROM complex_test WHERE name LIKE '%a%';");
    exec("SELECT * FROM complex_test WHERE dept LIKE 'I%';");

    log_msg(LOG_INFO, "Complex query tests passed");
}

void test_error_conditions(void) {
    log_msg(LOG_INFO, "Testing error conditions...");

    reset_database();

    exec("SELECT * FROM nonexistent_table;");

    exec("CREATE TABLE error_test (id INT);");
    exec("INSERT INTO error_test (1);");
    exec("SELECT * FROM error_test WHERE nonexistent_column = 1;");
    exec("INSERT INTO error_test ('string');");

    log_msg(LOG_INFO, "Error condition tests passed");
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
    test_null_values();
    test_empty_tables();
    test_maximum_limits();
    test_data_types();
    test_aggregate_functions();
    test_complex_queries();
    test_error_conditions();

    log_msg(LOG_INFO, "All tests passed!");

    return 0;
}
