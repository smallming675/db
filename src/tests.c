#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "db.h"
#include "logger.h"
#include "arraylist.h"
#include "table.h"

extern ArrayList tables;

static Table* find_table(const char* name) {
    int table_count = alist_length(&tables);
    for (int i = 0; i < table_count; i++) {
        Table* table = (Table*)alist_get(&tables, i);
        if (table && strcmp(table->name, name) == 0) {
            return table;
        }
    }
    return NULL;
}

void reset_database(void) { 
    if (tables.data != NULL) {
        alist_destroy(&tables);
    }
    alist_init(&tables, sizeof(Table), free_table_internal);
}

static void exec(const char* sql) {
    Token* tokens = tokenize(sql);
    ASTNode* ast = parse(tokens);
    if (ast) {
        IRNode* ir = ast_to_ir(ast);
        if (ir) {
            exec_ir(ir);
        } else {
            log_msg(LOG_WARN, "exec_ir: Called with NULL IR");
        }
        free_ir(ir);
    } else {
        log_msg(LOG_WARN, "ast_to_ir: called with NULL AST");
    }
    free_tokens(tokens);
    free_ast(ast);
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
    assert(table->rows[0].values[0].type == TYPE_INT);
    assert(table->rows[0].values[0].int_val == 1);
    assert(table->rows[0].values[1].type == TYPE_STRING);
    assert(strcmp(table->rows[0].values[1].char_val, "Alice") == 0);
    assert(table->rows[0].values[2].type == TYPE_INT);
    assert(table->rows[0].values[2].int_val == 25);

    log_msg(LOG_INFO, "INSERT tests passed");
}

void test_select_data(void) {
    log_msg(LOG_INFO, "Testing SELECT...");

    reset_database();

    exec("CREATE TABLE users (id INT, name STRING, age INT);");
    exec("INSERT INTO users (1, 'Alice', 25);");
    exec("INSERT INTO users (2, 'Bob', 30);");
    exec("SELECT * FROM users;");


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

    assert(alist_length(&tables) == 2);

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

    assert(users->rows[0].values[0].type == TYPE_INT);
    assert(users->rows[0].values[0].int_val == 1);
    assert(users->rows[0].values[1].type == TYPE_STRING);
    assert(strcmp(users->rows[0].values[1].char_val, "Alice") == 0);
    assert(users->rows[0].values[2].type == TYPE_INT);
    assert(users->rows[0].values[2].int_val == 25);

    assert(users->rows[1].values[0].type == TYPE_INT);
    assert(users->rows[1].values[0].int_val == 2);
    assert(users->rows[1].values[1].type == TYPE_STRING);
    assert(strcmp(users->rows[1].values[1].char_val, "Bob") == 0);
    assert(users->rows[1].values[2].type == TYPE_INT);
    assert(users->rows[1].values[2].int_val == 30);

    assert(products->rows[0].values[1].type == TYPE_STRING);
    assert(strcmp(products->rows[0].values[1].char_val, "Laptop") == 0);
    assert(products->rows[0].values[2].float_val > 999.98 &&
           products->rows[0].values[2].float_val < 1000.00);

    exec("DROP TABLE users;");
    exec("DROP TABLE products;");

    assert(find_table("users") == NULL);
    assert(find_table("products") == NULL);
    assert(alist_length(&tables) == 0);

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
    assert(table->rows[0].values[2].type == TYPE_NULL);
    assert((table->rows[1].values[1].type != TYPE_NULL));

    exec("SELECT * FROM null_test WHERE value IS NULL;");
    exec("SELECT * FROM null_test WHERE value IS NOT NULL;");
    exec("SELECT * FROM null_test WHERE name IS NULL;");

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

void test_limits(void) {
    log_msg(LOG_INFO, "Testing limits...");
    reset_database();

    for (int i = 0; i < MAX_TABLES; i++) {
        char table_name[32];
        char create_sql[100];
        snprintf(table_name, sizeof(table_name), "table%d", i);
        snprintf(create_sql, sizeof(create_sql), "CREATE TABLE %s (id INT);", table_name);
        exec(create_sql);
    }

    assert(alist_length(&tables) == MAX_TABLES);

    exec("CREATE TABLE extra_table (id INT);");
    assert(alist_length(&tables) == MAX_TABLES);

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

static void test_scalar_functions(void) {
    log_msg(LOG_INFO, "Testing scalar functions...");

    reset_database();

    exec("CREATE TABLE test_func (id INT, name STRING, value FLOAT, desc STRING);");
    exec("INSERT INTO test_func (1, 'Hello', -15.5, 'World');");
    exec("INSERT INTO test_func (2, 'TEST', 42.7, 'Database');");
    exec("INSERT INTO test_func (3, 'Mixed', 0.0, 'CASE');");

    exec("SELECT ABS(value) FROM test_func;");
    exec("SELECT UPPER(name), LOWER(name) FROM test_func;");
    exec("SELECT LENGTH(name), LENGTH(desc) FROM test_func;");
    exec("SELECT LEFT(name, 2), RIGHT(desc, 3) FROM test_func;");
    exec("SELECT MID(desc, 2, 3), SUBSTRING(name, 2, 2) FROM test_func;");
    exec("SELECT ROUND(value), FLOOR(value), CEIL(value) FROM test_func;");
    exec("SELECT SQRT(16), SQRT(value + 20) FROM test_func WHERE id = 2;");
    exec("SELECT MOD(10, 3), MOD(value, 5) FROM test_func WHERE id = 2;");
    exec("SELECT CONCAT(name, desc), CONCAT('ID:', id) FROM test_func;");
    exec("SELECT POWER(2, 3), POWER(value, 2) FROM test_func WHERE id = 2;");
    exec("INSERT INTO test_func VALUES (4, NULL, NULL, NULL);");
    exec("SELECT ABS(value), UPPER(name), LENGTH(desc) FROM test_func WHERE id = 4;");
}

void test_queries(void) {
    log_msg(LOG_INFO, "Testing complex queries...");

    reset_database();
    exec("CREATE TABLE test (id INT, name STRING, age INT, salary FLOAT, dept STRING);");
    exec("INSERT INTO test (1, 'Alice', 25, 50000.0, 'IT');");
    exec("INSERT INTO test (2, 'Bob', 30, 60000.0, 'HR');");
    exec("INSERT INTO test (3, 'Charlie', 35, 70000.0, 'IT');");
    exec("INSERT INTO test (4, 'Diana', 28, 55000.0, 'Finance');");
    exec("INSERT INTO test (5, 'Eve', 22, 45000.0, 'IT');");

    exec("SELECT * FROM test WHERE age > 25 AND salary > 50000.0;");
    exec("SELECT * FROM test WHERE dept = 'IT' OR age < 25;");
    exec("SELECT * FROM test WHERE NOT dept = 'HR';");
    exec("SELECT * FROM test WHERE (dept = 'IT' AND age > 25) OR salary > 60000.0;");

    exec("SELECT * FROM test WHERE name LIKE 'A%';");
    exec("SELECT * FROM test WHERE name LIKE '%a%';");
    exec("SELECT * FROM test WHERE dept LIKE 'I%';");

    log_msg(LOG_INFO, "Complex query tests passed");
}

void test_date_time_operations(void) {
    log_msg(LOG_INFO, "Testing DATE and TIME operations...");

    reset_database();

    exec("CREATE TABLE events (id INT, name STRING, event_date DATE, event_time TIME);");
    
    Table* events = find_table("events");
    assert(events != NULL);
    assert(events->schema.column_count == 4);
    assert(events->schema.columns[2].type == TYPE_DATE);
    assert(events->schema.columns[3].type == TYPE_TIME);
    
    log_msg(LOG_INFO, "DATE and TIME table creation successful");

    exec("INSERT INTO events (1, 'Christmas', '2023-12-25', '14:30:45');");
    exec("INSERT INTO events (2, 'New Year', '2024-01-01', '09:15:00');");
    exec("INSERT INTO events (3, 'Leap Day', '2024-02-29', '12:00:00');");
    exec("INSERT INTO events (4, 'Min Date', '1900-01-01', '00:00:00');");
    exec("INSERT INTO events (5, 'Max Date', '2100-12-31', '23:59:59');");
    exec("INSERT INTO events (6, 'Midnight', '2023-06-15', '00:00:00');");
    exec("INSERT INTO events (7, 'Noon', '2023-06-15', '12:00:00');");
    exec("INSERT INTO events (8, 'Day End', '2023-06-15', '23:59:59');");
    exec("INSERT INTO events (9, 'Null Date', NULL, '10:30:00');");
    exec("INSERT INTO events (10, 'Null Time', '2023-06-15', NULL);");
    
    log_msg(LOG_INFO, "DATE and TIME INSERT operations successful");
    exec("INSERT INTO events (11, 'Invalid Date', '2023-99-99', '12:30:45');");
    assert(events->row_count == 11);
    
    exec("INSERT INTO events (12, 'Invalid Time', '2023-06-15', '99:99:99');");
    assert(events->row_count == 12);
    
    exec("INSERT INTO events (13, 'Zero Date', '0000-00-00', '00:00:00');");
    assert(events->row_count == 13);
    
    exec("INSERT INTO events (14, 'Zero Time', '2023-06-15', '00:00:00');");
    assert(events->row_count == 14);

    assert(events->row_count == 14);
    
    log_msg(LOG_INFO, "Testing DATE and TIME value retrieval...");
    assert(events->rows[0].values[2].type == TYPE_DATE);
    assert(events->rows[0].values[2].date_val.year == 2023);
    assert(events->rows[0].values[2].date_val.month == 12);
    assert(events->rows[0].values[2].date_val.day == 25);
    
    assert(events->rows[0].values[3].type == TYPE_TIME);
    assert(events->rows[0].values[3].time_val.hour == 14);
    assert(events->rows[0].values[3].time_val.minute == 30);
    assert(events->rows[0].values[3].time_val.second == 45);
    
    log_msg(LOG_INFO, "Testing NULL handling...");
    assert(events->rows[8].values[2].type == TYPE_NULL);
    assert(events->rows[9].values[3].type == TYPE_NULL);
    
    log_msg(LOG_INFO, "DATE and TIME operations tests passed");
}

void test_date_time_edge_cases(void) {
    log_msg(LOG_INFO, "Testing DATE and TIME edge cases...");

    reset_database();

    exec("CREATE TABLE edge_test (id INT, date_col DATE, time_col TIME, name STRING);");
    
    exec("INSERT INTO edge_test (1, '2023-02-30', '24:00:00', 'Invalid Feb 30');");
    exec("INSERT INTO edge_test (2, '2023-13-01', '12:60:00', 'Invalid Month 13');");
    exec("INSERT INTO edge_test (3, '2023-00-01', '12:00:60', 'Invalid Month 0');");
    
    exec("INSERT INTO edge_test (4, '0000-01-01', '00:00:00', 'Min Boundary');");
    exec("INSERT INTO edge_test (5, '9999-12-31', '23:59:59', 'Max Boundary');");
    
    log_msg(LOG_INFO, "DATE and TIME edge cases tests passed");
}

void test_date_time_aggregations(void) {
    log_msg(LOG_INFO, "Testing DATE and TIME with aggregations...");

    reset_database();

    exec("CREATE TABLE mixed_data (id INT, name STRING, created_date DATE, created_time TIME);");
    
    Table* mixed = find_table("mixed_data");
    assert(mixed != NULL);
    assert(mixed->schema.column_count == 4);
    
    exec("INSERT INTO mixed_data (1, 'Test Entry', '2023-06-15', '10:30:45');");
    
    log_msg(LOG_INFO, "DATE and TIME aggregation tests passed");
}

static void test_date_time_literal_parsing(void) {
    log_msg(LOG_INFO, "Testing DATE and TIME literal parsing...");
    
    reset_database();
    
    exec("CREATE TABLE parse_test (id INT, date_col DATE, time_col TIME, name STRING);");
    
    Table* table = find_table("parse_test");
    assert(table != NULL);
    assert(table->schema.column_count == 4);
    assert(table->schema.columns[1].type == TYPE_DATE);
    assert(table->schema.columns[2].type == TYPE_TIME);
    
    log_msg(LOG_INFO, "Testing valid date literals...");
    exec("INSERT INTO parse_test (1, '2023-01-01', '09:00:00', 'New Year');");
    exec("INSERT INTO parse_test (2, '2023-12-31', '23:59:59', 'Year End');");
    exec("INSERT INTO parse_test (3, '2024-02-29', '12:30:45', 'Leap Year');");
    exec("INSERT INTO parse_test (4, '1900-01-01', '00:00:00', 'Min Date');");
    exec("INSERT INTO parse_test (5, '9999-12-31', '23:59:59', 'Max Date');");
    
    log_msg(LOG_INFO, "Testing valid time literals...");
    exec("INSERT INTO parse_test (6, '2023-06-15', '00:00:00', 'Midnight');");
    exec("INSERT INTO parse_test (7, '2023-06-15', '12:00:00', 'Noon');");
    exec("INSERT INTO parse_test (8, '2023-06-15', '23:59:59', 'Day End');");
    exec("INSERT INTO parse_test (9, '2023-06-15', '01:23:45', 'Random Time');");
    
    log_msg(LOG_INFO, "Testing boundary values...");
    exec("INSERT INTO parse_test (10, '2000-02-29', '00:00:00', 'Y2K Leap');");
    exec("INSERT INTO parse_test (11, '2100-02-28', '23:59:59', 'Century Edge');");
    
    log_msg(LOG_INFO, "Testing same day different times...");
    exec("INSERT INTO parse_test (12, '2023-07-04', '08:00:00', 'Morning');");
    exec("INSERT INTO parse_test (13, '2023-07-04', '14:30:00', 'Afternoon');");
    exec("INSERT INTO parse_test (14, '2023-07-04', '20:45:30', 'Evening');");
    
    assert(table->row_count == 14);
    
    log_msg(LOG_INFO, "Testing date/time literals in WHERE clauses...");
    exec("SELECT * FROM parse_test WHERE date_col = '2023-01-01';");
    exec("SELECT * FROM parse_test WHERE time_col = '12:00:00';");
    exec("SELECT * FROM parse_test WHERE date_col > '2023-06-01';");
    exec("SELECT * FROM parse_test WHERE time_col < '12:00:00';");
    exec("SELECT * FROM parse_test WHERE date_col >= '2023-01-01' AND date_col <= '2023-12-31';");
    exec("SELECT * FROM parse_test WHERE time_col > '09:00:00' AND time_col < '18:00:00';");
    
    log_msg(LOG_INFO, "Testing UPDATE with date/time literals...");
    exec("UPDATE parse_test SET date_col = '2023-12-25' WHERE id = 1;");
    exec("UPDATE parse_test SET time_col = '15:30:00' WHERE id = 2;");
    
    log_msg(LOG_INFO, "Testing mixed date/time operations...");
    exec("SELECT * FROM parse_test WHERE date_col = '2023-07-04' AND time_col = '08:00:00';");
    exec("SELECT * FROM parse_test WHERE date_col > '2023-01-01' OR time_col < '12:00:00';");
    
    log_msg(LOG_INFO, "Testing NULL date/time values...");
    exec("INSERT INTO parse_test (15, NULL, NULL, 'NULL Values');");
    exec("SELECT * FROM parse_test WHERE date_col IS NULL;");
    exec("SELECT * FROM parse_test WHERE time_col IS NULL;");
    
    assert(table->row_count == 15);
    
    log_msg(LOG_INFO, "DATE and TIME literal parsing tests passed");
}

static void test_between_operator(void) {
    log_msg(LOG_INFO, "Testing BETWEEN operator...");

    reset_database();

    exec("CREATE TABLE products (id INT, name STRING, price FLOAT, category STRING);");
    
    exec("INSERT INTO products (1, 'Product A', 10.50, 'Electronics');");
    exec("INSERT INTO products (2, 'Product B', 25.00, 'Books');");
    exec("INSERT INTO products (3, 'Product C', 50.75, 'Electronics');");
    exec("INSERT INTO products (4, 'Product D', 75.25, 'Clothing');");
    exec("INSERT INTO products (5, 'Product E', 100.00, 'Books');");
    exec("INSERT INTO products (6, 'Product F', 150.50, 'Electronics');");
    
    Table* table = find_table("products");
    assert(table != NULL);
    assert(table->row_count == 6);
    
    log_msg(LOG_INFO, "Testing BETWEEN with numeric values...");
    exec("SELECT * FROM products WHERE price BETWEEN 20.0 AND 80.0;");
    exec("SELECT * FROM products WHERE price BETWEEN 10.0 AND 25.0;");
    exec("SELECT * FROM products WHERE price BETWEEN 75.0 AND 150.0;");
    
    log_msg(LOG_INFO, "Testing BETWEEN with integer values...");
    exec("SELECT * FROM products WHERE id BETWEEN 2 AND 4;");
    exec("SELECT * FROM products WHERE id BETWEEN 1 AND 3;");
    
    log_msg(LOG_INFO, "Testing BETWEEN with string values...");
    exec("SELECT * FROM products WHERE name BETWEEN 'Product B' AND 'Product E';");
    exec("SELECT * FROM products WHERE category BETWEEN 'Books' AND 'Electronics';");
    
    log_msg(LOG_INFO, "Testing BETWEEN with AND combinations...");
    exec("SELECT * FROM products WHERE price BETWEEN 20.0 AND 80.0 AND category = 'Electronics';");
    exec("SELECT * FROM products WHERE id BETWEEN 2 AND 5 AND price > 50.0;");
    
    log_msg(LOG_INFO, "Testing BETWEEN with OR conditions...");
    exec("SELECT * FROM products WHERE price BETWEEN 10.0 AND 25.0 OR price BETWEEN 100.0 AND 200.0;");
    exec("SELECT * FROM products WHERE category = 'Electronics' OR price BETWEEN 20.0 AND 30.0;");
    
    log_msg(LOG_INFO, "Testing edge cases...");
    exec("SELECT * FROM products WHERE price BETWEEN 10.50 AND 10.50;");
    exec("SELECT * FROM products WHERE price BETWEEN 0.0 AND 10.0;");
    exec("SELECT * FROM products WHERE price BETWEEN 200.0 AND 300.0;");
    
    log_msg(LOG_INFO, "Testing BETWEEN with date/time values...");
    exec("CREATE TABLE events (id INT, name STRING, event_date DATE, event_time TIME);");
    exec("INSERT INTO events (1, 'Meeting', '2023-06-15', '09:00:00');");
    exec("INSERT INTO events (2, 'Lunch', '2023-06-15', '12:30:00');");
    exec("INSERT INTO events (3, 'Workshop', '2023-06-16', '14:00:00');");
    exec("INSERT INTO events (4, 'Conference', '2023-06-17', '10:00:00');");
    exec("INSERT INTO events (5, 'Party', '2023-06-18', '18:00:00');");
    
    exec("SELECT * FROM events WHERE event_date BETWEEN '2023-06-15' AND '2023-06-16';");
    exec("SELECT * FROM events WHERE event_time BETWEEN '09:00:00' AND '14:00:00';");
    
    log_msg(LOG_INFO, "BETWEEN operator tests passed");
}

void test_order_by(void) {
    log_msg(LOG_INFO, "Testing ORDER BY...");

    reset_database();

    exec("CREATE TABLE users (id INT, name STRING, age INT);");
    exec("INSERT INTO users (3, 'Charlie', 35);");
    exec("INSERT INTO users (1, 'Alice', 30);");
    exec("INSERT INTO users (2, 'Bob', 25);");
    exec("INSERT INTO users (4, 'Diana', 28);");

    Table* table = find_table("users");
    assert(table != NULL);
    assert(table->row_count == 4);

    log_msg(LOG_INFO, "Testing ORDER BY age ASC...");
    exec("SELECT * FROM users ORDER BY age;");

    log_msg(LOG_INFO, "Testing ORDER BY age DESC...");
    exec("SELECT * FROM users ORDER BY age DESC;");

    log_msg(LOG_INFO, "Testing ORDER BY name...");
    exec("SELECT * FROM users ORDER BY name;");

    log_msg(LOG_INFO, "Testing ORDER BY id DESC...");
    exec("SELECT * FROM users ORDER BY id DESC;");

    log_msg(LOG_INFO, "Testing ORDER BY multiple columns...");
    exec("CREATE TABLE products (id INT, category STRING, price FLOAT);");
    exec("INSERT INTO products (1, 'Electronics', 100.0);");
    exec("INSERT INTO products (2, 'Books', 50.0);");
    exec("INSERT INTO products (3, 'Electronics', 200.0);");
    exec("INSERT INTO products (4, 'Books', 75.0);");
    exec("INSERT INTO products (5, 'Clothing', 150.0);");
    exec("SELECT * FROM products ORDER BY category, price;");

    log_msg(LOG_INFO, "ORDER BY tests passed");
}

void test_limit(void) {
    log_msg(LOG_INFO, "Testing LIMIT...");

    reset_database();

    exec("CREATE TABLE users (id INT, name STRING, age INT);");
    exec("INSERT INTO users (1, 'Alice', 25);");
    exec("INSERT INTO users (2, 'Bob', 30);");
    exec("INSERT INTO users (3, 'Charlie', 35);");
    exec("INSERT INTO users (4, 'Diana', 28);");
    exec("INSERT INTO users (5, 'Eve', 22);");

    Table* table = find_table("users");
    assert(table != NULL);
    assert(table->row_count == 5);

    log_msg(LOG_INFO, "Testing LIMIT 2...");
    exec("SELECT * FROM users LIMIT 2;");

    log_msg(LOG_INFO, "Testing LIMIT 3...");
    exec("SELECT * FROM users LIMIT 3;");

    log_msg(LOG_INFO, "Testing LIMIT with larger value than row count...");
    exec("SELECT * FROM users LIMIT 10;");

    log_msg(LOG_INFO, "Testing LIMIT 0...");
    exec("SELECT * FROM users LIMIT 0;");

    log_msg(LOG_INFO, "LIMIT tests passed");
}

void test_order_by_with_limit(void) {
    log_msg(LOG_INFO, "Testing ORDER BY with LIMIT...");

    reset_database();

    exec("CREATE TABLE users (id INT, name STRING, age INT);");
    exec("INSERT INTO users (1, 'Alice', 30);");
    exec("INSERT INTO users (2, 'Bob', 25);");
    exec("INSERT INTO users (3, 'Charlie', 35);");
    exec("INSERT INTO users (4, 'Diana', 28);");

    Table* table = find_table("users");
    assert(table != NULL);
    assert(table->row_count == 4);

    log_msg(LOG_INFO, "Testing ORDER BY age LIMIT 2...");
    exec("SELECT * FROM users ORDER BY age LIMIT 2;");

    log_msg(LOG_INFO, "Testing ORDER BY age DESC LIMIT 3...");
    exec("SELECT * FROM users ORDER BY age DESC LIMIT 3;");

    log_msg(LOG_INFO, "Testing ORDER BY name...");
    exec("SELECT * FROM users ORDER BY name;");

    log_msg(LOG_INFO, "ORDER BY with LIMIT tests passed");
}

void test_distinct(void) {
    log_msg(LOG_INFO, "Testing DISTINCT...");

    reset_database();

    exec("CREATE TABLE distinct_test (id INT, category STRING, value INT);");
    exec("INSERT INTO distinct_test (1, 'A', 100);");
    exec("INSERT INTO distinct_test (2, 'A', 100);");
    exec("INSERT INTO distinct_test (3, 'B', 100);");
    exec("INSERT INTO distinct_test (4, 'A', 200);");
    exec("INSERT INTO distinct_test (5, 'B', 200);");
    exec("INSERT INTO distinct_test (6, 'C', 300);");

    Table* table = find_table("distinct_test");
    assert(table != NULL);
    assert(table->row_count == 6);

    log_msg(LOG_INFO, "Testing COUNT(DISTINCT category)...");
    exec("SELECT COUNT(DISTINCT category) FROM distinct_test;");

    log_msg(LOG_INFO, "Testing COUNT(DISTINCT value)...");
    exec("SELECT COUNT(DISTINCT value) FROM distinct_test;");

    log_msg(LOG_INFO, "Testing COUNT(DISTINCT id)...");
    exec("SELECT COUNT(DISTINCT id) FROM distinct_test;");

    log_msg(LOG_INFO, "Testing SUM with DISTINCT...");
    exec("SELECT SUM(DISTINCT value) FROM distinct_test;");

    log_msg(LOG_INFO, "Testing AVG with DISTINCT...");
    exec("SELECT AVG(DISTINCT value) FROM distinct_test;");

    log_msg(LOG_INFO, "Testing MIN with DISTINCT...");
    exec("SELECT MIN(DISTINCT value) FROM distinct_test;");

    log_msg(LOG_INFO, "Testing MAX with DISTINCT...");
    exec("SELECT MAX(DISTINCT value) FROM distinct_test;");

    log_msg(LOG_INFO, "Testing distinct with all same values...");
    exec("CREATE TABLE same_vals (id INT, val INT);");
    exec("INSERT INTO same_vals (1, 5);");
    exec("INSERT INTO same_vals (2, 5);");
    exec("INSERT INTO same_vals (3, 5);");
    exec("SELECT COUNT(DISTINCT val) FROM same_vals;");

    log_msg(LOG_INFO, "DISTINCT tests passed");
}

void test_arithmetic_operations(void) {
    log_msg(LOG_INFO, "Testing arithmetic operations...");

    reset_database();

    exec("CREATE TABLE arithmetic_test (id INT, a INT, b INT, c FLOAT);");
    exec("INSERT INTO arithmetic_test (1, 10, 5, 100.5);");
    exec("INSERT INTO arithmetic_test (2, 20, 3, 50.25);");
    exec("INSERT INTO arithmetic_test (3, 15, 0, 75.0);");

    Table* table = find_table("arithmetic_test");
    assert(table != NULL);
    assert(table->row_count == 3);

    log_msg(LOG_INFO, "Testing addition...");
    exec("SELECT a + b FROM arithmetic_test;");
    exec("SELECT a + 100 FROM arithmetic_test;");
    exec("SELECT 50 + b FROM arithmetic_test;");

    log_msg(LOG_INFO, "Testing subtraction...");
    exec("SELECT a - b FROM arithmetic_test;");
    exec("SELECT a - 5 FROM arithmetic_test;");
    exec("SELECT 100 - b FROM arithmetic_test;");

    log_msg(LOG_INFO, "Testing multiplication...");
    exec("SELECT a * b FROM arithmetic_test;");
    exec("SELECT a * 2 FROM arithmetic_test;");
    exec("SELECT 10 * b FROM arithmetic_test;");

    log_msg(LOG_INFO, "Testing division...");
    exec("SELECT a / b FROM arithmetic_test WHERE b != 0;");
    exec("SELECT a / 2 FROM arithmetic_test;");
    exec("SELECT 100 / a FROM arithmetic_test;");

    log_msg(LOG_INFO, "Testing modulus...");
    exec("SELECT a % b FROM arithmetic_test WHERE b != 0;");
    exec("SELECT a % 3 FROM arithmetic_test;");
    exec("SELECT 100 % a FROM arithmetic_test;");

    log_msg(LOG_INFO, "Testing mixed arithmetic...");
    exec("SELECT a + b * 2 FROM arithmetic_test;");
    exec("SELECT (a + b) * 2 FROM arithmetic_test;");
    exec("SELECT a * b + c FROM arithmetic_test;");

    log_msg(LOG_INFO, "Testing division by zero...");
    exec("SELECT a / 0 FROM arithmetic_test;");
    exec("SELECT a % 0 FROM arithmetic_test;");

    log_msg(LOG_INFO, "Arithmetic operation tests passed");
}

void test_arithmetic_in_where(void) {
    log_msg(LOG_INFO, "Testing arithmetic in WHERE clauses...");

    reset_database();

    exec("CREATE TABLE sales (id INT, product STRING, quantity INT, price FLOAT);");
    exec("INSERT INTO sales (1, 'Widget', 10, 25.50);");
    exec("INSERT INTO sales (2, 'Gadget', 5, 50.00);");
    exec("INSERT INTO sales (3, 'Widget', 20, 25.50);");
    exec("INSERT INTO sales (4, 'Gadget', 15, 50.00);");
    exec("INSERT INTO sales (5, 'Thingy', 8, 15.00);");

    log_msg(LOG_INFO, "Testing WHERE with addition...");
    exec("SELECT * FROM sales WHERE quantity + 5 > 20;");

    log_msg(LOG_INFO, "Testing WHERE with subtraction...");
    exec("SELECT * FROM sales WHERE price - 5.0 > 20.0;");

    log_msg(LOG_INFO, "Testing WHERE with multiplication...");
    exec("SELECT * FROM sales WHERE quantity * price > 500.0;");

    log_msg(LOG_INFO, "Testing WHERE with division...");
    exec("SELECT * FROM sales WHERE price / 2.0 > 20.0;");

    log_msg(LOG_INFO, "Testing WHERE with mixed arithmetic...");
    exec("SELECT * FROM sales WHERE quantity * price - 100 > 200;");

    log_msg(LOG_INFO, "Testing WHERE with parentheses...");
    exec("SELECT * FROM sales WHERE (quantity + 5) * price > 300;");

    log_msg(LOG_INFO, "Arithmetic in WHERE clause tests passed");
}

void test_string_operations(void) {
    log_msg(LOG_INFO, "Testing string operations...");

    reset_database();

    exec("CREATE TABLE strings (id INT, name STRING, email STRING);");
    exec("INSERT INTO strings (1, 'Alice', 'alice@example.com');");
    exec("INSERT INTO strings (2, 'Bob', 'bob@test.org');");
    exec("INSERT INTO strings (3, 'Charlie', 'charlie@domain.net');");
    exec("INSERT INTO strings (4, '', 'empty@name.com');");
    exec("INSERT INTO strings (5, 'Dave', 'dave@provider.io');");

    Table* table = find_table("strings");
    assert(table != NULL);
    assert(table->row_count == 5);

    log_msg(LOG_INFO, "Testing string equality...");
    exec("SELECT * FROM strings WHERE name = 'Alice';");
    exec("SELECT * FROM strings WHERE name != 'Alice';");

    log_msg(LOG_INFO, "Testing string comparison...");
    exec("SELECT * FROM strings WHERE name < 'Charlie';");
    exec("SELECT * FROM strings WHERE name > 'Bob';");
    exec("SELECT * FROM strings WHERE name <= 'Charlie';");

    log_msg(LOG_INFO, "Testing empty string handling...");
    exec("SELECT * FROM strings WHERE name = '';");
    exec("SELECT * FROM strings WHERE name != '';");

    log_msg(LOG_INFO, "Testing string concatenation...");
    exec("SELECT name || '@' || email FROM strings;");

    log_msg(LOG_INFO, "String operation tests passed");
}

void test_complex_expressions(void) {
    log_msg(LOG_INFO, "Testing complex expressions...");

    reset_database();

    exec("CREATE TABLE complex (id INT, a INT, b INT, c INT);");
    exec("INSERT INTO complex (1, 10, 20, 30);");
    exec("INSERT INTO complex (2, 5, 15, 25);");
    exec("INSERT INTO complex (3, 0, 10, 20);");

    log_msg(LOG_INFO, "Testing nested expressions...");
    exec("SELECT (a + b) * (c - a) FROM complex;");
    exec("SELECT ((a + b) + c) * 2 FROM complex;");

    log_msg(LOG_INFO, "Testing multiple operations...");
    exec("SELECT a + b + c FROM complex;");
    exec("SELECT a * b + c FROM complex;");
    exec("SELECT a * (b + c) FROM complex;");

    log_msg(LOG_INFO, "Testing with NULL values...");
    exec("CREATE TABLE null_expr (id INT, x INT, y INT);");
    exec("INSERT INTO null_expr (1, 10, NULL);");
    exec("INSERT INTO null_expr (2, NULL, 20);");
    exec("INSERT INTO null_expr (3, NULL, NULL);");
    exec("SELECT x + y FROM null_expr;");

    log_msg(LOG_INFO, "Complex expression tests passed");
}

void test_comparison_edge_cases(void) {
    log_msg(LOG_INFO, "Testing comparison edge cases...");

    reset_database();

    exec("CREATE TABLE comparisons (id INT, val INT);");
    exec("INSERT INTO comparisons (1, 0);");
    exec("INSERT INTO comparisons (2, -1);");
    exec("INSERT INTO comparisons (3, 1);");
    exec("INSERT INTO comparisons (4, 2147483647);");
    exec("INSERT INTO comparisons (5, -2147483648);");

    log_msg(LOG_INFO, "Testing zero comparisons...");
    exec("SELECT * FROM comparisons WHERE val = 0;");
    exec("SELECT * FROM comparisons WHERE val != 0;");
    exec("SELECT * FROM comparisons WHERE val > 0;");
    exec("SELECT * FROM comparisons WHERE val < 0;");
    exec("SELECT * FROM comparisons WHERE val >= 0;");
    exec("SELECT * FROM comparisons WHERE val <= 0;");

    log_msg(LOG_INFO, "Testing boundary value comparisons...");
    exec("SELECT * FROM comparisons WHERE val = 2147483647;");
    exec("SELECT * FROM comparisons WHERE val = -2147483648;");

    log_msg(LOG_INFO, "Testing float comparisons with integers...");
    exec("CREATE TABLE float_comp (id INT, price FLOAT);");
    exec("INSERT INTO float_comp (1, 99.99);");
    exec("INSERT INTO float_comp (2, 100.00);");
    exec("INSERT INTO float_comp (3, 100.01);");
    exec("SELECT * FROM float_comp WHERE price = 100.00;");

    log_msg(LOG_INFO, "Comparison edge case tests passed");
}

void test_aggregate_edge_cases(void) {
    log_msg(LOG_INFO, "Testing aggregate edge cases...");

    reset_database();

    log_msg(LOG_INFO, "Testing aggregates on empty table...");
    exec("CREATE TABLE empty_agg (id INT, value INT);");
    exec("SELECT COUNT(*) FROM empty_agg;");
    exec("SELECT COUNT(value) FROM empty_agg;");
    exec("SELECT SUM(value) FROM empty_agg;");
    exec("SELECT AVG(value) FROM empty_agg;");
    exec("SELECT MIN(value) FROM empty_agg;");
    exec("SELECT MAX(value) FROM empty_agg;");

    log_msg(LOG_INFO, "Testing aggregates with all NULL values...");
    exec("CREATE TABLE null_agg (id INT, value INT);");
    exec("INSERT INTO null_agg (1, NULL);");
    exec("INSERT INTO null_agg (2, NULL);");
    exec("INSERT INTO null_agg (3, NULL);");
    exec("SELECT COUNT(value) FROM null_agg;");
    exec("SELECT SUM(value) FROM null_agg;");
    exec("SELECT AVG(value) FROM null_agg;");
    exec("SELECT MIN(value) FROM null_agg;");
    exec("SELECT MAX(value) FROM null_agg;");

    log_msg(LOG_INFO, "Testing aggregates with mixed NULL and values...");
    exec("CREATE TABLE mixed_null (id INT, value INT);");
    exec("INSERT INTO mixed_null (1, 10);");
    exec("INSERT INTO mixed_null (2, NULL);");
    exec("INSERT INTO mixed_null (3, 20);");
    exec("INSERT INTO mixed_null (4, NULL);");
    exec("INSERT INTO mixed_null (5, 30);");
    exec("SELECT COUNT(value) FROM mixed_null;");
    exec("SELECT SUM(value) FROM mixed_null;");

    log_msg(LOG_INFO, "Testing aggregates with single row...");
    exec("CREATE TABLE single_row (id INT, value INT);");
    exec("INSERT INTO single_row (1, 42);");
    exec("SELECT COUNT(*) FROM single_row;");
    exec("SELECT COUNT(value) FROM single_row;");
    exec("SELECT SUM(value) FROM single_row;");
    exec("SELECT AVG(value) FROM single_row;");
    exec("SELECT MIN(value) FROM single_row;");
    exec("SELECT MAX(value) FROM single_row;");

    log_msg(LOG_INFO, "Aggregate edge case tests passed");
}

void test_scalar_function_edge_cases(void) {
    log_msg(LOG_INFO, "Testing scalar function edge cases...");

    reset_database();

    exec("CREATE TABLE scalar_edge (id INT, val INT, str STRING);");
    exec("INSERT INTO scalar_edge (1, 0, 'hello');");
    exec("INSERT INTO scalar_edge (2, -100, 'WORLD');");
    exec("INSERT INTO scalar_edge (3, NULL, 'Mixed');");
    exec("INSERT INTO scalar_edge (4, 42, NULL);");
    exec("INSERT INTO scalar_edge (5, -1, '');");

    log_msg(LOG_INFO, "Testing ABS with edge values...");
    exec("SELECT ABS(val) FROM scalar_edge;");
    exec("SELECT ABS(0), ABS(-1), ABS(100) FROM scalar_edge;");

    log_msg(LOG_INFO, "Testing math functions with NULL...");
    exec("SELECT ABS(val), SQRT(ABS(val)) FROM scalar_edge;");

    log_msg(LOG_INFO, "Testing string functions with NULL...");
    exec("SELECT UPPER(str), LOWER(str) FROM scalar_edge;");
    exec("SELECT LENGTH(str) FROM scalar_edge;");

    log_msg(LOG_INFO, "Testing string functions with empty string...");
    exec("SELECT LENGTH(str), UPPER(str), LOWER(str) FROM scalar_edge WHERE id = 5;");

    log_msg(LOG_INFO, "Testing CONCAT with NULL...");
    exec("SELECT CONCAT(str, ' suffix') FROM scalar_edge;");

    log_msg(LOG_INFO, "Testing SUBSTRING with edge cases...");
    exec("SELECT SUBSTRING(str, 1, 0) FROM scalar_edge;");
    exec("SELECT SUBSTRING(str, 10, 5) FROM scalar_edge;");

    log_msg(LOG_INFO, "Testing LEFT and RIGHT with edge cases...");
    exec("SELECT LEFT(str, 0), LEFT(str, 100) FROM scalar_edge;");
    exec("SELECT RIGHT(str, 0), RIGHT(str, 100) FROM scalar_edge;");

    log_msg(LOG_INFO, "Scalar function edge case tests passed");
}

void test_type_conversion(void) {
    log_msg(LOG_INFO, "Testing type conversion...");

    reset_database();

    exec("CREATE TABLE type_conv (id INT, int_val INT, float_val FLOAT, str_val STRING);");
    exec("INSERT INTO type_conv (1, 42, 3.14, '100');");
    exec("INSERT INTO type_conv (2, 0, 0.0, '0');");
    exec("INSERT INTO type_conv (3, -10, -5.5, '-50');");

    log_msg(LOG_INFO, "Testing INT to FLOAT comparisons...");
    exec("SELECT * FROM type_conv WHERE float_val > 0.0;");
    exec("SELECT * FROM type_conv WHERE float_val < int_val;");

    log_msg(LOG_INFO, "Testing INT to STRING comparisons...");
    exec("SELECT * FROM type_conv WHERE str_val = '100';");

    log_msg(LOG_INFO, "Testing FLOAT to INT in expressions...");
    exec("SELECT int_val + float_val FROM type_conv;");

    log_msg(LOG_INFO, "Type conversion tests passed");
}

void test_multiple_aggregates(void) {
    log_msg(LOG_INFO, "Testing multiple aggregates in single query...");

    reset_database();

    exec("CREATE TABLE multi_agg (id INT, a INT, b INT, c INT);");
    exec("INSERT INTO multi_agg (1, 10, 20, 30);");
    exec("INSERT INTO multi_agg (2, 15, 25, 35);");
    exec("INSERT INTO multi_agg (3, 20, 30, 40);");

    log_msg(LOG_INFO, "Testing multiple COUNT...");
    exec("SELECT COUNT(a), COUNT(b), COUNT(c) FROM multi_agg;");

    log_msg(LOG_INFO, "Testing multiple SUM...");
    exec("SELECT SUM(a), SUM(b), SUM(c) FROM multi_agg;");

    log_msg(LOG_INFO, "Testing multiple AVG...");
    exec("SELECT AVG(a), AVG(b), AVG(c) FROM multi_agg;");

    log_msg(LOG_INFO, "Testing MIN and MAX together...");
    exec("SELECT MIN(a), MAX(a), MIN(b), MAX(b) FROM multi_agg;");

    log_msg(LOG_INFO, "Testing mixed aggregate types...");
    exec("SELECT COUNT(*), SUM(a), AVG(b), MIN(c), MAX(c) FROM multi_agg;");

    log_msg(LOG_INFO, "Multiple aggregate tests passed");
}

void test_date_time_comparisons(void) {
    log_msg(LOG_INFO, "Testing DATE and TIME comparisons...");

    reset_database();

    exec("CREATE TABLE dt_compare (id INT, d1 DATE, d2 DATE, t1 TIME, t2 TIME);");
    exec("INSERT INTO dt_compare (1, '2023-01-01', '2023-06-15', '10:00:00', '14:30:00');");
    exec("INSERT INTO dt_compare (2, '2023-12-31', '2023-01-01', '23:59:59', '00:00:00');");
    exec("INSERT INTO dt_compare (3, '2024-02-29', '2024-02-28', '12:00:00', '12:00:01');");

    log_msg(LOG_INFO, "Testing DATE equality...");
    exec("SELECT * FROM dt_compare WHERE d1 = '2023-01-01';");

    log_msg(LOG_INFO, "Testing DATE inequality...");
    exec("SELECT * FROM dt_compare WHERE d1 != '2023-01-01';");

    log_msg(LOG_INFO, "Testing DATE comparisons...");
    exec("SELECT * FROM dt_compare WHERE d1 < '2023-06-01';");
    exec("SELECT * FROM dt_compare WHERE d1 > '2023-06-01';");
    exec("SELECT * FROM dt_compare WHERE d1 <= '2023-12-31';");
    exec("SELECT * FROM dt_compare WHERE d1 >= '2024-01-01';");

    log_msg(LOG_INFO, "Testing TIME comparisons...");
    exec("SELECT * FROM dt_compare WHERE t1 < '12:00:00';");
    exec("SELECT * FROM dt_compare WHERE t1 > '12:00:00';");
    exec("SELECT * FROM dt_compare WHERE t1 = '10:00:00';");

    log_msg(LOG_INFO, "Testing combined DATE and TIME conditions...");
    exec("SELECT * FROM dt_compare WHERE d1 < '2024-01-01' AND t1 > '00:00:00';");

    log_msg(LOG_INFO, "DATE and TIME comparison tests passed");
}

void test_case_sensitivity(void) {
    log_msg(LOG_INFO, "Testing case sensitivity...");

    reset_database();

    exec("CREATE TABLE case_test (id INT, Name STRING, NAME STRING, name STRING);");
    exec("INSERT INTO case_test (1, 'Alice', 'Bob', 'Charlie');");

    log_msg(LOG_INFO, "Testing column name case sensitivity...");
    exec("SELECT Name, NAME, name FROM case_test;");

    log_msg(LOG_INFO, "Testing table name case sensitivity...");
    exec("SELECT * FROM case_test;");
    exec("SELECT * FROM CASE_TEST;");

    log_msg(LOG_INFO, "Testing keyword case sensitivity...");
    exec("select * from case_test;");
    exec("SELECT * FROM case_test WHERE id = 1;");
    exec("select id from case_test where id = 1;");

    log_msg(LOG_INFO, "Case sensitivity tests passed");
}

void test_error_parsing(void) {
    log_msg(LOG_INFO, "Testing parsing error handling...");

    reset_database();

    log_msg(LOG_INFO, "Testing invalid SQL syntax...");
    exec("SELECT * FROM;");
    exec("SELECT FROM table;");
    exec("INSERT INTO (1, 2);");
    exec("CREATE TABLE (id INT);");
    exec("SELECT * WHERE id = 1;");

    log_msg(LOG_INFO, "Testing missing semicolons...");
    exec("SELECT * FROM nonexistent");

    log_msg(LOG_INFO, "Testing unterminated strings...");
    exec("SELECT * FROM test WHERE name = 'unterminated;");

    log_msg(LOG_INFO, "Testing invalid numbers...");
    exec("SELECT * FROM test WHERE id = 12345678901234567890;");

    log_msg(LOG_INFO, "Parsing error handling tests passed");
}

void test_like_patterns(void) {
    log_msg(LOG_INFO, "Testing LIKE patterns...");

    reset_database();

    exec("CREATE TABLE like_test (id INT, name STRING, email STRING);");
    exec("INSERT INTO like_test (1, 'Alice', 'alice@gmail.com');");
    exec("INSERT INTO like_test (2, 'Bob', 'bob@yahoo.com');");
    exec("INSERT INTO like_test (3, 'Charlie', 'charlie@work.org');");
    exec("INSERT INTO like_test (4, 'Alex', 'alex@email.com');");
    exec("INSERT INTO like_test (5, 'Amanda', 'amanda@mail.com');");

    log_msg(LOG_INFO, "Testing LIKE with % wildcard at end...");
    exec("SELECT * FROM like_test WHERE name LIKE 'A%';");

    log_msg(LOG_INFO, "Testing LIKE with % wildcard at start...");
    exec("SELECT * FROM like_test WHERE email LIKE '%@gmail.com';");

    log_msg(LOG_INFO, "Testing LIKE with % wildcard in middle...");
    exec("SELECT * FROM like_test WHERE name LIKE 'A%a';");

    log_msg(LOG_INFO, "Testing LIKE with % wildcard on both sides...");
    exec("SELECT * FROM like_test WHERE email LIKE '%@%';");

    log_msg(LOG_INFO, "Testing NOT LIKE...");
    exec("SELECT * FROM like_test WHERE name NOT LIKE 'A%';");

    log_msg(LOG_INFO, "Testing LIKE with exact match...");
    exec("SELECT * FROM like_test WHERE name LIKE 'Alice';");

    log_msg(LOG_INFO, "Testing LIKE with no matches...");
    exec("SELECT * FROM like_test WHERE name LIKE 'Z%';");

    log_msg(LOG_INFO, "LIKE pattern tests passed");
}

void test_concat_variations(void) {
    log_msg(LOG_INFO, "Testing CONCAT variations...");

    reset_database();

    exec("CREATE TABLE concat_test (id INT, first_name STRING, last_name STRING, age INT);");
    exec("INSERT INTO concat_test (1, 'John', 'Doe', 30);");
    exec("INSERT INTO concat_test (2, 'Jane', 'Smith', 25);");
    exec("INSERT INTO concat_test (3, 'Bob', 'Johnson', 40);");

    log_msg(LOG_INFO, "Testing basic CONCAT...");
    exec("SELECT CONCAT(first_name, ' ', last_name) FROM concat_test;");

    log_msg(LOG_INFO, "Testing CONCAT with string literals...");
    exec("SELECT CONCAT(first_name, '-', last_name) FROM concat_test;");

    log_msg(LOG_INFO, "Testing CONCAT with multiple columns...");
    exec("SELECT CONCAT(first_name, last_name, age) FROM concat_test;");

    log_msg(LOG_INFO, "Testing nested CONCAT...");
    exec("SELECT CONCAT(CONCAT(first_name, ' '), last_name) FROM concat_test;");

    log_msg(LOG_INFO, "CONCAT variation tests passed");
}

void test_order_by_expressions(void) {
    log_msg(LOG_INFO, "Testing ORDER BY with expressions...");

    reset_database();

    exec("CREATE TABLE order_expr (id INT, a INT, b INT);");
    exec("INSERT INTO order_expr (1, 10, 5);");
    exec("INSERT INTO order_expr (2, 5, 10);");
    exec("INSERT INTO order_expr (3, 15, 3);");
    exec("INSERT INTO order_expr (4, 8, 8);");

    log_msg(LOG_INFO, "Testing ORDER BY single column...");
    exec("SELECT * FROM order_expr ORDER BY a;");

    log_msg(LOG_INFO, "Testing ORDER BY DESC...");
    exec("SELECT * FROM order_expr ORDER BY a DESC;");

    log_msg(LOG_INFO, "Testing ORDER BY with WHERE...");
    exec("SELECT * FROM order_expr WHERE a > 5 ORDER BY b;");

    log_msg(LOG_INFO, "ORDER BY expression tests passed");
}

void test_is_null_conditions(void) {
    log_msg(LOG_INFO, "Testing IS NULL/IS NOT NULL conditions...");

    reset_database();

    exec("CREATE TABLE null_test (id INT, name STRING, value INT);");
    exec("INSERT INTO null_test (1, 'Alice', NULL);");
    exec("INSERT INTO null_test (2, 'Bob', 100);");
    exec("INSERT INTO null_test (3, 'Charlie', NULL);");
    exec("INSERT INTO null_test (4, 'Diana', 200);");

    log_msg(LOG_INFO, "Testing IS NULL...");
    exec("SELECT * FROM null_test WHERE value IS NULL;");

    log_msg(LOG_INFO, "Testing IS NOT NULL...");
    exec("SELECT * FROM null_test WHERE value IS NOT NULL;");

    log_msg(LOG_INFO, "Testing IS NULL with NOT...");
    exec("SELECT * FROM null_test WHERE NOT value IS NULL;");

    log_msg(LOG_INFO, "Testing IS NULL in complex expressions...");
    exec("SELECT * FROM null_test WHERE value IS NULL OR name = 'Bob';");

    log_msg(LOG_INFO, "IS NULL condition tests passed");
}

void test_subqueries(void) {
    log_msg(LOG_INFO, "Testing subqueries...");

    reset_database();

    exec("CREATE TABLE orders (id INT, user_id INT, amount FLOAT);");
    exec("INSERT INTO orders (1, 1, 100.0);");
    exec("INSERT INTO orders (2, 1, 200.0);");
    exec("INSERT INTO orders (3, 2, 150.0);");
    exec("INSERT INTO orders (4, 3, 75.0);");

    exec("CREATE TABLE users (id INT, name STRING);");
    exec("INSERT INTO users (1, 'Alice');");
    exec("INSERT INTO users (2, 'Bob');");
    exec("INSERT INTO users (3, 'Charlie');");

    log_msg(LOG_INFO, "Testing EXISTS subquery...");
    exec("SELECT * FROM users WHERE EXISTS (SELECT * FROM orders WHERE orders.user_id = users.id);");

    log_msg(LOG_INFO, "Testing NOT EXISTS subquery...");
    exec("SELECT * FROM users WHERE NOT EXISTS (SELECT * FROM orders WHERE orders.amount > 500.0);");

    log_msg(LOG_INFO, "Testing correlated subquery...");
    exec("SELECT * FROM users WHERE EXISTS (SELECT * FROM orders WHERE orders.user_id = users.id AND orders.amount > 150.0);");

    log_msg(LOG_INFO, "Testing subquery with aggregate...");
    exec("SELECT * FROM users WHERE EXISTS (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id);");

    log_msg(LOG_INFO, "Testing IN with subquery...");
    exec("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders);");

    log_msg(LOG_INFO, "Testing NOT IN with subquery...");
    exec("SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders WHERE amount > 100.0);");

    log_msg(LOG_INFO, "Testing subquery in HAVING equivalent...");
    exec("SELECT * FROM users WHERE EXISTS (SELECT SUM(amount) FROM orders WHERE orders.user_id = users.id);");

    log_msg(LOG_INFO, "Subquery tests passed");
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

int main(void) {
    set_log_level(LOG_INFO);
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
    test_limits();
    test_data_types();
    test_aggregate_functions();
    test_scalar_functions();
    test_date_time_operations();
    test_date_time_edge_cases();
    test_date_time_aggregations();
    test_date_time_literal_parsing();
    test_queries();
    test_error_conditions();
    test_between_operator();
    test_order_by();
    test_limit();
    test_order_by_with_limit();
    test_distinct();
    test_arithmetic_operations();
    test_arithmetic_in_where();
    test_string_operations();
    test_complex_expressions();
    test_comparison_edge_cases();
    test_aggregate_edge_cases();
    test_scalar_function_edge_cases();
    test_type_conversion();
    test_multiple_aggregates();
    test_date_time_comparisons();
    test_case_sensitivity();
    test_error_parsing();
    test_like_patterns();
    test_concat_variations();
    test_order_by_expressions();
    test_is_null_conditions();
    test_subqueries();

    log_msg(LOG_INFO, "All tests passed!");
    return 0;
}
