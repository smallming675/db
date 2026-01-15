#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "db.h"
#include "logger.h"
#include "values.h"

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
static void test_scalar_functions(void);
static void test_date_time_operations(void);
static void test_date_time_edge_cases(void);
static void test_date_time_aggregations(void);
static void test_date_time_literal_parsing(void);
static void test_between_operator(void);
void reset_database(void) { table_count = 0; }

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
    assert(!table->rows[1].is_null[1]);

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
    assert(events->rows[8].is_null[2]);
    assert(events->rows[9].is_null[3]);
    
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
    exec("SELECT * FROM products WHERE price BETWEEN 10.50 AND 10.50;");  // Exact match
    exec("SELECT * FROM products WHERE price BETWEEN 0.0 AND 10.0;");      // No matches
    exec("SELECT * FROM products WHERE price BETWEEN 200.0 AND 300.0;");  // No matches
    
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

    log_msg(LOG_INFO, "All tests passed!");
    return 0;
}
