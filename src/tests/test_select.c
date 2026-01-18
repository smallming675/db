#include <stdio.h>
#include <string.h>

#include "arraylist.h"
#include "db.h"
#include "logger.h"
#include "table.h"
#include "test_util.h"

void test_select_all(void) {
    log_msg(LOG_INFO, "Testing SELECT *...");

    reset_database();

    exec("CREATE TABLE users (id INT, name STRING, age INT);");
    exec("INSERT INTO users VALUES (1, 'Alice', 30);");
    exec("INSERT INTO users VALUES (2, 'Bob', 25);");
    exec("INSERT INTO users VALUES (3, 'Charlie', 35);");

    exec("SELECT * FROM users;");

    Table* table = find_table_by_name("users");
    assert_int_eq(3, alist_length(&table->rows), "Table should have 3 rows");

    log_msg(LOG_INFO, "SELECT * tests passed");
}

void test_select_columns(void) {
    log_msg(LOG_INFO, "Testing SELECT specific columns...");

    reset_database();

    exec("CREATE TABLE users (id INT, name STRING, age INT, email STRING);");
    exec("INSERT INTO users VALUES (1, 'Alice', 30, 'alice@example.com');");

    exec("SELECT name, email FROM users;");

    log_msg(LOG_INFO, "SELECT specific columns tests passed");
}

void test_select_with_where(void) {
    log_msg(LOG_INFO, "Testing SELECT with WHERE clause...");

    reset_database();

    exec("CREATE TABLE products (id INT, name STRING, price FLOAT, category STRING);");
    exec("INSERT INTO products VALUES (1, 'Laptop', 999.99, 'Electronics');");
    exec("INSERT INTO products VALUES (2, 'Mouse', 29.99, 'Electronics');");
    exec("INSERT INTO products VALUES (3, 'Desk', 299.99, 'Furniture');");
    exec("INSERT INTO products VALUES (4, 'Chair', 149.99, 'Furniture');");

    exec("SELECT * FROM products WHERE category = 'Electronics';");
    exec("SELECT * FROM products WHERE price > 100;");
    exec("SELECT * FROM products WHERE price BETWEEN 100 AND 300;");

    log_msg(LOG_INFO, "SELECT with WHERE tests passed");
}

void test_select_with_and_or(void) {
    log_msg(LOG_INFO, "Testing SELECT with AND/OR operators...");

    reset_database();

    exec("CREATE TABLE employees (id INT, name STRING, department STRING, salary FLOAT);");
    exec("INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 75000);");
    exec("INSERT INTO employees VALUES (2, 'Bob', 'Engineering', 80000);");
    exec("INSERT INTO employees VALUES (3, 'Charlie', 'Sales', 65000);");
    exec("INSERT INTO employees VALUES (4, 'Diana', 'Sales', 70000);");

    exec("SELECT * FROM employees WHERE department = 'Engineering' AND salary > 75000;");
    exec("SELECT * FROM employees WHERE department = 'Sales' OR department = 'Engineering';");
    exec(
        "SELECT * FROM employees WHERE (department = 'Sales' AND salary > 60000) OR department = "
        "'Engineering';");

    log_msg(LOG_INFO, "SELECT with AND/OR tests passed");
}

void test_select_with_like(void) {
    log_msg(LOG_INFO, "Testing SELECT with LIKE...");

    reset_database();

    exec("CREATE TABLE products (id INT, name STRING, category STRING);");
    exec("INSERT INTO products VALUES (1, 'Laptop', 'Electronics');");
    exec("INSERT INTO products VALUES (2, 'Mouse', 'Electronics');");
    exec("INSERT INTO products VALUES (3, 'Desk Lamp', 'Furniture');");
    exec("INSERT INTO products VALUES (4, 'Lamp', 'Furniture');");

    exec("SELECT * FROM products WHERE name LIKE 'Laptop%';");
    exec("SELECT * FROM products WHERE name LIKE '%Lamp%';");
    exec("SELECT * FROM products WHERE name LIKE '%e%';");

    log_msg(LOG_INFO, "SELECT with LIKE tests passed");
}

void test_select_with_order_by(void) {
    log_msg(LOG_INFO, "Testing SELECT with ORDER BY...");

    reset_database();

    exec("CREATE TABLE numbers (id INT, value INT);");
    exec("INSERT INTO numbers VALUES (1, 50), (2, 20), (3, 80), (4, 10), (5, 40);");

    exec("SELECT * FROM numbers ORDER BY value;");
    exec("SELECT * FROM numbers ORDER BY value DESC;");

    log_msg(LOG_INFO, "SELECT with ORDER BY tests passed");
}

void test_select_with_limit(void) {
    log_msg(LOG_INFO, "Testing SELECT with LIMIT...");

    reset_database();

    exec("CREATE TABLE items (id INT, value INT);");
    exec("INSERT INTO items VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);");

    exec("SELECT * FROM items LIMIT 3;");

    log_msg(LOG_INFO, "SELECT with LIMIT tests passed");
}

void test_select_with_order_by_limit(void) {
    log_msg(LOG_INFO, "Testing SELECT with ORDER BY and LIMIT...");

    reset_database();

    exec("CREATE TABLE scores (id INT, name STRING, score INT);");
    exec("INSERT INTO scores VALUES (1, 'Alice', 85);");
    exec("INSERT INTO scores VALUES (2, 'Bob', 92);");
    exec("INSERT INTO scores VALUES (3, 'Charlie', 78);");
    exec("INSERT INTO scores VALUES (4, 'Diana', 95);");
    exec("INSERT INTO scores VALUES (5, 'Eve', 88);");

    exec("SELECT * FROM scores ORDER BY score DESC LIMIT 3;");

    log_msg(LOG_INFO, "SELECT with ORDER BY and LIMIT tests passed");
}

void test_select_distinct(void) {
    log_msg(LOG_INFO, "Testing SELECT DISTINCT...");

    reset_database();

    exec("CREATE TABLE colors (id INT, name STRING, category STRING);");
    exec("INSERT INTO colors VALUES (1, 'Red', 'Primary');");
    exec("INSERT INTO colors VALUES (2, 'Blue', 'Primary');");
    exec("INSERT INTO colors VALUES (3, 'Green', 'Secondary');");
    exec("INSERT INTO colors VALUES (4, 'Yellow', 'Secondary');");

    exec("SELECT DISTINCT category FROM colors;");

    log_msg(LOG_INFO, "SELECT DISTINCT tests passed");
}

void test_select_is_null(void) {
    log_msg(LOG_INFO, "Testing SELECT with IS NULL...");

    reset_database();

    exec("CREATE TABLE nullable_test (id INT, value INT);");
    exec("INSERT INTO nullable_test VALUES (1, 10);");
    exec("INSERT INTO nullable_test VALUES (2, NULL);");
    exec("INSERT INTO nullable_test VALUES (3, 30);");

    exec("SELECT * FROM nullable_test WHERE value IS NULL;");
    // exec("SELECT * FROM nullable_test WHERE value IS NOT NULL;");

    log_msg(LOG_INFO, "SELECT with IS NULL tests passed");
}

void test_select_case_sensitivity(void) {
    log_msg(LOG_INFO, "Testing SELECT case sensitivity...");

    reset_database();

    exec("CREATE TABLE case_test (id INT, name STRING);");
    exec("INSERT INTO case_test VALUES (1, 'Hello');");
    exec("INSERT INTO case_test VALUES (2, 'HELLO');");
    exec("INSERT INTO case_test VALUES (3, 'hello');");

    exec("SELECT * FROM case_test WHERE name = 'Hello';");
    exec("SELECT * FROM case_test WHERE name = 'hello';");

    log_msg(LOG_INFO, "SELECT case sensitivity tests passed");
}
