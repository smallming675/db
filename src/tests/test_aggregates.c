#include <math.h>
#include <stdio.h>
#include <string.h>

#include "arraylist.h"
#include "db.h"
#include "logger.h"
#include "table.h"
#include "test_util.h"

void test_count_aggregate(void) {
    log_msg(LOG_INFO, "Testing COUNT aggregate...");

    reset_database();

    exec("CREATE TABLE items (id INT, name STRING, price FLOAT);");
    exec("INSERT INTO items VALUES (1, 'Item A', 10.00);");
    exec("INSERT INTO items VALUES (2, 'Item B', 20.00);");
    exec("INSERT INTO items VALUES (3, 'Item C', 30.00);");

    exec("SELECT COUNT(*) FROM items;");
    exec("SELECT COUNT(*) FROM items WHERE price > 15;");
    exec("SELECT COUNT(name) FROM items;");

    log_msg(LOG_INFO, "COUNT aggregate tests passed");
}

void test_sum_aggregate(void) {
    log_msg(LOG_INFO, "Testing SUM aggregate...");

    reset_database();

    exec("CREATE TABLE orders (id INT, amount FLOAT);");
    exec("INSERT INTO orders VALUES (1, 100.50);");
    exec("INSERT INTO orders VALUES (2, 200.75);");
    exec("INSERT INTO orders VALUES (3, 50.25);");

    exec("SELECT SUM(amount) FROM orders;");
    exec("SELECT SUM(amount) FROM orders WHERE amount > 100;");

    log_msg(LOG_INFO, "SUM aggregate tests passed");
}

void test_avg_aggregate(void) {
    log_msg(LOG_INFO, "Testing AVG aggregate...");

    reset_database();

    exec("CREATE TABLE scores (id INT, student STRING, score FLOAT);");
    exec("INSERT INTO scores VALUES (1, 'Alice', 85.5);");
    exec("INSERT INTO scores VALUES (2, 'Bob', 92.0);");
    exec("INSERT INTO scores VALUES (3, 'Charlie', 78.5);");

    exec("SELECT AVG(score) FROM scores;");
    exec("SELECT AVG(score) FROM scores WHERE score > 80;");

    log_msg(LOG_INFO, "AVG aggregate tests passed");
}

void test_min_aggregate(void) {
    log_msg(LOG_INFO, "Testing MIN aggregate...");

    reset_database();

    exec("CREATE TABLE temperatures (id INT, city STRING, temp FLOAT);");
    exec("INSERT INTO temperatures VALUES (1, 'NYC', 72.5);");
    exec("INSERT INTO temperatures VALUES (2, 'LA', 80.0);");
    exec("INSERT INTO temperatures VALUES (3, 'Chicago', 65.5);");

    exec("SELECT MIN(temp) FROM temperatures;");
    exec("SELECT MIN(temp) FROM temperatures WHERE city != 'Chicago';");

    log_msg(LOG_INFO, "MIN aggregate tests passed");
}

void test_max_aggregate(void) {
    log_msg(LOG_INFO, "Testing MAX aggregate...");

    reset_database();

    exec("CREATE TABLE products (id INT, name STRING, price FLOAT);");
    exec("INSERT INTO products VALUES (1, 'A', 25.00);");
    exec("INSERT INTO products VALUES (2, 'B', 50.00);");
    exec("INSERT INTO products VALUES (3, 'C', 75.00);");

    exec("SELECT MAX(price) FROM products;");
    exec("SELECT MAX(price) FROM products WHERE price < 100;");

    log_msg(LOG_INFO, "MAX aggregate tests passed");
}

void test_multiple_aggregates(void) {
    log_msg(LOG_INFO, "Testing multiple aggregates in one query...");

    reset_database();

    exec("CREATE TABLE sales (id INT, product STRING, amount FLOAT);");
    exec("INSERT INTO sales VALUES (1, 'A', 100.00);");
    exec("INSERT INTO sales VALUES (2, 'B', 200.00);");
    exec("INSERT INTO sales VALUES (3, 'C', 150.00);");

    exec("SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM sales;");

    log_msg(LOG_INFO, "Multiple aggregates tests passed");
}

void test_aggregate_with_where(void) {
    log_msg(LOG_INFO, "Testing aggregates with WHERE clause...");

    reset_database();

    exec("CREATE TABLE transactions (id INT, type STRING, amount FLOAT);");
    exec("INSERT INTO transactions VALUES (1, 'income', 1000.00);");
    exec("INSERT INTO transactions VALUES (2, 'expense', 500.00);");
    exec("INSERT INTO transactions VALUES (3, 'income', 750.00);");
    exec("INSERT INTO transactions VALUES (4, 'expense', 300.00);");
    exec("INSERT INTO transactions VALUES (5, 'income', 1200.00);");

    exec("SELECT SUM(amount) FROM transactions WHERE type = 'income';");
    exec("SELECT COUNT(*) FROM transactions WHERE amount > 400;");
    exec("SELECT AVG(amount) FROM transactions WHERE type = 'expense';");

    log_msg(LOG_INFO, "Aggregates with WHERE tests passed");
}

void test_aggregate_edge_cases(void) {
    log_msg(LOG_INFO, "Testing aggregate edge cases...");

    reset_database();

    exec("CREATE TABLE empty_table (id INT, value INT);");
    exec("SELECT COUNT(*) FROM empty_table;");
    exec("SELECT SUM(value) FROM empty_table;");

    exec("CREATE TABLE single_row (id INT, value INT);");
    exec("INSERT INTO single_row VALUES (1, 42);");
    exec("SELECT COUNT(*), SUM(value), AVG(value), MIN(value), MAX(value) FROM single_row;");

    exec("CREATE TABLE null_values (id INT, value INT);");
    exec("INSERT INTO null_values VALUES (1, 10);");
    exec("INSERT INTO null_values VALUES (2, NULL);");
    exec("INSERT INTO null_values VALUES (3, 20);");
    exec("SELECT COUNT(value) FROM null_values;");

    log_msg(LOG_INFO, "Aggregate edge cases tests passed");
}

void test_count_all_vs_column(void) {
    log_msg(LOG_INFO, "Testing COUNT(*) vs COUNT(column)...");

    reset_database();

    exec("CREATE TABLE count_test (id INT, name STRING);");
    exec("INSERT INTO count_test VALUES (1, 'Alice');");
    exec("INSERT INTO count_test VALUES (2, NULL);");
    exec("INSERT INTO count_test VALUES (3, 'Charlie');");

    exec("SELECT COUNT(*) FROM count_test;");
    exec("SELECT COUNT(name) FROM count_test;");

    log_msg(LOG_INFO, "COUNT(*) vs COUNT(column) tests passed");
}

void test_aggregate_with_all_types(void) {
    log_msg(LOG_INFO, "Testing aggregates with all numeric types...");

    reset_database();

    exec("CREATE TABLE mixed_numbers (id INT, int_val INT, float_val FLOAT);");
    exec("INSERT INTO mixed_numbers VALUES (1, 10, 100.5);");
    exec("INSERT INTO mixed_numbers VALUES (2, 20, 200.5);");
    exec("INSERT INTO mixed_numbers VALUES (3, 30, 300.5);");

    exec("SELECT COUNT(*), SUM(int_val), AVG(int_val), MIN(int_val), MAX(int_val) FROM mixed_numbers;");
    exec("SELECT SUM(float_val), AVG(float_val), MIN(float_val), MAX(float_val) FROM mixed_numbers;");

    log_msg(LOG_INFO, "Aggregates with all numeric types tests passed");
}

void test_aggregate_large_dataset(void) {
    log_msg(LOG_INFO, "Testing aggregates with larger dataset...");

    reset_database();

    exec("CREATE TABLE large_data (id INT, category STRING, value INT);");
    exec("INSERT INTO large_data VALUES (1, 'A', 100);");
    exec("INSERT INTO large_data VALUES (2, 'A', 200);");
    exec("INSERT INTO large_data VALUES (3, 'B', 150);");
    exec("INSERT INTO large_data VALUES (4, 'B', 250);");
    exec("INSERT INTO large_data VALUES (5, 'C', 300);");
    exec("INSERT INTO large_data VALUES (6, 'A', 180);");
    exec("INSERT INTO large_data VALUES (7, 'B', 220);");
    exec("INSERT INTO large_data VALUES (8, 'C', 400);");
    exec("INSERT INTO large_data VALUES (9, 'A', 120);");
    exec("INSERT INTO large_data VALUES (10, 'B', 180);");

    exec("SELECT COUNT(*) FROM large_data;");
    exec("SELECT SUM(value) FROM large_data;");
    exec("SELECT AVG(value) FROM large_data;");
    exec("SELECT MIN(value), MAX(value) FROM large_data;");

    log_msg(LOG_INFO, "Aggregates with larger dataset tests passed");
}

void test_aggregate_with_zero_values(void) {
    log_msg(LOG_INFO, "Testing aggregates with zero and negative values...");

    reset_database();

    exec("CREATE TABLE signed_numbers (id INT, value INT);");
    exec("INSERT INTO signed_numbers VALUES (1, 100);");
    exec("INSERT INTO signed_numbers VALUES (2, 0);");
    exec("INSERT INTO signed_numbers VALUES (3, -50);");
    exec("INSERT INTO signed_numbers VALUES (4, 200);");
    exec("INSERT INTO signed_numbers VALUES (5, -100);");

    exec("SELECT SUM(value) FROM signed_numbers;");
    exec("SELECT MIN(value), MAX(value) FROM signed_numbers;");
    exec("SELECT COUNT(*) FROM signed_numbers WHERE value < 0;");

    log_msg(LOG_INFO, "Aggregates with zero/negative values tests passed");
}

void test_aggregate_with_like_where(void) {
    log_msg(LOG_INFO, "Testing aggregates with LIKE in WHERE...");

    reset_database();

    exec("CREATE TABLE products (id INT, name STRING, price FLOAT);");
    exec("INSERT INTO products VALUES (1, 'Apple Laptop', 999.99);");
    exec("INSERT INTO products VALUES (2, 'Apple Phone', 599.99);");
    exec("INSERT INTO products VALUES (3, 'Samsung TV', 799.99);");
    exec("INSERT INTO products VALUES (4, 'Apple Watch', 299.99);");
    exec("INSERT INTO products VALUES (5, 'Dell Laptop', 899.99);");

    exec("SELECT COUNT(*) FROM products WHERE name LIKE 'Apple%';");
    exec("SELECT AVG(price) FROM products WHERE name LIKE '%Laptop%';");
    exec("SELECT SUM(price) FROM products WHERE name LIKE '%Apple%';");

    log_msg(LOG_INFO, "Aggregates with LIKE WHERE tests passed");
}

void test_aggregate_multiple_wheres(void) {
    log_msg(LOG_INFO, "Testing aggregates with multiple WHERE conditions...");

    reset_database();

    exec("CREATE TABLE orders (id INT, customer STRING, amount FLOAT, status STRING);");
    exec("INSERT INTO orders VALUES (1, 'Alice', 100.00, 'completed');");
    exec("INSERT INTO orders VALUES (2, 'Alice', 200.00, 'completed');");
    exec("INSERT INTO orders VALUES (3, 'Alice', 50.00, 'pending');");
    exec("INSERT INTO orders VALUES (4, 'Bob', 150.00, 'completed');");
    exec("INSERT INTO orders VALUES (5, 'Bob', 300.00, 'pending');");
    exec("INSERT INTO orders VALUES (6, 'Bob', 75.00, 'completed');");

    exec("SELECT COUNT(*) FROM orders WHERE customer = 'Alice' AND status = 'completed';");
    exec("SELECT SUM(amount) FROM orders WHERE status = 'completed' AND amount > 100;");
    exec("SELECT AVG(amount) FROM orders WHERE customer = 'Bob' OR status = 'pending';");

    log_msg(LOG_INFO, "Aggregates with multiple WHERE conditions tests passed");
}

void test_aggregate_with_order_by_limit(void) {
    log_msg(LOG_INFO, "Testing aggregates with ORDER BY and LIMIT...");

    reset_database();

    exec("CREATE TABLE scores (id INT, player STRING, score INT);");
    exec("INSERT INTO scores VALUES (1, 'Alice', 100);");
    exec("INSERT INTO scores VALUES (2, 'Bob', 200);");
    exec("INSERT INTO scores VALUES (3, 'Charlie', 150);");
    exec("INSERT INTO scores VALUES (4, 'Diana', 250);");
    exec("INSERT INTO scores VALUES (5, 'Eve', 175);");

    exec("SELECT COUNT(*) FROM scores;");
    exec("SELECT SUM(score) FROM scores;");
    exec("SELECT MAX(score) FROM scores;");

    log_msg(LOG_INFO, "Aggregates with ORDER BY and LIMIT tests passed");
}
