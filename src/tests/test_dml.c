#include <stdio.h>
#include <string.h>

#include "arraylist.h"
#include "db.h"
#include "logger.h"
#include "table.h"
#include "test_util.h"

void test_insert_single_row(void) {
    log_msg(LOG_INFO, "Testing INSERT single row...");

    reset_database();

    exec("CREATE TABLE users (id INT, name STRING, age INT);");
    exec("INSERT INTO users VALUES (1, 'Alice', 30);");

    Table* table = find_table_by_name("users");
    assert_ptr_not_null(table, "Table should exist");
    assert_int_eq(1, alist_length(&table->rows), "Table should have 1 row");

    Row* row = (Row*)alist_get(&table->rows, 0);
    assert_ptr_not_null(row, "Row should exist");
    assert_int_eq(3, row->value_count, "Row should have 3 values");

    log_msg(LOG_INFO, "INSERT single row tests passed");
}

void test_insert_multiple_values(void) {
    log_msg(LOG_INFO, "Testing INSERT with multiple value sets...");

    reset_database();

    exec("CREATE TABLE numbers (id INT, value INT);");
    exec("INSERT INTO numbers VALUES (1, 100), (2, 200), (3, 300);");

    Table* table = find_table_by_name("numbers");
    assert_ptr_not_null(table, "Table should exist");
    assert_int_eq(3, alist_length(&table->rows), "Table should have 3 rows");

    log_msg(LOG_INFO, "Testing mixed single and multi-value INSERT...");
    exec("INSERT INTO numbers VALUES (4, 400);");
    assert_int_eq(4, alist_length(&table->rows), "Table should have 4 rows");

    exec("INSERT INTO numbers VALUES (5, 500), (6, 600);");
    assert_int_eq(6, alist_length(&table->rows), "Table should have 6 rows");

    log_msg(LOG_INFO, "INSERT multiple values tests passed");
}

void test_insert_mixed_types(void) {
    log_msg(LOG_INFO, "Testing INSERT with mixed data types...");

    reset_database();

    exec(
        "CREATE TABLE mixed_table ("
        "id INT, "
        "name STRING, "
        "price FLOAT, "
        "active INT"
        ");");

    exec(
        "INSERT INTO mixed_table VALUES "
        "(1, 'Product A', 29.99, 1), "
        "(2, 'Product B', 49.99, 0), "
        "(3, 'Product C', 19.99, 1);");

    Table* table = find_table_by_name("mixed_table");
    assert_ptr_not_null(table, "Table should exist");
    assert_int_eq(3, alist_length(&table->rows), "Table should have 3 rows");

    log_msg(LOG_INFO, "INSERT mixed types tests passed");
}

void test_update_single_row(void) {
    log_msg(LOG_INFO, "Testing UPDATE single row...");

    reset_database();

    exec("CREATE TABLE products (id INT, name STRING, price FLOAT);");
    exec("INSERT INTO products VALUES (1, 'Laptop', 999.99);");
    exec("INSERT INTO products VALUES (2, 'Mouse', 29.99);");

    exec("UPDATE products SET price = 1099.99 WHERE name = 'Laptop';");

    Table* table = find_table_by_name("products");
    Row* row = (Row*)alist_get(&table->rows, 0);
    assert_ptr_not_null(row, "First row should exist");

    log_msg(LOG_INFO, "UPDATE single row tests passed");
}

void test_update_multiple_rows(void) {
    log_msg(LOG_INFO, "Testing UPDATE multiple rows...");

    reset_database();

    exec("CREATE TABLE items (id INT, category STRING, price FLOAT);");
    exec("INSERT INTO items VALUES (1, 'Electronics', 100.00);");
    exec("INSERT INTO items VALUES (2, 'Electronics', 200.00);");
    exec("INSERT INTO items VALUES (3, 'Furniture', 300.00);");

    exec("UPDATE items SET price = price * 1.1 WHERE category = 'Electronics';");

    Table* table = find_table_by_name("items");
    assert_int_eq(3, alist_length(&table->rows), "Table should still have 3 rows");

    log_msg(LOG_INFO, "UPDATE multiple rows tests passed");
}

void test_update_all_rows(void) {
    log_msg(LOG_INFO, "Testing UPDATE all rows...");

    reset_database();

    exec("CREATE TABLE counters (id INT, total INT);");
    exec("INSERT INTO counters VALUES (1, 10);");
    exec("INSERT INTO counters VALUES (2, 20);");
    exec("INSERT INTO counters VALUES (3, 30);");

    exec("UPDATE counters SET total = total + 5;");

    Table* table = find_table_by_name("counters");
    assert_int_eq(3, alist_length(&table->rows), "Table should have 3 rows");

    log_msg(LOG_INFO, "UPDATE all rows tests passed");
}

void test_delete_single_row(void) {
    log_msg(LOG_INFO, "Testing DELETE single row...");

    reset_database();

    exec("CREATE TABLE users (id INT, name STRING);");
    exec("INSERT INTO users VALUES (1, 'Alice');");
    exec("INSERT INTO users VALUES (2, 'Bob');");

    exec("DELETE FROM users WHERE id = 1;");

    Table* table = find_table_by_name("users");
    assert_int_eq(1, alist_length(&table->rows), "Table should have 1 row after delete");

    log_msg(LOG_INFO, "DELETE single row tests passed");
}

void test_delete_multiple_rows(void) {
    log_msg(LOG_INFO, "Testing DELETE multiple rows...");

    reset_database();

    exec("CREATE TABLE orders (id INT, status STRING);");
    exec("INSERT INTO orders VALUES (1, 'completed');");
    exec("INSERT INTO orders VALUES (2, 'completed');");
    exec("INSERT INTO orders VALUES (3, 'pending');");

    exec("DELETE FROM orders WHERE status = 'completed';");

    Table* table = find_table_by_name("orders");
    assert_int_eq(1, alist_length(&table->rows), "Table should have 1 row remaining");

    log_msg(LOG_INFO, "DELETE multiple rows tests passed");
}

void test_delete_all_rows(void) {
    log_msg(LOG_INFO, "Testing DELETE all rows...");

    reset_database();

    exec("CREATE TABLE temp_data (id INT, value INT);");
    exec("INSERT INTO temp_data VALUES (1, 100);");
    exec("INSERT INTO temp_data VALUES (2, 200);");

    exec("DELETE FROM temp_data;");

    Table* table = find_table_by_name("temp_data");
    assert_int_eq(0, alist_length(&table->rows), "Table should be empty after DELETE");

    log_msg(LOG_INFO, "DELETE all rows tests passed");
}

void test_insert_with_comments(void) {
    log_msg(LOG_INFO, "Testing INSERT with comments...");

    reset_database();

    exec("CREATE TABLE comments_test (id INT, value INT); -- Create test table");
    exec("INSERT INTO comments_test VALUES (1, 100); -- Insert first row");
    exec("INSERT INTO comments_test VALUES (2, 200), (3, 300); -- Insert multiple rows");

    Table* table = find_table_by_name("comments_test");
    assert_ptr_not_null(table, "Table should exist");
    assert_int_eq(3, alist_length(&table->rows), "Table should have 3 rows");

    log_msg(LOG_INFO, "INSERT with comments tests passed");
}

void test_multiple_statements(void) {
    log_msg(LOG_INFO, "Testing multiple SQL statements...");

    reset_database();

    bool result = exec("CREATE TABLE multi_test (id INT, name STRING);");
    assert_true(result, "CREATE TABLE should succeed");

    result = exec("INSERT INTO multi_test VALUES (1, 'First');");
    assert_true(result, "First INSERT should succeed");

    result = exec("INSERT INTO multi_test VALUES (2, 'Second');");
    assert_true(result, "Second INSERT should succeed");

    result = exec("INSERT INTO multi_test VALUES (3, 'Third');");
    assert_true(result, "Third INSERT should succeed");

    Table* table = find_table_by_name("multi_test");
    assert_ptr_not_null(table, "Table should exist");
    assert_int_eq(3, alist_length(&table->rows), "Table should have 3 rows");

    log_msg(LOG_INFO, "Multiple SQL statements tests passed");
}
