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
    assert_int_eq(3, alist_length(row), "Row should have 3 values");

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

void test_insert_with_column_names(void) {
    log_msg(LOG_INFO, "Testing INSERT with explicit column names...");

    reset_database();

    exec("CREATE TABLE test (id INT, name STRING, age INT, city STRING);");

    exec("INSERT INTO test (id, name) VALUES (1, 'Alice');");
    Table* table = find_table_by_name("test");
    assert_ptr_not_null(table, "Table should exist");
    assert_int_eq(1, alist_length(&table->rows), "Table should have 1 row");

    Row* row = (Row*)alist_get(&table->rows, 0);
    assert_ptr_not_null(row, "Row should exist");
    assert_int_eq(4, alist_length(row), "Row should have 4 values (NULLs for missing columns)");

    log_msg(LOG_INFO, "Testing INSERT with subset of columns...");
    exec("INSERT INTO test (name, city) VALUES ('Bob', 'NYC');");
    assert_int_eq(2, alist_length(&table->rows), "Table should have 2 rows");

    exec("INSERT INTO test (age, id, name) VALUES (25, 3, 'Charlie');");
    assert_int_eq(3, alist_length(&table->rows), "Table should have 3 rows");

    log_msg(LOG_INFO, "Testing INSERT with all columns in different order...");
    exec("INSERT INTO test (city, age, name, id) VALUES ('LA', 35, 'Diana', 4);");
    assert_int_eq(4, alist_length(&table->rows), "Table should have 4 rows");

    log_msg(LOG_INFO, "Testing INSERT with column names and multiple value sets...");
    exec("INSERT INTO test (id, name) VALUES (5, 'Eve'), (6, 'Frank');");
    assert_int_eq(6, alist_length(&table->rows), "Table should have 6 rows");

    log_msg(LOG_INFO, "INSERT with explicit column names tests passed");
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

void test_update_with_expression(void) {
    log_msg(LOG_INFO, "Testing UPDATE with expressions...");

    reset_database();

    exec("CREATE TABLE accounts (id INT, balance FLOAT);");
    exec("INSERT INTO accounts VALUES (1, 1000.00);");
    exec("INSERT INTO accounts VALUES (2, 2500.00);");
    exec("INSERT INTO accounts VALUES (3, 500.00);");

    exec("UPDATE accounts SET balance = balance * 1.1 WHERE balance > 1000;");
    exec("UPDATE accounts SET balance = balance + 100 WHERE balance < 1000;");

    Table* table = find_table_by_name("accounts");
    assert_int_eq(3, alist_length(&table->rows), "Table should have 3 rows");

    log_msg(LOG_INFO, "UPDATE with expressions tests passed");
}

void test_update_multiple_columns(void) {
    log_msg(LOG_INFO, "Testing UPDATE with multiple columns...");

    reset_database();

    exec("CREATE TABLE products (id INT, name STRING, price FLOAT, quantity INT);");
    exec("INSERT INTO products VALUES (1, 'Widget', 19.99, 100);");
    exec("INSERT INTO products VALUES (2, 'Gadget', 29.99, 50);");

    exec("UPDATE products SET price = 24.99, quantity = 75 WHERE name = 'Widget';");

    Table* table = find_table_by_name("products");
    assert_int_eq(2, alist_length(&table->rows), "Table should have 2 rows");

    log_msg(LOG_INFO, "UPDATE multiple columns tests passed");
}

void test_update_no_match(void) {
    log_msg(LOG_INFO, "Testing UPDATE with no matching rows...");

    reset_database();

    exec("CREATE TABLE test_update (id INT, value INT);");
    exec("INSERT INTO test_update VALUES (1, 100);");
    exec("INSERT INTO test_update VALUES (2, 200);");

    exec("UPDATE test_update SET value = 999 WHERE value > 1000;");

    Table* table = find_table_by_name("test_update");
    assert_int_eq(2, alist_length(&table->rows), "Table should still have 2 rows");

    log_msg(LOG_INFO, "UPDATE with no match tests passed");
}

void test_delete_with_and_or(void) {
    log_msg(LOG_INFO, "Testing DELETE with AND/OR conditions...");

    reset_database();

    exec("CREATE TABLE tasks (id INT, name STRING, priority INT, status STRING);");
    exec("INSERT INTO tasks VALUES (1, 'Task A', 1, 'pending');");
    exec("INSERT INTO tasks VALUES (2, 'Task B', 2, 'completed');");
    exec("INSERT INTO tasks VALUES (3, 'Task C', 1, 'completed');");
    exec("INSERT INTO tasks VALUES (4, 'Task D', 3, 'pending');");

    exec("DELETE FROM tasks WHERE priority = 1 AND status = 'pending';");
    exec("DELETE FROM tasks WHERE priority = 3 OR status = 'completed';");

    Table* table = find_table_by_name("tasks");
    assert_int_eq(0, alist_length(&table->rows), "Table should be empty after deletes");

    log_msg(LOG_INFO, "DELETE with AND/OR tests passed");
}

void test_insert_empty_values(void) {
    log_msg(LOG_INFO, "Testing INSERT with empty table...");

    reset_database();

    exec("CREATE TABLE empty_test (id INT, name STRING);");
    exec("INSERT INTO empty_test VALUES (1, 'First');");

    Table* table = find_table_by_name("empty_test");
    assert_int_eq(1, alist_length(&table->rows), "Table should have 1 row");

    log_msg(LOG_INFO, "INSERT with empty table tests passed");
}

void test_insert_string_with_spaces(void) {
    log_msg(LOG_INFO, "Testing INSERT with strings containing spaces...");

    reset_database();

    exec("CREATE TABLE phrases (id INT, phrase STRING);");
    exec("INSERT INTO phrases VALUES (1, 'Hello World');");
    exec("INSERT INTO phrases VALUES (2, 'Test Phrase With Multiple Words');");

    Table* table = find_table_by_name("phrases");
    assert_int_eq(2, alist_length(&table->rows), "Table should have 2 rows");

    log_msg(LOG_INFO, "INSERT with spaces in strings tests passed");
}

void test_update_same_value(void) {
    log_msg(LOG_INFO, "Testing UPDATE setting same value...");

    reset_database();

    exec("CREATE TABLE same_value (id INT, status STRING);");
    exec("INSERT INTO same_value VALUES (1, 'active');");
    exec("INSERT INTO same_value VALUES (2, 'active');");
    exec("INSERT INTO same_value VALUES (3, 'inactive');");

    exec("UPDATE same_value SET status = 'active' WHERE status = 'active';");

    Table* table = find_table_by_name("same_value");
    assert_int_eq(3, alist_length(&table->rows), "Table should still have 3 rows");

    log_msg(LOG_INFO, "UPDATE with same value tests passed");
}

void test_insert_select_new_types(void) {
    log_msg(LOG_INFO, "Testing INSERT and SELECT with new data types...");

    reset_database();

    exec("CREATE TABLE new_types (id INT, active BOOLEAN, price DECIMAL, description BLOB);");

    exec("INSERT INTO new_types VALUES (1, TRUE, 99.99, 'Product A');");
    exec("INSERT INTO new_types VALUES (2, FALSE, 150.50, 'Product B');");
    exec("INSERT INTO new_types VALUES (3, TRUE, 75.00, NULL);");

    Table* table = find_table_by_name("new_types");
    assert_int_eq(3, alist_length(&table->rows), "Table should have 3 rows");

    Row* row = (Row*)alist_get(&table->rows, 0);
    assert_ptr_not_null(row, "First row should exist");
    assert_int_eq(4, alist_length(row), "Row should have 4 values");

    Value* val = (Value*)alist_get(row, 0);
    assert_int_eq(TYPE_INT, val->type, "First value should be INT");
    assert_int_eq(1, val->int_val, "id should be 1");

    val = (Value*)alist_get(row, 1);
    assert_int_eq(TYPE_BOOLEAN, val->type, "Second value should be BOOLEAN");
    assert_true(val->bool_val, "active should be TRUE");

    val = (Value*)alist_get(row, 2);
    assert_true(val->type == TYPE_FLOAT || val->type == TYPE_DECIMAL, "Third value should be numeric");

    val = (Value*)alist_get(row, 3);
    assert_int_eq(TYPE_STRING, val->type, "Fourth value should be STRING");
    assert_true(val->char_val && strcmp(val->char_val, "Product A") == 0, "description should be 'Product A'");

    row = (Row*)alist_get(&table->rows, 1);
    val = (Value*)alist_get(row, 1);
    assert_int_eq(TYPE_BOOLEAN, val->type, "Second row active should be BOOLEAN");
    assert_true(!val->bool_val, "active should be FALSE");

    row = (Row*)alist_get(&table->rows, 2);
    val = (Value*)alist_get(row, 3);
    assert_int_eq(TYPE_NULL, val->type, "Third row description should be NULL");

    log_msg(LOG_INFO, "INSERT and SELECT with new data types tests passed");
}
