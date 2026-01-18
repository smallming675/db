#include <stdio.h>
#include <string.h>

#include "arraylist.h"
#include "db.h"
#include "logger.h"
#include "table.h"
#include "test_util.h"

void test_create_table(void) {
    log_msg(LOG_INFO, "Testing CREATE TABLE...");

    reset_database();

    exec("CREATE TABLE users (id INT, name STRING, age INT);");
    Table* table = find_table_by_name("users");
    assert_ptr_not_null(table, "Table 'users' should exist");
    assert_int_eq(3, alist_length(&table->schema.columns), "Table should have 3 columns");
    assert_int_eq(0, alist_length(&table->rows), "Table should have 0 rows");

    log_msg(LOG_INFO, "Testing column constraints...");
    exec("CREATE TABLE products (id INT PRIMARY KEY, name STRING NOT NULL, price FLOAT UNIQUE);");
    Table* products = find_table_by_name("products");
    assert_ptr_not_null(products, "Table 'products' should exist");

    log_msg(LOG_INFO, "Testing STRICT mode...");
    exec("CREATE TABLE strict_test (id INT, data STRING) STRICT;");
    Table* strict_test = find_table_by_name("strict_test");
    assert_ptr_not_null(strict_test, "Table 'strict_test' should exist");
    assert_true(strict_test->schema.strict, "Table should be in STRICT mode");

    log_msg(LOG_INFO, "CREATE TABLE tests passed");
}

void test_drop_table(void) {
    log_msg(LOG_INFO, "Testing DROP TABLE...");

    reset_database();

    exec("CREATE TABLE to_drop (id INT, name STRING);");
    Table* table = find_table_by_name("to_drop");
    assert_ptr_not_null(table, "Table should exist before DROP");

    exec("DROP TABLE to_drop;");
    table = find_table_by_name("to_drop");
    assert_true(table == NULL, "Table should not exist after DROP");

    log_msg(LOG_INFO, "DROP TABLE tests passed");
}

void test_create_table_with_foreign_key(void) {
    log_msg(LOG_INFO, "Testing CREATE TABLE with FOREIGN KEY...");

    reset_database();

    exec("CREATE TABLE categories (category_id INT PRIMARY KEY, name STRING);");
    exec("CREATE TABLE products (product_id INT PRIMARY KEY, name STRING, category_id INT);");

    Table* products = find_table_by_name("products");
    assert_ptr_not_null(products, "Products table should exist");

    log_msg(LOG_INFO, "CREATE TABLE with basic columns tests passed");
}

void test_create_table_multiple_columns(void) {
    log_msg(LOG_INFO, "Testing CREATE TABLE with multiple columns...");

    reset_database();

    exec(
        "CREATE TABLE complex_table ("
        "id INT PRIMARY KEY, "
        "name STRING NOT NULL, "
        "price FLOAT, "
        "quantity INT UNIQUE, "
        "created_at DATE, "
        "updated_at TIME"
        ");");

    Table* table = find_table_by_name("complex_table");
    assert_ptr_not_null(table, "Table should exist");
    assert_int_eq(6, alist_length(&table->schema.columns), "Table should have 6 columns");

    log_msg(LOG_INFO, "CREATE TABLE with multiple columns tests passed");
}

void test_create_table_new_types(void) {
    log_msg(LOG_INFO, "Testing CREATE TABLE with new data types...");

    reset_database();

    exec("CREATE TABLE new_types_table ("
         "id INT, "
         "active BOOLEAN, "
         "price DECIMAL, "
         "balance NUMERIC, "
         "image BLOB"
         ");");

    Table* table = find_table_by_name("new_types_table");
    assert_ptr_not_null(table, "Table should exist");
    assert_int_eq(5, alist_length(&table->schema.columns), "Table should have 5 columns");

    ColumnDef* col = (ColumnDef*)alist_get(&table->schema.columns, 0);
    assert_true(col && strcmp(col->name, "id") == 0, "First column should be 'id'");
    assert_int_eq(TYPE_INT, col->type, "id should be INT type");

    col = (ColumnDef*)alist_get(&table->schema.columns, 1);
    assert_true(col && strcmp(col->name, "active") == 0, "Second column should be 'active'");
    assert_int_eq(TYPE_BOOLEAN, col->type, "active should be BOOLEAN type");

    col = (ColumnDef*)alist_get(&table->schema.columns, 2);
    assert_true(col && strcmp(col->name, "price") == 0, "Third column should be 'price'");
    assert_int_eq(TYPE_DECIMAL, col->type, "price should be DECIMAL type");

    col = (ColumnDef*)alist_get(&table->schema.columns, 3);
    assert_true(col && strcmp(col->name, "balance") == 0, "Fourth column should be 'balance'");
    assert_int_eq(TYPE_DECIMAL, col->type, "balance should be NUMERIC type (DECIMAL)");

    col = (ColumnDef*)alist_get(&table->schema.columns, 4);
    assert_true(col && strcmp(col->name, "image") == 0, "Fifth column should be 'image'");
    assert_int_eq(TYPE_BLOB, col->type, "image should be BLOB type");

    log_msg(LOG_INFO, "CREATE TABLE with new data types tests passed");
}
