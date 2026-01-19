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
    Table* categories = find_table_by_name("categories");
    assert_ptr_not_null(categories, "Categories table should exist");

    exec("CREATE TABLE products (product_id INT PRIMARY KEY, name STRING, category_id INT REFERENCES categories(category_id));");

    Table* products = find_table_by_name("products");
    assert_ptr_not_null(products, "Products table should exist");

    ColumnDef* cat_col = (ColumnDef*)alist_get(&products->schema.columns, 2);
    assert_ptr_not_null(cat_col, "category_id column should exist");
    assert_true(cat_col->flags & COL_FLAG_FOREIGN_KEY, "category_id should have FOREIGN KEY flag");
    assert_str_eq("categories", cat_col->references_table, "references_table should be 'categories'");
    assert_str_eq("category_id", cat_col->references_column, "references_column should be 'category_id'");

    log_msg(LOG_INFO, "CREATE TABLE with FOREIGN KEY tests passed");
}

void test_foreign_key_validation(void) {
    log_msg(LOG_INFO, "Testing FOREIGN KEY validation on INSERT...");

    reset_database();

    exec("CREATE TABLE categories (category_id INT PRIMARY KEY, name STRING);");
    exec("INSERT INTO categories VALUES (1, 'Electronics');");
    exec("INSERT INTO categories VALUES (2, 'Books');");

    exec("CREATE TABLE products (product_id INT PRIMARY KEY, name STRING, category_id INT REFERENCES categories(category_id));");

    exec("INSERT INTO products VALUES (101, 'Laptop', 1);");
    assert_int_eq(1, alist_length(&find_table_by_name("products")->rows), "Should insert product with valid FK");

    exec("INSERT INTO products VALUES (102, 'Mouse', 1);");
    assert_int_eq(2, alist_length(&find_table_by_name("products")->rows), "Should insert another product with valid FK");

    log_msg(LOG_INFO, "Testing FOREIGN KEY violation on INSERT...");

    exec("INSERT INTO products VALUES (103, 'Invalid', 999);");
    assert_int_eq(2, alist_length(&find_table_by_name("products")->rows), "Should not insert product with invalid FK");

    exec("INSERT INTO products VALUES (103, 'Also Invalid', 3);");
    assert_int_eq(2, alist_length(&find_table_by_name("products")->rows), "Should not insert product with non-existent FK");

    log_msg(LOG_INFO, "Testing FOREIGN KEY validation on UPDATE...");

    exec("UPDATE products SET category_id = 2 WHERE product_id = 101;");
    Row* row = (Row*)alist_get(&find_table_by_name("products")->rows, 0);
    Value* cat_val = (Value*)alist_get(row, 2);
    assert_int_eq(2, cat_val->int_val, "Should update to valid FK value");

    exec("UPDATE products SET category_id = 999 WHERE product_id = 101;");
    assert_int_eq(2, cat_val->int_val, "Should not update to invalid FK value");

    log_msg(LOG_INFO, "Testing FOREIGN KEY with NULL values...");

    exec("CREATE TABLE nullable_fk (id INT, ref_id INT REFERENCES categories(category_id));");
    exec("INSERT INTO nullable_fk VALUES (1, NULL);");
    assert_int_eq(1, alist_length(&find_table_by_name("nullable_fk")->rows), "NULL FK should be allowed");

    log_msg(LOG_INFO, "FOREIGN KEY validation tests passed");
}

void test_foreign_key_self_reference(void) {
    log_msg(LOG_INFO, "Testing FOREIGN KEY self-reference...");

    reset_database();

    exec("CREATE TABLE employees (emp_id INT PRIMARY KEY, name STRING, manager_id INT REFERENCES employees(emp_id));");
    exec("INSERT INTO employees VALUES (1, 'CEO', NULL);");
    exec("INSERT INTO employees VALUES (2, 'Manager', 1);");
    exec("INSERT INTO employees VALUES (3, 'Worker', 2);");

    assert_int_eq(3, alist_length(&find_table_by_name("employees")->rows), "Should insert employees with valid self-reference");

    log_msg(LOG_INFO, "Testing FOREIGN KEY self-reference violation...");

    exec("INSERT INTO employees VALUES (4, 'Invalid', 999);");
    assert_int_eq(3, alist_length(&find_table_by_name("employees")->rows), "Should not insert with invalid self-reference");

    log_msg(LOG_INFO, "FOREIGN KEY self-reference tests passed");
}

void test_foreign_key_multiple_references(void) {
    log_msg(LOG_INFO, "Testing multiple FOREIGN KEY references...");

    reset_database();

    exec("CREATE TABLE suppliers (supplier_id INT PRIMARY KEY, name STRING);");
    exec("INSERT INTO suppliers VALUES (1, 'Supplier A');");
    exec("INSERT INTO suppliers VALUES (2, 'Supplier B');");

    exec("CREATE TABLE customers (customer_id INT PRIMARY KEY, name STRING);");
    exec("INSERT INTO customers VALUES (1, 'Customer X');");
    exec("INSERT INTO customers VALUES (2, 'Customer Y');");

    exec("CREATE TABLE orders (order_id INT PRIMARY KEY, customer_id INT REFERENCES customers(customer_id), supplier_id INT REFERENCES suppliers(supplier_id));");

    exec("INSERT INTO orders VALUES (1, 1, 1);");
    assert_int_eq(1, alist_length(&find_table_by_name("orders")->rows), "Should insert order with valid FKs");

    exec("INSERT INTO orders VALUES (2, 2, 2);");
    assert_int_eq(2, alist_length(&find_table_by_name("orders")->rows), "Should insert another order");

    log_msg(LOG_INFO, "Testing FOREIGN KEY multiple reference violations...");

    exec("INSERT INTO orders VALUES (3, 1, 999);");
    assert_int_eq(2, alist_length(&find_table_by_name("orders")->rows), "Should not insert with invalid supplier FK");

    exec("INSERT INTO orders VALUES (3, 999, 1);");
    assert_int_eq(2, alist_length(&find_table_by_name("orders")->rows), "Should not insert with invalid customer FK");

    log_msg(LOG_INFO, "FOREIGN KEY multiple references tests passed");
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
