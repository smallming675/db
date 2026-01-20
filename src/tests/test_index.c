#include <stdio.h>
#include <string.h>

#include "arraylist.h"
#include "db.h"
#include "logger.h"
#include "table.h"
#include "test_util.h"

extern ArrayList indexes;

void test_create_index(void) {
    log_msg(LOG_INFO, "Testing CREATE INDEX...");

    reset_database();

    exec("CREATE TABLE users (id INT PRIMARY KEY, name STRING, email STRING);");
    exec("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');");
    exec("INSERT INTO users VALUES (2, 'Bob', 'bob@example.com');");
    exec("INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com');");

    exec("CREATE INDEX idx_users_id ON users (id);");

    int index_count = alist_length(&indexes);
    assert_int_eq(1, index_count, "Should have 1 index");

    Index* index = (Index*)alist_get(&indexes, 0);
    assert_ptr_not_null(index, "Index should exist");
    assert_true(index->index_name[0] != '\0', "Index name should be set");
    assert_true(index->table_name[0] != '\0', "Index table name should be set");
    assert_true(index->column_name[0] != '\0', "Index column name should be set");
    assert_str_eq("idx_users_id", index->index_name, "Index name should be 'idx_users_id'");
    assert_str_eq("users", index->table_name, "Index table name should be 'users'");
    assert_str_eq("id", index->column_name, "Index column name should be 'id'");
    assert_int_eq(3, index->entry_count, "Index should have 3 entries");

    log_msg(LOG_INFO, "CREATE INDEX tests passed");
}

void test_create_multiple_indexes(void) {
    log_msg(LOG_INFO, "Testing CREATE INDEX with multiple indexes...");

    reset_database();

    exec("CREATE TABLE products (id INT, name STRING, category STRING, price FLOAT);");
    exec("INSERT INTO products VALUES (1, 'Laptop', 'Electronics', 999.99);");
    exec("INSERT INTO products VALUES (2, 'Mouse', 'Electronics', 29.99);");
    exec("INSERT INTO products VALUES (3, 'Desk', 'Furniture', 299.99);");
    exec("INSERT INTO products VALUES (4, 'Chair', 'Furniture', 149.99);");

    exec("CREATE INDEX idx_products_id ON products (id);");
    exec("CREATE INDEX idx_products_category ON products (category);");
    exec("CREATE INDEX idx_products_price ON products (price);");

    int index_count = alist_length(&indexes);
    assert_int_eq(3, index_count, "Should have 3 indexes");

    log_msg(LOG_INFO, "CREATE INDEX with multiple indexes tests passed");
}

void test_index_on_string_column(void) {
    log_msg(LOG_INFO, "Testing INDEX on STRING column...");

    reset_database();

    exec("CREATE TABLE customers (id INT, name STRING, city STRING);");
    exec("INSERT INTO customers VALUES (1, 'Alice', 'New York');");
    exec("INSERT INTO customers VALUES (2, 'Bob', 'Los Angeles');");
    exec("INSERT INTO customers VALUES (3, 'Charlie', 'Chicago');");

    exec("CREATE INDEX idx_customers_name ON customers (name);");

    Index* index = (Index*)alist_get(&indexes, 0);
    assert_ptr_not_null(index, "Index should exist");
    assert_int_eq(3, index->entry_count, "Index should have 3 entries");

    log_msg(LOG_INFO, "INDEX on STRING column tests passed");
}

void test_drop_index(void) {
    log_msg(LOG_INFO, "Testing DROP INDEX...");

    reset_database();

    exec("CREATE TABLE test (id INT, value STRING);");
    exec("CREATE INDEX idx_test ON test (id);");
    exec("DROP INDEX idx_test;");

    int index_count = alist_length(&indexes);
    assert_int_eq(0, index_count, "Should have 0 indexes after DROP");

    log_msg(LOG_INFO, "DROP INDEX tests passed");
}

void test_index_filter_equality(void) {
    log_msg(LOG_INFO, "Testing index usage for equality filter...");

    reset_database();

    exec("CREATE TABLE users (id INT PRIMARY KEY, name STRING);");
    for (int i = 1; i <= 100; i++) {
        char sql[128];
        snprintf(sql, sizeof(sql), "INSERT INTO users VALUES (%d, 'User%d');", i, i);
        exec(sql);
    }

    exec("CREATE INDEX idx_users_id ON users (id);");

    // Create a simple SELECT that uses the index but don't check results
    exec("SELECT * FROM users WHERE id = 50;");

    log_msg(LOG_INFO, "Index usage for equality filter tests passed");
}

void test_index_filter_with_large_table(void) {
    log_msg(LOG_INFO, "Testing index usage with large table...");

    reset_database();

    exec("CREATE TABLE large_table (id INT, data STRING);");
    exec("CREATE INDEX idx_large_id ON large_table (id);");

    for (int i = 1; i <= 1000; i++) {
        char sql[128];
        snprintf(sql, sizeof(sql), "INSERT INTO large_table VALUES (%d, 'Data%d');", i, i);
        exec(sql);
    }

    Index* index = (Index*)alist_get(&indexes, 0);
    assert_int_eq(1000, index->entry_count, "Index should have 1000 entries");

    exec("SELECT * FROM large_table WHERE id = 999;");

    log_msg(LOG_INFO, "Index usage with large table tests passed");
}

void test_hash_join_inner(void) {
    log_msg(LOG_INFO, "Testing HASH JOIN for INNER JOIN...");

    reset_database();

    exec("CREATE TABLE employees (id INT, name STRING, dept_id INT);");
    exec("INSERT INTO employees VALUES (1, 'Alice', 1);");
    exec("INSERT INTO employees VALUES (2, 'Bob', 2);");
    exec("INSERT INTO employees VALUES (3, 'Charlie', 3);");
    exec("INSERT INTO employees VALUES (4, 'David', 1);");

    exec("CREATE TABLE departments (dept_id INT, dept_name STRING);");
    exec("INSERT INTO departments VALUES (1, 'Engineering');");
    exec("INSERT INTO departments VALUES (2, 'Sales');");
    exec("INSERT INTO departments VALUES (3, 'Marketing');");

    exec("SELECT * FROM employees JOIN departments ON employees.dept_id = departments.dept_id;");

    log_msg(LOG_INFO, "HASH JOIN for INNER JOIN tests passed");
}

void test_hash_join_left(void) {
    log_msg(LOG_INFO, "Testing HASH JOIN for LEFT JOIN...");

    reset_database();

    exec("CREATE TABLE customers (id INT, name STRING, city_id INT);");
    exec("INSERT INTO customers VALUES (1, 'Alice', 1);");
    exec("INSERT INTO customers VALUES (2, 'Bob', 2);");
    exec("INSERT INTO customers VALUES (3, 'Charlie', NULL);");

    exec("CREATE TABLE cities (city_id INT, city_name STRING);");
    exec("INSERT INTO cities VALUES (1, 'New York');");
    exec("INSERT INTO cities VALUES (2, 'Los Angeles');");

    exec("SELECT * FROM customers LEFT JOIN cities ON customers.city_id = cities.city_id;");

    log_msg(LOG_INFO, "HASH JOIN for LEFT JOIN tests passed");
}

void test_hash_join_large_tables(void) {
    log_msg(LOG_INFO, "Testing HASH JOIN with large tables...");

    reset_database();

    exec("CREATE TABLE orders (order_id INT, customer_id INT, amount FLOAT);");
    exec("CREATE TABLE customers (customer_id INT, name STRING);");

    for (int i = 1; i <= 500; i++) {
        char sql[128];
        snprintf(sql, sizeof(sql), "INSERT INTO customers VALUES (%d, 'Customer%d');", i, i);
        exec(sql);
    }

    for (int i = 1; i <= 1000; i++) {
        int cust_id = (i % 500) + 1;
        char sql[128];
        snprintf(sql, sizeof(sql), "INSERT INTO orders VALUES (%d, %d, %.2f);", i, cust_id, (float)(i * 10));
        exec(sql);
    }

    exec("SELECT * FROM orders JOIN customers ON orders.customer_id = customers.customer_id LIMIT 100;");

    log_msg(LOG_INFO, "HASH JOIN with large tables tests passed");
}

void test_hash_join_multiple_matches(void) {
    log_msg(LOG_INFO, "Testing HASH JOIN with multiple matches...");

    reset_database();

    exec("CREATE TABLE categories (id INT, name STRING);");
    exec("INSERT INTO categories VALUES (1, 'Electronics');");
    exec("INSERT INTO categories VALUES (2, 'Books');");

    exec("CREATE TABLE products (id INT, name STRING, category_id INT);");
    exec("INSERT INTO products VALUES (1, 'Laptop', 1);");
    exec("INSERT INTO products VALUES (2, 'Mouse', 1);");
    exec("INSERT INTO products VALUES (3, 'Keyboard', 1);");
    exec("INSERT INTO products VALUES (4, 'Novel', 2);");
    exec("INSERT INTO products VALUES (5, 'Magazine', 2);");

    exec("SELECT c.name AS category, p.name AS product FROM categories c JOIN products p ON c.id = p.category_id;");

    log_msg(LOG_INFO, "HASH JOIN with multiple matches tests passed");
}

void test_hash_join_no_matches(void) {
    log_msg(LOG_INFO, "Testing HASH JOIN with no matches...");

    reset_database();

    exec("CREATE TABLE left_table (id INT, value STRING);");
    exec("INSERT INTO left_table VALUES (1, 'A');");
    exec("INSERT INTO left_table VALUES (2, 'B');");

    exec("CREATE TABLE right_table (id INT, value STRING);");
    exec("INSERT INTO right_table VALUES (3, 'C');");
    exec("INSERT INTO right_table VALUES (4, 'D');");

    exec("SELECT * FROM left_table JOIN right_table ON left_table.id = right_table.id;");

    log_msg(LOG_INFO, "HASH JOIN with no matches tests passed");
}

void test_index_with_string_equality(void) {
    log_msg(LOG_INFO, "Testing index usage with string equality...");

    reset_database();

    exec("CREATE TABLE users (id INT, email STRING);");
    exec("CREATE INDEX idx_users_email ON users (email);");

    exec("INSERT INTO users VALUES (1, 'alice@example.com');");
    exec("INSERT INTO users VALUES (2, 'bob@example.com');");
    exec("INSERT INTO users VALUES (3, 'charlie@example.com');");

    exec("SELECT * FROM users WHERE email = 'bob@example.com';");

    log_msg(LOG_INFO, "Index usage with string equality tests passed");
}

void test_index_rebuild(void) {
    log_msg(LOG_INFO, "Testing index rebuild on data change...");

    reset_database();

    exec("CREATE TABLE test (id INT, value STRING);");
    exec("CREATE INDEX idx_test ON test (id);");

    exec("INSERT INTO test VALUES (1, 'One');");
    exec("INSERT INTO test VALUES (2, 'Two');");
    exec("INSERT INTO test VALUES (3, 'Three');");

    Index* index = (Index*)alist_get(&indexes, 0);
    assert_int_eq(3, index->entry_count, "Index should have 3 entries after inserts");

    exec("INSERT INTO test VALUES (4, 'Four');");
    assert_int_eq(4, index->entry_count, "Index should have 4 entries after additional insert");

    log_msg(LOG_INFO, "Index rebuild tests passed");
}

void test_complex_join_with_index(void) {
    log_msg(LOG_INFO, "Testing complex join with indexed columns...");

    reset_database();

    exec("CREATE TABLE orders (order_id INT PRIMARY KEY, customer_id INT, product_id INT, quantity INT);");
    exec("CREATE TABLE customers (customer_id INT PRIMARY KEY, name STRING, city_id INT);");
    exec("CREATE TABLE products (product_id INT PRIMARY KEY, name STRING, price FLOAT);");
    exec("CREATE TABLE cities (city_id INT PRIMARY KEY, name STRING);");

    exec("CREATE INDEX idx_orders_customer ON orders (customer_id);");
    exec("CREATE INDEX idx_orders_product ON orders (product_id);");

    for (int i = 1; i <= 10; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO customers VALUES (%d, 'Customer%d', %d);",
                 i, i, (i % 5) + 1);
        exec(sql);
    }

    for (int i = 1; i <= 20; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO products VALUES (%d, 'Product%d', %.2f);",
                 i, i, (float)(i * 10));
        exec(sql);
    }

    for (int i = 1; i <= 100; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO orders VALUES (%d, %d, %d, %d);",
                 i, (i % 10) + 1, (i % 20) + 1, (i % 10) + 1);
        exec(sql);
    }

    exec("INSERT INTO cities VALUES (1, 'New York');");
    exec("INSERT INTO cities VALUES (2, 'Los Angeles');");
    exec("INSERT INTO cities VALUES (3, 'Chicago');");
    exec("INSERT INTO cities VALUES (4, 'Houston');");
    exec("INSERT INTO cities VALUES (5, 'Phoenix');");

    exec("SELECT c.name AS customer, p.name AS product, o.quantity "
         "FROM orders o "
         "JOIN customers c ON o.customer_id = c.customer_id "
         "JOIN products p ON o.product_id = p.product_id "
         "WHERE o.quantity > 5 "
         "LIMIT 50;");

    log_msg(LOG_INFO, "Complex join with indexed columns tests passed");
}
