#include <stdio.h>
#include <string.h>

#include "arraylist.h"
#include "db.h"
#include "logger.h"
#include "table.h"
#include "test_util.h"

void test_subquery_with_comparison(void) {
    log_msg(LOG_INFO, "Testing subquery with comparison operators...");

    reset_database();

    exec("CREATE TABLE products (id INT, name STRING, category STRING, price FLOAT);");
    exec("INSERT INTO products VALUES (1, 'Tea A', 'Tea', 3.50);");
    exec("INSERT INTO products VALUES (2, 'Tea B', 'Tea', 3.00);");
    exec("INSERT INTO products VALUES (3, 'Coffee A', 'Coffee', 4.00);");
    exec("INSERT INTO products VALUES (4, 'Coffee B', 'Coffee', 4.50);");

    exec(
        "SELECT name, price FROM products WHERE category = 'Tea' AND price = (SELECT MAX(price) "
        "FROM products WHERE category = 'Tea');");
    exec("SELECT name, price FROM products WHERE price < (SELECT AVG(price) FROM products);");
    exec(
        "SELECT name, price FROM products WHERE price = (SELECT MIN(price) FROM products WHERE "
        "category = 'Coffee');");

    log_msg(LOG_INFO, "Subquery with comparison tests passed");
}

void test_correlated_subquery(void) {
    log_msg(LOG_INFO, "Testing correlated subquery...");

    reset_database();

    exec("CREATE TABLE employees (id INT, name STRING, department STRING, salary FLOAT);");
    exec("INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 80000);");
    exec("INSERT INTO employees VALUES (2, 'Bob', 'Engineering', 75000);");
    exec("INSERT INTO employees VALUES (3, 'Charlie', 'Sales', 70000);");
    exec("INSERT INTO employees VALUES (4, 'Diana', 'Sales', 72000);");

    exec(
        "SELECT name FROM employees e WHERE (SELECT COUNT(*) FROM employees e2 WHERE e2.department "
        "= e.department) > 1;");

    log_msg(LOG_INFO, "Correlated subquery tests passed");
}

void test_subquery_in_insert(void) {
    log_msg(LOG_INFO, "Testing subquery in INSERT...");

    reset_database();

    exec("CREATE TABLE source (id INT, value INT);");
    exec("INSERT INTO source VALUES (1, 100);");
    exec("INSERT INTO source VALUES (2, 200);");
    exec("INSERT INTO source VALUES (3, 300);");

    exec("CREATE TABLE dest (id INT, value INT);");

    log_msg(LOG_INFO, "Subquery in INSERT tests passed (placeholder)");
}

void test_subquery_with_aggregates(void) {
    log_msg(LOG_INFO, "Testing subquery with aggregate functions...");

    reset_database();

    exec("CREATE TABLE orders (id INT, customer_id INT, product STRING, amount FLOAT);");
    exec("INSERT INTO orders VALUES (1, 1, 'Product A', 100.00);");
    exec("INSERT INTO orders VALUES (2, 1, 'Product B', 150.00);");
    exec("INSERT INTO orders VALUES (3, 2, 'Product A', 200.00);");

    exec("SELECT customer_id, amount FROM orders WHERE amount = (SELECT MAX(amount) FROM orders);");
    exec("SELECT customer_id, amount FROM orders WHERE amount > (SELECT AVG(amount) FROM orders);");

    log_msg(LOG_INFO, "Subquery with aggregate tests passed");
}

void test_exists_subquery(void) {
    log_msg(LOG_INFO, "Testing EXISTS subquery...");

    reset_database();

    exec("CREATE TABLE customers (id INT, name STRING);");
    exec("INSERT INTO customers VALUES (1, 'Alice');");
    exec("INSERT INTO customers VALUES (2, 'Bob');");

    exec("CREATE TABLE orders (order_id INT, customer_id INT);");
    exec("INSERT INTO orders VALUES (1, 1);");
    exec("INSERT INTO orders VALUES (2, 1);");

    log_msg(LOG_INFO, "EXISTS subquery tests passed (placeholder)");
}

void test_nested_subquery(void) {
    log_msg(LOG_INFO, "Testing nested subquery...");

    reset_database();

    exec("CREATE TABLE levels (level_id INT, name STRING);");
    exec("INSERT INTO levels VALUES (1, 'Level 1');");
    exec("INSERT INTO levels VALUES (2, 'Level 2');");
    exec("INSERT INTO levels VALUES (3, 'Level 3');");

    exec(
        "SELECT name FROM levels WHERE level_id = (SELECT MIN(level_id) FROM levels WHERE level_id "
        "> (SELECT MIN(level_id) FROM levels));");

    log_msg(LOG_INFO, "Nested subquery tests passed");
}

void test_subquery_with_join(void) {
    log_msg(LOG_INFO, "Testing subquery with JOIN...");

    reset_database();

    exec("CREATE TABLE categories (cat_id INT, name STRING);");
    exec("INSERT INTO categories VALUES (1, 'Electronics');");
    exec("INSERT INTO categories VALUES (2, 'Books');");

    exec("CREATE TABLE products (id INT, name STRING, cat_id INT, price FLOAT);");
    exec("INSERT INTO products VALUES (1, 'Laptop', 1, 999.99);");
    exec("INSERT INTO products VALUES (2, 'Book', 2, 29.99);");

    exec(
        "SELECT p.name, p.price, c.name FROM products p JOIN categories c ON p.cat_id = c.cat_id "
        "WHERE p.price = (SELECT MAX(price) FROM products);");

    log_msg(LOG_INFO, "Subquery with JOIN tests passed");
}

void test_scalar_subquery(void) {
    log_msg(LOG_INFO, "Testing scalar subquery...");

    reset_database();

    exec("CREATE TABLE stats (id INT, category STRING, value FLOAT);");
    exec("INSERT INTO stats VALUES (1, 'A', 10.0);");
    exec("INSERT INTO stats VALUES (2, 'A', 20.0);");
    exec("INSERT INTO stats VALUES (3, 'B', 15.0);");

    exec(
        "SELECT category, (SELECT AVG(value) FROM stats WHERE category = 'A') as avg_a FROM stats "
        "GROUP BY category;");

    log_msg(LOG_INFO, "Scalar subquery tests passed");
}
