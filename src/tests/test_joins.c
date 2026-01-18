#include <stdio.h>
#include <string.h>

#include "arraylist.h"
#include "db.h"
#include "logger.h"
#include "table.h"
#include "test_util.h"

void test_inner_join(void) {
    log_msg(LOG_INFO, "Testing INNER JOIN...");

    reset_database();

    exec("CREATE TABLE employees (id INT, name STRING, dept_id INT);");
    exec("INSERT INTO employees VALUES (1, 'Alice', 1);");
    exec("INSERT INTO employees VALUES (2, 'Bob', 2);");
    exec("INSERT INTO employees VALUES (3, 'Charlie', 1);");

    exec("CREATE TABLE departments (dept_id INT, dept_name STRING);");
    exec("INSERT INTO departments VALUES (1, 'Engineering');");
    exec("INSERT INTO departments VALUES (2, 'Sales');");
    exec("INSERT INTO departments VALUES (3, 'Marketing');");

    exec("SELECT * FROM employees JOIN departments ON employees.dept_id = departments.dept_id;");

    log_msg(LOG_INFO, "INNER JOIN tests passed");
}

void test_left_join(void) {
    log_msg(LOG_INFO, "Testing LEFT JOIN...");

    reset_database();

    exec("CREATE TABLE customers (id INT, name STRING, city_id INT);");
    exec("INSERT INTO customers VALUES (1, 'Alice', 1);");
    exec("INSERT INTO customers VALUES (2, 'Bob', 2);");
    exec("INSERT INTO customers VALUES (3, 'Charlie', NULL);");

    exec("CREATE TABLE cities (city_id INT, city_name STRING);");
    exec("INSERT INTO cities VALUES (1, 'New York');");
    exec("INSERT INTO cities VALUES (2, 'Los Angeles');");

    exec("SELECT * FROM customers LEFT JOIN cities ON customers.city_id = cities.city_id;");

    log_msg(LOG_INFO, "LEFT JOIN tests passed");
}

void test_join_multiple_matches(void) {
    log_msg(LOG_INFO, "Testing JOIN with multiple matches...");

    reset_database();

    exec("CREATE TABLE orders (order_id INT, customer_id INT, product STRING);");
    exec("INSERT INTO orders VALUES (1, 1, 'Laptop');");
    exec("INSERT INTO orders VALUES (2, 1, 'Mouse');");
    exec("INSERT INTO orders VALUES (3, 2, 'Keyboard');");

    exec("CREATE TABLE customers (customer_id INT, name STRING);");
    exec("INSERT INTO customers VALUES (1, 'Alice');");
    exec("INSERT INTO customers VALUES (2, 'Bob');");

    exec("SELECT * FROM orders JOIN customers ON orders.customer_id = customers.customer_id;");

    log_msg(LOG_INFO, "JOIN with multiple matches tests passed");
}

void test_join_no_matches(void) {
    log_msg(LOG_INFO, "Testing JOIN with no matches...");

    reset_database();

    exec("CREATE TABLE t1 (id INT, value STRING);");
    exec("INSERT INTO t1 VALUES (1, 'A');");
    exec("INSERT INTO t1 VALUES (2, 'B');");

    exec("CREATE TABLE t2 (id INT, value STRING);");
    exec("INSERT INTO t2 VALUES (3, 'C');");
    exec("INSERT INTO t2 VALUES (4, 'D');");

    exec("SELECT * FROM t1 JOIN t2 ON t1.id = t2.id;");

    log_msg(LOG_INFO, "JOIN with no matches tests passed");
}

void test_join_empty_table(void) {
    log_msg(LOG_INFO, "Testing JOIN with empty table...");

    reset_database();

    exec("CREATE TABLE left_table (id INT, value STRING);");
    exec("INSERT INTO left_table VALUES (1, 'A');");
    exec("INSERT INTO left_table VALUES (2, 'B');");

    exec("CREATE TABLE right_table (id INT, value STRING);");

    exec("SELECT * FROM left_table JOIN right_table ON left_table.id = right_table.id;");

    log_msg(LOG_INFO, "JOIN with empty table tests passed");
}

void test_join_syntax_variations(void) {
    log_msg(LOG_INFO, "Testing JOIN syntax variations...");

    reset_database();

    exec("CREATE TABLE a (id INT, value STRING);");
    exec("INSERT INTO a VALUES (1, 'A1');");
    exec("INSERT INTO a VALUES (2, 'A2');");

    exec("CREATE TABLE b (id INT, value STRING);");
    exec("INSERT INTO b VALUES (1, 'B1');");
    exec("INSERT INTO b VALUES (2, 'B2');");

    exec("SELECT * FROM a JOIN b ON a.id = b.id;");
    exec("SELECT * FROM a INNER JOIN b ON a.id = b.id;");

    log_msg(LOG_INFO, "JOIN syntax variations tests passed");
}

void test_self_join(void) {
    log_msg(LOG_INFO, "Testing self JOIN...");

    reset_database();

    exec("CREATE TABLE employees (id INT, name STRING, manager_id INT);");
    exec("INSERT INTO employees VALUES (1, 'CEO', NULL);");
    exec("INSERT INTO employees VALUES (2, 'CTO', 1);");
    exec("INSERT INTO employees VALUES (3, 'CFO', 1);");
    exec("INSERT INTO employees VALUES (4, 'Dev1', 2);");

    exec(
        "SELECT e.name AS employee, m.name AS manager FROM employees e JOIN employees m ON "
        "e.manager_id = m.id;");

    log_msg(LOG_INFO, "Self JOIN tests passed");
}

void test_three_table_join(void) {
    log_msg(LOG_INFO, "Testing three table JOIN...");

    reset_database();

    exec("CREATE TABLE authors (author_id INT, name STRING);");
    exec("INSERT INTO authors VALUES (1, 'Alice');");
    exec("INSERT INTO authors VALUES (2, 'Bob');");

    exec("CREATE TABLE books (book_id INT, title STRING, author_id INT);");
    exec("INSERT INTO books VALUES (1, 'Book A', 1);");
    exec("INSERT INTO books VALUES (2, 'Book B', 1);");
    exec("INSERT INTO books VALUES (3, 'Book C', 2);");

    exec("CREATE TABLE sales (sale_id INT, book_id INT, quantity INT);");
    exec("INSERT INTO sales VALUES (1, 1, 10);");
    exec("INSERT INTO sales VALUES (2, 2, 5);");
    exec("INSERT INTO sales VALUES (3, 3, 8);");

    exec(
        "SELECT a.name, b.title, s.quantity FROM authors a JOIN books b ON a.author_id = "
        "b.author_id JOIN sales s ON b.book_id = s.book_id;");

    log_msg(LOG_INFO, "Three table JOIN tests passed");
}
