#include <math.h>
#include <stdio.h>
#include <string.h>

#include "arraylist.h"
#include "db.h"
#include "logger.h"
#include "table.h"
#include "test_util.h"

void test_string_functions(void) {
    log_msg(LOG_INFO, "Testing string scalar functions...");

    reset_database();

    exec("CREATE TABLE strings (id INT, text STRING);");
    exec("INSERT INTO strings VALUES (1, 'hello');");
    exec("INSERT INTO strings VALUES (2, 'WORLD');");
    exec("INSERT INTO strings VALUES (3, 'OpenCode');");

    exec("SELECT UPPER(text) FROM strings;");
    exec("SELECT LOWER(text) FROM strings;");
    exec("SELECT LENGTH(text) FROM strings;");

    log_msg(LOG_INFO, "String scalar functions tests passed");
}

void test_numeric_functions(void) {
    log_msg(LOG_INFO, "Testing numeric scalar functions...");

    reset_database();

    exec("CREATE TABLE numbers (id INT, value FLOAT);");
    exec("INSERT INTO numbers VALUES (1, 4.7);");
    exec("INSERT INTO numbers VALUES (2, -3.2);");
    exec("INSERT INTO numbers VALUES (3, 9.0);");

    exec("SELECT ROUND(value) FROM numbers;");
    exec("SELECT ABS(value) FROM numbers;");
    exec("SELECT FLOOR(value) FROM numbers;");
    exec("SELECT CEIL(value) FROM numbers;");
    exec("SELECT SQRT(value) FROM numbers;");

    log_msg(LOG_INFO, "Numeric scalar functions tests passed");
}

void test_concat_function(void) {
    log_msg(LOG_INFO, "Testing CONCAT function...");

    reset_database();

    exec("CREATE TABLE names (id INT, first_name STRING, last_name STRING);");
    exec("INSERT INTO names VALUES (1, 'John', 'Doe');");
    exec("INSERT INTO names VALUES (2, 'Jane', 'Smith');");

    exec("SELECT CONCAT(first_name, ' ', last_name) FROM names;");

    log_msg(LOG_INFO, "CONCAT function tests passed");
}

void test_substring_function(void) {
    log_msg(LOG_INFO, "Testing SUBSTRING function...");

    reset_database();

    exec("CREATE TABLE text_data (id INT, text STRING);");
    exec("INSERT INTO text_data VALUES (1, 'Hello World');");
    exec("INSERT INTO text_data VALUES (2, 'OpenCode Database');");

    exec("SELECT SUBSTRING(text, 1, 5) FROM text_data;");
    exec("SELECT SUBSTRING(text, 7) FROM text_data;");

    log_msg(LOG_INFO, "SUBSTRING function tests passed");
}

void test_length_function(void) {
    log_msg(LOG_INFO, "Testing LENGTH function...");

    reset_database();

    exec("CREATE TABLE lengths (id INT, text STRING);");
    exec("INSERT INTO lengths VALUES (1, 'abc');");
    exec("INSERT INTO lengths VALUES (2, 'hello');");
    exec("INSERT INTO lengths VALUES (3, '');");

    exec("SELECT LENGTH(text) FROM lengths;");

    log_msg(LOG_INFO, "LENGTH function tests passed");
}

void test_mod_function(void) {
    log_msg(LOG_INFO, "Testing MOD function...");

    reset_database();

    exec("CREATE TABLE modulo (id INT, value INT, divisor INT);");
    exec("INSERT INTO modulo VALUES (1, 10, 3);");
    exec("INSERT INTO modulo VALUES (2, 17, 5);");
    exec("INSERT INTO modulo VALUES (3, 20, 4);");

    exec("SELECT MOD(value, divisor) FROM modulo;");

    log_msg(LOG_INFO, "MOD function tests passed");
}

void test_power_function(void) {
    log_msg(LOG_INFO, "Testing POWER function...");

    reset_database();

    exec("CREATE TABLE powers (id INT, base FLOAT, exponent FLOAT);");
    exec("INSERT INTO powers VALUES (1, 2, 3);");
    exec("INSERT INTO powers VALUES (2, 5, 2);");
    exec("INSERT INTO powers VALUES (3, 10, 0);");

    exec("SELECT POWER(base, exponent) FROM powers;");

    log_msg(LOG_INFO, "POWER function tests passed");
}

void test_mid_function(void) {
    log_msg(LOG_INFO, "Testing MID function...");

    reset_database();

    exec("CREATE TABLE mid_test (id INT, text STRING);");
    exec("INSERT INTO mid_test VALUES (1, 'OpenCode');");
    exec("INSERT INTO mid_test VALUES (2, 'Database');");

    exec("SELECT MID(text, 1, 4) FROM mid_test;");

    log_msg(LOG_INFO, "MID function tests passed");
}

void test_scalar_in_expressions(void) {
    log_msg(LOG_INFO, "Testing scalar functions in expressions...");

    reset_database();

    exec("CREATE TABLE calc (id INT, a FLOAT, b FLOAT);");
    exec("INSERT INTO calc VALUES (1, 10, 3);");
    exec("INSERT INTO calc VALUES (2, 5, 2);");

    exec("SELECT ROUND(a / b) FROM calc;");
    exec("SELECT ABS(a - b) FROM calc;");
    exec("SELECT POWER(a, b) FROM calc;");

    log_msg(LOG_INFO, "Scalar functions in expressions tests passed");
}
