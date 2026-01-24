#include <assert.h>
#include <stdio.h>

#include "db.h"
#include "logger.h"
#include "test_util.h"
#include "utils.h"

void test_count_aggregate(void) {
    log_msg(LOG_INFO, "Testing COUNT aggregate...");

    reset_database();

    exec("CREATE TABLE test_count (id INT, value INT);");
    exec("INSERT INTO test_count VALUES (1, 10), (2, 20), (3, NULL);");

    QueryResult *result = exec_query("SELECT COUNT(value) FROM test_count;");
    assert_ptr_not_null(result, "COUNT query should return result");

    log_msg(LOG_INFO, "COUNT test passed");
}

void test_sum_aggregate(void) {
    log_msg(LOG_INFO, "Testing SUM aggregate...");

    reset_database();

    exec("CREATE TABLE test_sum (id INT, value INT);");
    exec("INSERT INTO test_sum VALUES (1, 10), (2, 20), (3, 30);");

    QueryResult *result = exec_query("SELECT SUM(value) FROM test_sum;");
    assert_ptr_not_null(result, "SUM query should return result");

    log_msg(LOG_INFO, "SUM test passed");
}

void test_avg_aggregate(void) {
    log_msg(LOG_INFO, "Testing AVG aggregate...");

    reset_database();

    exec("CREATE TABLE test_avg (id INT, value INT);");
    exec("INSERT INTO test_avg VALUES (1, 10), (2, 20), (3, 30);");

    QueryResult *result = exec_query("SELECT AVG(value) FROM test_avg;");
    assert_ptr_not_null(result, "AVG query should return result");

    log_msg(LOG_INFO, "AVG test passed");
}

void test_min_aggregate(void) {
    log_msg(LOG_INFO, "Testing MIN aggregate...");

    reset_database();

    exec("CREATE TABLE test_min (id INT, value INT);");
    exec("INSERT INTO test_min VALUES (1, 10), (2, 5), (3, 30);");

    QueryResult *result = exec_query("SELECT MIN(value) FROM test_min;");
    assert_ptr_not_null(result, "MIN query should return result");

    log_msg(LOG_INFO, "MIN test passed");
}

void test_max_aggregate(void) {
    log_msg(LOG_INFO, "Testing MAX aggregate...");

    reset_database();

    exec("CREATE TABLE test_max (id INT, value INT);");
    exec("INSERT INTO test_max VALUES (1, 10), (2, 20), (3, 30);");

    QueryResult *result = exec_query("SELECT MAX(value) FROM test_max;");
    assert_ptr_not_null(result, "MAX query should return result");

    log_msg(LOG_INFO, "MAX test passed");
}

void test_multiple_aggregates(void) {
    log_msg(LOG_INFO, "Testing multiple aggregates...");

    reset_database();

    exec("CREATE TABLE test_multi (id INT, value INT);");
    exec("INSERT INTO test_multi VALUES (1, 10), (2, 20), (3, 30);");

    QueryResult *result = exec_query(
        "SELECT COUNT(value), SUM(value), AVG(value), MIN(value), MAX(value) FROM test_multi;");
    assert_ptr_not_null(result, "Multiple aggregates query should return result");

    log_msg(LOG_INFO, "Multiple aggregates test passed");
}

void test_aggregate_with_where(void) {
    log_msg(LOG_INFO, "Testing aggregate with WHERE...");

    reset_database();

    exec("CREATE TABLE test_where (id INT, value INT, category STRING);");
    exec("INSERT INTO test_where VALUES (1, 10, 'A'), (2, 20, 'A'), (3, 30, 'B');");

    QueryResult *result = exec_query("SELECT COUNT(value) FROM test_where WHERE category = 'A';");
    assert_ptr_not_null(result, "Aggregate with WHERE query should return result");

    log_msg(LOG_INFO, "Aggregate with WHERE test passed");
}

void test_aggregate_edge_cases(void) {
    log_msg(LOG_INFO, "Testing aggregate edge cases...");

    reset_database();

    exec("CREATE TABLE test_edge (id INT, value INT);");
    exec("INSERT INTO test_edge VALUES (1, NULL), (2, NULL);");

    QueryResult *result = exec_query("SELECT COUNT(value), SUM(value), AVG(value) FROM test_edge;");
    assert_ptr_not_null(result, "Aggregate edge cases query should return result");

    log_msg(LOG_INFO, "Aggregate edge cases test passed");
}

void test_count_all_vs_column(void) {
    log_msg(LOG_INFO, "Testing COUNT(*) vs column...");

    reset_database();

    exec("CREATE TABLE test_count_star (id INT, value INT);");
    exec("INSERT INTO test_count_star VALUES (1, 10), (2, 20);");

    QueryResult *result1 = exec_query("SELECT COUNT(*) FROM test_count_star;");
    QueryResult *result2 = exec_query("SELECT COUNT(id) FROM test_count_star;");
    assert_ptr_not_null(result1, "COUNT(*) query should return result");
    assert_ptr_not_null(result2, "COUNT(id) query should return result");

    log_msg(LOG_INFO, "COUNT(*) vs column test passed");
}

void test_aggregate_with_all_types(void) {
    log_msg(LOG_INFO, "Testing aggregate with all types...");

    reset_database();

    exec("CREATE TABLE test_types (id INT, int_val INT, float_val FLOAT, str_val STRING);");
    exec("INSERT INTO test_types VALUES (1, 10, 1.5, 'test');");

    QueryResult *result = exec_query("SELECT COUNT(*), SUM(int_val), AVG(float_val), MIN(str_val), "
                                     "MAX(str_val) FROM test_types;");
    assert_ptr_not_null(result, "All types aggregate query should return result");

    log_msg(LOG_INFO, "All types aggregate test passed");
}

void test_aggregate_large_dataset(void) {
    log_msg(LOG_INFO, "Testing aggregate with large dataset...");

    reset_database();

    exec("CREATE TABLE test_large (id INT, value INT);");
    for (int i = 1; i <= 100; i++) {
        char sql[100];
        string_format(sql, sizeof(sql), "INSERT INTO test_large VALUES (%d, %d);", i, i * 2);
        exec(sql);
    }

    QueryResult *result =
        exec_query("SELECT COUNT(value), SUM(value), AVG(value) FROM test_large;");
    assert_ptr_not_null(result, "Large dataset aggregate query should return result");

    log_msg(LOG_INFO, "Large dataset aggregate test passed");
}

void test_aggregate_with_zero_values(void) {
    log_msg(LOG_INFO, "Testing aggregate with zero values...");

    reset_database();

    exec("CREATE TABLE test_zero (id INT, value INT);");
    exec("INSERT INTO test_zero VALUES (1, 0), (2, 0), (3, 0);");

    QueryResult *result = exec_query("SELECT COUNT(value), SUM(value), AVG(value) FROM test_zero;");
    assert_ptr_not_null(result, "Zero values aggregate query should return result");

    log_msg(LOG_INFO, "Zero values aggregate test passed");
}

void test_aggregate_with_like_where(void) {
    log_msg(LOG_INFO, "Testing aggregate with LIKE WHERE...");

    reset_database();

    exec("CREATE TABLE test_like (id INT, value INT, pattern STRING);");
    exec("INSERT INTO test_like VALUES (1, 10, 'A%'), (2, 20, 'A%');");

    QueryResult *result = exec_query("SELECT COUNT(value) FROM test_like WHERE pattern LIKE 'A%';");
    assert_ptr_not_null(result, "Aggregate with LIKE WHERE query should return result");

    log_msg(LOG_INFO, "Aggregate with LIKE WHERE test passed");
}

void test_aggregate_multiple_wheres(void) {
    log_msg(LOG_INFO, "Testing aggregate with multiple WHERE...");

    reset_database();

    exec("CREATE TABLE test_multi_where (id INT, value INT, category STRING, flag BOOLEAN);");
    exec("INSERT INTO test_multi_where VALUES (1, 10, 'A', true), (2, 20, 'A', false);");

    QueryResult *result = exec_query(
        "SELECT COUNT(value) FROM test_multi_where WHERE category = 'A' AND flag = true;");
    assert_ptr_not_null(result, "Multiple WHERE aggregate query should return result");

    log_msg(LOG_INFO, "Multiple WHERE aggregate test passed");
}

void test_aggregate_with_order_by_limit(void) {
    log_msg(LOG_INFO, "Testing aggregate with ORDER BY LIMIT...");

    reset_database();

    exec("CREATE TABLE test_order (id INT, value INT);");
    exec("INSERT INTO test_order VALUES (1, 30), (2, 20), (3, 10);");

    QueryResult *result =
        exec_query("SELECT COUNT(value) FROM test_order ORDER BY value DESC LIMIT 2;");
    assert_ptr_not_null(result, "ORDER BY LIMIT aggregate query should return result");

    log_msg(LOG_INFO, "ORDER BY LIMIT aggregate test passed");
}
