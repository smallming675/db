#include "test_util.h"
#include "logger.h"
#include "db.h"
#include "test_util.h"
#include <time.h>

void test_now_function(void) {
    log_msg(LOG_INFO, "Testing NOW function...");

    reset_database();

    exec("CREATE TABLE now_test (id INT, timestamp TIME);");
    exec("INSERT INTO now_test VALUES (1, NOW());");

    QueryResult *result = exec_query("SELECT id, timestamp FROM now_test;");
    assert_ptr_not_null(result, "NOW query should return result");

    assert_int_eq(1, alist_length(&result->rows), "Should return 1 row");

    Row *row = (Row *)alist_get(&result->rows, 0);
    Value *id_val = (Value *)alist_get(row, 0);
    Value *timestamp_val = (Value *)alist_get(row, 1);

    assert_int_eq(1, id_val->int_val, "ID should be 1");
    assert_true(timestamp_val->type == TYPE_TIME, "Timestamp should be TIME type");
    assert_true(timestamp_val->time_val > 0, "Timestamp should be positive");

    free_query_result(result);

    log_msg(LOG_INFO, "NOW function tests passed");
}

void test_dateadd_function(void) {
    log_msg(LOG_INFO, "Testing DATEADD function...");

    reset_database();

    exec("CREATE TABLE date_test (id INT, base_date DATE, added_date DATE);");
    exec("INSERT INTO date_test VALUES (1, 20230101, DATEADD(base_date, 1));");

    QueryResult *result = exec_query("SELECT id, base_date, added_date FROM date_test;");
    assert_ptr_not_null(result, "DATEADD query should return result");

    assert_int_eq(1, alist_length(&result->rows), "Should return 1 row");

    Row *row = (Row *)alist_get(&result->rows, 0);
    Value *base_val = (Value *)alist_get(row, 1);
    Value *added_val = (Value *)alist_get(row, 2);

    assert_int_eq(TYPE_DATE, base_val->type, "Base date should be DATE type");
    assert_int_eq(TYPE_DATE, added_val->type, "Added date should be DATE type");

    time_t t = added_val->date_val;
    struct tm *tm_info = localtime(&t);
    assert_int_eq(2023 - 1900, tm_info->tm_year, "Year should be 2023");
    assert_int_eq(1, tm_info->tm_mon, "Month should be 1");
    assert_int_eq(2, tm_info->tm_mday, "Day should be 2");

    free_query_result(result);

    log_msg(LOG_INFO, "DATEADD function tests passed");
}

void test_datediff_function(void) {
    log_msg(LOG_INFO, "Testing DATEDIFF function...");

    reset_database();

    exec("CREATE TABLE diff_test (id INT, start_date DATE, end_date DATE);");
    exec("INSERT INTO diff_test VALUES (1, 20230101, 20230131);");
    exec("INSERT INTO diff_test VALUES (2, 20230101, 20230201);");

    QueryResult *result = exec_query("SELECT id, DATEDIFF(end_date, start_date) FROM diff_test;");
    assert_ptr_not_null(result, "DATEDIFF query should return result");

    assert_int_eq(2, alist_length(&result->rows), "Should return 2 rows");

    Row *row1 = (Row *)alist_get(&result->rows, 0);
    Row *row2 = (Row *)alist_get(&result->rows, 1);
    Value *diff1 = (Value *)alist_get(row1, 1);
    Value *diff2 = (Value *)alist_get(row2, 1);

    assert_int_eq(30, diff1->int_val, "Days between Jan 1 and Jan 31 should be 30");
    assert_int_eq(31, diff2->int_val, "Days between Jan 1 and Feb 1 should be 31");

    free_query_result(result);

    log_msg(LOG_INFO, "DATEDIFF function tests passed");
}

void test_extract_function(void) {
    log_msg(LOG_INFO, "Testing EXTRACT function...");

    reset_database();

    exec("CREATE TABLE extract_test (id INT, test_date DATE);");
    exec("INSERT INTO extract_test VALUES (1, 20230615);");

    QueryResult *result = exec_query("SELECT id, EXTRACT(test_date, 'YEAR') as year, "
                                     "EXTRACT(test_date, 'MONTH') as month, "
                                     "EXTRACT(test_date, 'DAY') as day, "
                                     "EXTRACT(test_date, 'HOUR') as hour FROM extract_test;");
    assert_ptr_not_null(result, "EXTRACT query should return result");

    assert_int_eq(1, alist_length(&result->rows), "Should return 1 row");

    Row *row = (Row *)alist_get(&result->rows, 0);
    Value *year_val = (Value *)alist_get(row, 1);
    Value *month_val = (Value *)alist_get(row, 2);
    Value *day_val = (Value *)alist_get(row, 3);
    Value *hour_val = (Value *)alist_get(row, 4);

    assert_int_eq(2023, year_val->int_val, "Year should be 2023");
    assert_int_eq(6, month_val->int_val, "Month should be 6");
    assert_int_eq(15, day_val->int_val, "Day should be 15");
    assert_int_eq(0, hour_val->int_val, "Hour should be 0");

    free_query_result(result);

    log_msg(LOG_INFO, "EXTRACT function tests passed");
}

void test_datetime_edge_cases(void) {
    log_msg(LOG_INFO, "Testing date/time edge cases...");

    reset_database();

    exec("CREATE TABLE edge_test (id INT, test_date DATE, test_time TIME);");
    exec("INSERT INTO edge_test VALUES (1, NULL, NULL);");
    exec("INSERT INTO edge_test VALUES (2, 19991231, 235959);");

    QueryResult *result =
        exec_query("SELECT id, DATEDIFF(test_date, NULL), EXTRACT(test_time, 'MINUTE'), "
                   "DATEADD(test_date, 365) FROM edge_test;");
    assert_ptr_not_null(result, "Edge case query should return result");

    assert_int_eq(2, alist_length(&result->rows), "Should return 2 rows");

    Row *row1 = (Row *)alist_get(&result->rows, 0);
    Value *diff_val = (Value *)alist_get(row1, 1);
    Row *row2 = (Row *)alist_get(&result->rows, 1);
    Value *minute_val = (Value *)alist_get(row2, 1);

    assert_int_eq(TYPE_NULL, diff_val->type, "DATEDIFF with NULL should return NULL");
    assert_int_eq(59, minute_val->int_val, "EXTRACT of 23:59 should be 59");

    free_query_result(result);

    log_msg(LOG_INFO, "Date/time edge cases tests passed");
}
