#ifndef TEST_UTIL_H
#define TEST_UTIL_H

#include <stdbool.h>
#include <stdarg.h>
#include "db.h"

void reset_database(void);
bool exec(const char *sql);
Table *find_table_by_name(const char *name);
QueryResult *exec_query(const char *sql);
void assert_true(bool condition, const char *format, ...);
void assert_false(bool condition, const char *format, ...);
void assert_int_eq(int expected, int actual, const char *format, ...);
void assert_str_eq(const char *expected, const char *actual, const char *format, ...);
void assert_ptr_not_null(void *ptr, const char *format, ...);
void assert_ptr_null(void *ptr, const char *format, ...);
void assert_float_eq(double expected, double actual, double epsilon, const char *format, ...);

#endif
