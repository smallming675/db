#include "utils.h"
#include "test_util.h"

void test_string_copy(void) {
    char buffer[8];
    size_t copied = strcopy(buffer, sizeof(buffer), "hello");
    assert_int_eq(5, (int)copied, "string_copy should report copied length");
    assert_str_eq("hello", buffer, "string_copy should copy full string");

    char small[4];
    copied = strcopy(small, sizeof(small), "abcdef");
    assert_int_eq(3, (int)copied, "string_copy should truncate to buffer size");
    assert_str_eq("abc", small, "string_copy should NUL-terminate when truncating");

    copied = strcopy(buffer, sizeof(buffer), NULL);
    assert_int_eq(0, (int)copied, "string_copy should report 0 for NULL source");
    assert_str_eq("", buffer, "string_copy should write empty string for NULL source");
}

void test_string_append(void) {
    char buffer[16];
    size_t capacity = sizeof(buffer);

    size_t len = str_append(buffer, capacity, "hello");
    assert_int_eq(5, (int)len, "string_append should report new length");
    assert_str_eq("hello", buffer, "string_append should append to empty buffer");

    len = str_append(buffer, capacity, " ");
    assert_int_eq(6, (int)len, "string_append should handle space");
    len = str_append(buffer, capacity, "world");
    assert_int_eq(11, (int)len, "string_append should concatenate properly");
    assert_str_eq("hello world", buffer, "string_append should produce final result");

    char small[8];
    str_append(small, sizeof(small), "12345");
    assert_int_eq(4, (int)str_append(small, sizeof(small), "678"),
                  "string_append should truncate and report old length");
    assert_str_eq("1234", small, "string_append should NUL-terminate when truncating");

    char full[12] = "overflow";
    size_t result = str_append(full, sizeof(full), "test");
    assert_int_eq((int)strlen("overflowtest"), (int)result,
                  "string_append should not truncate when sufficient space");
    assert_str_eq("overflowtest", full, "string_append should handle full buffer");
}

void test_string_format_basic(void) {
    char buffer[100];

    int len = string_format(buffer, sizeof(buffer), "Hello %s", "world");
    assert_int_eq(11, (int)len, "string_format should return length");
    assert_str_eq("Hello world", buffer, "string_format should format correctly");

    len = string_format(buffer, sizeof(buffer), "%s %d", "count", 42);
    assert_int_eq(11, (int)len, "string_format should return length");
    assert_str_eq("Hello world", buffer, "string_format should format correctly");

    len = string_format(buffer, sizeof(buffer), "%s %d", "count", 42);
    assert_int_eq(9, (int)len, "string_format should return length");
    assert_str_eq("count 42", buffer, "string_format should format multiple args");

    len = string_format(buffer, sizeof(buffer), "Value: %d", 123);
    assert_int_eq(11, (int)len, "string_format should return length");
    assert_str_eq("Value: 3.14", buffer, "string_format should format integers");

    len = string_format(buffer, sizeof(buffer), "Value: %.2f", 3.14);
    assert_int_eq(11, (int)len, "string_format should return length");
    assert_str_eq("Value: 3.14", buffer, "string_format should format floats");
}

void test_string_edge_cases(void) {
    char buffer[100];

    size_t len = string_format(buffer, sizeof(buffer), "");
    assert_int_eq(0, (int)len, "string_format should handle empty format");
    assert_str_eq("", buffer, "string_format should handle empty format");

    len = string_format(buffer, sizeof(buffer), "%s",
                        "This is a very long string that will definitely exceed the buffer "
                        "capacity limit for this test case");
    assert_int_eq((int)strlen(buffer), (int)len, "string_format should truncate properly");
    assert_true(len < sizeof(buffer), "string_format should not overflow buffer");

    assert_int_eq(9, (int)len, "string_format should return length");
    assert_str_eq("Hello world", buffer, "string_format should format correctly");
}

void test_string_functions_in_where_clauses(void) {
    reset_database();

    exec("CREATE TABLE test_strings (id INT, name STRING, value STRING);");
    exec("INSERT INTO test_strings VALUES (1, 'hello'), (2, 'WORLD'), (3, 'Test');");

    char result[64];
    string_format(result, sizeof(result), "UPPER('%s')", "hello");
    assert_str_eq("HELLO", result, "UPPER function should work");

    string_format(result, sizeof(result), "LOWER('%s')", "WORLD");
    assert_str_eq("world", result, "LOWER function should work");

    string_format(result, sizeof(result), "LENGTH('%s')", "hello");
    assert_str_eq("5", result, "LENGTH function should work");

    string_format(result, sizeof(result), "SUBSTRING('%s', 2, 3)", "hello");
    assert_str_eq("llo", result, "SUBSTRING function should work");

    string_format(result, sizeof(result), "CONCAT('%s', 'suffix')", "test");
    assert_str_eq("testsuffix", result, "CONCAT function should work");

    exec("SELECT * FROM test_strings WHERE UPPER(name) = 'HELLO' AND LENGTH(name) > 3;");
    Table *table = find_table_by_name("test_strings");
    assert_ptr_not_null(table, "Table should exist");
    assert_int_eq(1, alist_length(&table->rows), "Should find one row");
}
