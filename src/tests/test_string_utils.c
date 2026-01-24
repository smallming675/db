#include "utils.h"
#include "test_util.h"

void test_string_copy(void) {
    char buffer[8];
    size_t copied = string_copy(buffer, sizeof(buffer), "hello");
    assert_int_eq(5, (int)copied, "string_copy should report copied length");
    assert_str_eq("hello", buffer, "string_copy should copy full string");

    char small[4];
    copied = string_copy(small, sizeof(small), "abcdef");
    assert_int_eq(3, (int)copied, "string_copy should truncate to buffer size");
    assert_str_eq("abc", small, "string_copy should NUL-terminate when truncating");

    copied = string_copy(buffer, sizeof(buffer), NULL);
    assert_int_eq(0, (int)copied, "string_copy should report 0 for NULL source");
    assert_str_eq("", buffer, "string_copy should write empty string for NULL source");
}
