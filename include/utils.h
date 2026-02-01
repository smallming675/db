#ifndef UTILS_H
#define UTILS_H

#include <stdarg.h>
#include <stddef.h>

// Safe version of strcpy that respects null termination and string lengths.
size_t strcopy(char *dest, size_t dest_size, const char *src);
size_t str_append(char *dest, size_t dest_size, const char *src);
int string_format(char *dest, size_t dest_size, const char *fmt, ...);
int string_format_v(char *dest, size_t dest_size, const char *fmt, va_list args);
void memclear(void *dest, size_t size);
void memcopy(void *dest, const void *src, size_t size);

#endif
