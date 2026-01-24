#ifndef UTILS_H
#define UTILS_H

#include <stdarg.h>
#include <stddef.h>

size_t string_copy(char *dest, size_t dest_size, const char *src);
size_t string_append(char *dest, size_t dest_size, const char *src);
int string_format(char *dest, size_t dest_size, const char *fmt, ...);
int string_format_v(char *dest, size_t dest_size, const char *fmt, va_list args);
void memory_clear(void *dest, size_t size);
void memory_copy(void *dest, const void *src, size_t size);

#endif
