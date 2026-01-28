#include "utils.h"
#include <stdarg.h>
#include <stdio.h>
#include <string.h>

static size_t strlength(const char *value, size_t max_len) {
    if (!value) {
        return 0;
    }
    size_t len = 0;
    while (len < max_len && value[len] != '\0') {
        len++;
    }
    return len;
}

size_t strcopy(char *dest, size_t dest_size, const char *src) {
    if (!dest || dest_size == 0) {
        return 0;
    }

    if (!src) {
        dest[0] = '\0';
        return 0;
    }

    size_t src_len = strlen(src);
    size_t copy_len = src_len;
    if (copy_len >= dest_size) {
        copy_len = dest_size - 1;
    }

    for (size_t i = 0; i < copy_len; i++) {
        dest[i] = src[i];
    }
    dest[copy_len] = '\0';
    return copy_len;
}

size_t str_append(char *dest, size_t dest_size, const char *src) {
    if (!dest || dest_size == 0) {
        return 0;
    }

    size_t dest_len = strlength(dest, dest_size);
    if (dest_len >= dest_size) {
        dest_len = dest_size - 1;
        dest[dest_len] = '\0';
    }

    if (!src) {
        return dest_len;
    }

    size_t i = 0;
    while (src[i] && dest_len + i + 1 < dest_size) {
        dest[dest_len + i] = src[i];
        i++;
    }
    dest[dest_len + i] = '\0';
    return dest_len + i;
}

int string_format_v(char *dest, size_t dest_size, const char *fmt, va_list args) {
    if (!dest || dest_size == 0) {
        return 0;
    }

    if (!fmt) {
        dest[0] = '\0';
        return 0;
    }

    // NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
    int written = vsnprintf(dest, dest_size, fmt, args);
    if (written < 0) {
        dest[0] = '\0';
        return 0;
    }
    dest[dest_size - 1] = '\0';
    return written;
}

int string_format(char *dest, size_t dest_size, const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    int written = string_format_v(dest, dest_size, fmt, args);
    va_end(args);
    return written;
}

void memclear(void *dest, size_t size) {
    if (!dest || size == 0) {
        return;
    }
    // NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
    memset(dest, 0, size);
}

void memcopy(void *dest, const void *src, size_t size) {
    if (!dest || !src || size == 0) {
        return;
    }
    // NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
    memcpy(dest, src, size);
}
