#include "logger.h"

#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include <ctype.h>

LogLevel g_log_level = LOG_INFO;

#define COLOR_RESET "\x1b[0m"
#define COLOR_RED "\x1b[31m"
#define COLOR_GREEN "\x1b[32m"
#define COLOR_YELLOW "\x1b[33m"
#define COLOR_BLUE "\x1b[34m"
#define COLOR_BOLD "\x1b[1m"

static const char* lvl_str(LogLevel lvl) {
    switch (lvl) {
        case LOG_INFO:
            return COLOR_GREEN "INFO" COLOR_RESET;
        case LOG_WARN:
            return COLOR_YELLOW "WARN" COLOR_RESET;
        case LOG_ERROR:
            return COLOR_RED "ERROR" COLOR_RESET;
        case LOG_DEBUG:
            return COLOR_BLUE "DEBUG" COLOR_RESET;
    }
    return "";
}

static int levenshtein_distance(const char *s1, const char *s2) {
    int len1 = strlen(s1);
    int len2 = strlen(s2);
    int matrix[len1 + 1][len2 + 1];

    for (int i = 0; i <= len1; i++) {
        matrix[i][0] = i;
    }
    for (int j = 0; j <= len2; j++) {
        matrix[0][j] = j;
    }

    for (int i = 1; i <= len1; i++) {
        for (int j = 1; j <= len2; j++) {
            int cost = (tolower(s1[i - 1]) == tolower(s2[j - 1])) ? 0 : 1;
            matrix[i][j] = (matrix[i - 1][j] + 1 < matrix[i][j - 1] + 1)
                               ? matrix[i - 1][j] + 1
                               : matrix[i][j - 1] + 1;
            matrix[i][j] = (matrix[i][j] < matrix[i - 1][j - 1] + cost)
                               ? matrix[i][j]
                               : matrix[i - 1][j - 1] + cost;
        }
    }
    return matrix[len1][len2];
}

static int get_similarity_score(const char *input, const char *candidate) {
    int input_len = strlen(input);
    int candidate_len = strlen(candidate);

    if (input_len == 0 || candidate_len == 0) {
        return 0;
    }

    if (strcasecmp(input, candidate) == 0) {
        return 100;
    }

    if (strncasecmp(input, candidate, 1) == 0) {
        if (strstr(candidate, input) != NULL || strstr(input, candidate) != NULL) {
            return 80;
        }
    }

    int distance = levenshtein_distance(input, candidate);
    int max_len = (input_len > candidate_len) ? input_len : candidate_len;
    int similarity = ((max_len - distance) * 100) / max_len;

    return similarity;
}

void suggest_similar(const char *input, const char *candidates[], int candidate_count,
                    char *output, int output_size) {
    int best_score = 0;
    int best_idx = -1;
    int threshold = 40;

    for (int i = 0; i < candidate_count; i++) {
        int score = get_similarity_score(input, candidates[i]);
        if (score > best_score) {
            best_score = score;
            best_idx = i;
        }
    }

    if (best_idx >= 0 && best_score >= threshold) {
        snprintf(output, output_size, "Did you mean '%s'?", candidates[best_idx]);
    } else {
        output[0] = '\0';
    }
}

void set_log_level(LogLevel level) { g_log_level = level; }

LogLevel log_level_from_str(const char* level_str) {
    if (strcmp(level_str, "DEBUG") == 0) {
        return LOG_DEBUG;
    } else if (strcmp(level_str, "INFO") == 0) {
        return LOG_INFO;
    } else if (strcmp(level_str, "WARN") == 0) {
        return LOG_WARN;
    } else if (strcmp(level_str, "ERROR") == 0) {
        return LOG_ERROR;
    } else
        return LOG_INFO;
}

void log_msg(LogLevel level, const char* fmt, ...) {
    if (level < g_log_level) return;
    time_t t = time(NULL);
    struct tm tm = *localtime(&t);
    char ts[64];
    strftime(ts, sizeof(ts), "%F %T", &tm);

    fprintf(stderr, "[%s] %s: ", ts, lvl_str(level));
    va_list ap;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    fputs(COLOR_RESET "\n", stderr);
}

void show_prominent_error(const char* fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    printf(COLOR_RESET "\n");
}
