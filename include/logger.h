#ifndef LOGGER_H
#define LOGGER_H

#include <stdio.h>

typedef enum {
  LOG_DEBUG = 0,
  LOG_INFO = 1,
  LOG_WARN = 2,
  LOG_ERROR = 3,
} LogLevel;

extern LogLevel g_log_level;

void set_log_level(LogLevel level);
LogLevel log_level_from_str(const char *level_str);
void log_msg(LogLevel level, const char *fmt, ...);
void show_prominent_error(const char *fmt, ...);
void suggest_similar(const char *input, const char *candidates[], int candidate_count,
                    char *output, int output_size);

#endif /* LOGGER_H */
