#ifndef LOGGER_H
#define LOGGER_H

#define COLOR_RESET "\x1b[0m"
#define COLOR_RED "\x1b[31m"
#define COLOR_GREEN "\x1b[32m"
#define COLOR_YELLOW "\x1b[33m"
#define COLOR_BLUE "\x1b[34m"
#define COLOR_MAGENTA "\x1b[35m"
#define COLOR_CYAN "\x1b[36m"
#define COLOR_BOLD "\x1b[1m"
#define COLOR_DIM "\x1b[2m"

typedef enum {
    LOG_DEBUG = 0,
    LOG_INFO = 1,
    LOG_WARN = 2,
    LOG_ERROR = 3,
    LOG_NONE = 4,
} LogLevel;

extern LogLevel g_log_level;

void set_log_level(LogLevel level);
LogLevel log_level_from_str(const char *level_str);
void log_msg(LogLevel level, const char *fmt, ...);
void show_prominent_error(const char *fmt, ...);
void suggest_similar(const char *input, const char *candidates[], int candidate_count, char *output,
                     int output_size);

#endif /* LOGGER_H */
