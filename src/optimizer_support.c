#include <string.h>

#include "db.h"
#include "logger.h"
#include "utils.h"

static TableStats g_stats[MAX_TABLES];
static int g_stats_count = 0;

TableStats *get_table_stats(const char *table_name) {
    if (!table_name)
        return NULL;

    for (int i = 0; i < g_stats_count; i++) {
        if (strcmp(g_stats[i].table_name, table_name) == 0) {
            return &g_stats[i];
        }
    }
    return NULL;
}

void collect_table_stats(const char *table_name) {
    if (!table_name)
        return;

    for (int i = 0; i < g_stats_count; i++) {
        if (strcmp(g_stats[i].table_name, table_name) == 0) {
            return;
        }
    }

    if (g_stats_count < MAX_TABLES) {
        string_copy(g_stats[g_stats_count].table_name, sizeof(g_stats[g_stats_count].table_name),
                    table_name);
        g_stats[g_stats_count].total_rows = 1000; // Default estimate
        g_stats[g_stats_count].has_stats = false;

        for (int i = 0; i < MAX_COLUMNS; i++) {
            g_stats[g_stats_count].distinct_values[i] = 100;
        }

        g_stats_count++;
        log_msg(LOG_INFO, "Collected stats for table '%s'", table_name);
    }
}
