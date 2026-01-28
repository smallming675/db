#include "db.h"
#include "table.h"

#include <stdlib.h>
#include <string.h>

#include "logger.h"
#include "utils.h"

static TableStats g_stats[MAX_TABLES];
static int g_stats_count = 0;

static double estimate_seq_scan_cost(const char *table_name) {
    TableStats *stats = get_table_stats(table_name);
    if (!stats) {
        return 1000.0;
    }

    double base_cost = stats->total_rows * 1.0;
    double io_cost = stats->total_rows * 0.1;

    return base_cost + io_cost;
}

static double estimate_index_scan_cost(const char *table_name, Index *index, OperatorType op,
                                       const Value *value) {
    TableStats *stats = get_table_stats(table_name);
    if (!stats) {
        return 500.0;
    }

    double selectivity = 0.1;
    if (index->column_count == 1) {
        int col_idx = -1;
        Table *table = find_table(table_name);
        if (table) {
            for (int i = 0; i < alist_length(&table->schema.columns); i++) {
                ColumnDef *col = (ColumnDef *)alist_get(&table->schema.columns, i);
                if (col && strcmp(col->name, index->column_names[0]) == 0) {
                    col_idx = i;
                    break;
                }
            }
        }

        if (col_idx >= 0) {
            selectivity = estimate_selectivity(stats, col_idx, op, value);
        }
    }

    double result_rows = stats->total_rows * selectivity;
    double index_cost = result_rows * 0.5;
    double table_access_cost = result_rows * 1.0;

    return index_cost + table_access_cost;
}

PlanNode *create_seq_scan_plan(const char *table_name, Expr *where_clause) {
    PlanNode *plan = malloc(sizeof(PlanNode));
    if (!plan)
        return NULL;

    plan->type = PLAN_SEQ_SCAN;
    plan->left = NULL;
    plan->right = NULL;

    strcopy(plan->plan.seq_scan.table_name, sizeof(plan->plan.seq_scan.table_name), table_name);

    plan->plan.seq_scan.where_clause = where_clause;
    plan->plan.seq_scan.table_id = find_table_id_by_name(table_name);

    plan->cost = estimate_seq_scan_cost(table_name);

    TableStats *stats = get_table_stats(table_name);
    plan->estimated_rows = stats ? stats->total_rows : 1000;

    return plan;
}

static PlanNode *create_index_scan_plan(const char *table_name, Index *index, Expr *where_clause,
                                        OperatorType op, const Value *value) {
    PlanNode *plan = malloc(sizeof(PlanNode));
    if (!plan)
        return NULL;

    plan->type = PLAN_INDEX_SCAN;
    plan->left = NULL;
    plan->right = NULL;

    strcopy(plan->plan.index_scan.table_name, sizeof(plan->plan.index_scan.table_name), table_name);

    plan->plan.index_scan.index = index;
    plan->plan.index_scan.where_clause = where_clause;
    plan->plan.index_scan.table_id = find_table_id_by_name(table_name);
    plan->plan.index_scan.op = op;

    if (value) {
        plan->plan.index_scan.search_key = malloc(sizeof(Value));
        *plan->plan.index_scan.search_key = copy_value(value);
    } else {
        plan->plan.index_scan.search_key = NULL;
    }

    plan->cost = estimate_index_scan_cost(table_name, index, op, value);

    TableStats *stats = get_table_stats(table_name);
    double base_rows = stats ? stats->total_rows : 1000;
    double selectivity = 0.1;

    if (index->column_count == 1 && stats) {
        int col_idx = -1;
        Table *table = find_table(table_name);
        if (table) {
            for (int i = 0; i < alist_length(&table->schema.columns); i++) {
                ColumnDef *col = (ColumnDef *)alist_get(&table->schema.columns, i);
                if (col && strcmp(col->name, index->column_names[0]) == 0) {
                    col_idx = i;
                    break;
                }
            }
        }

        if (col_idx >= 0) {
            selectivity = estimate_selectivity(stats, col_idx, op, value);
        }
    }

    plan->estimated_rows = base_rows * selectivity;

    return plan;
}

static bool is_simple_indexable_predicate(Expr *expr, char *table_name,
                                          char col_name[MAX_COLUMN_NAME_LEN], OperatorType *op,
                                          Value **value) {
    if (!expr || !table_name)
        return false;

    if (expr->type == EXPR_BINARY_OP) {
        if (expr->binary.left->type == EXPR_COLUMN &&
            (expr->binary.right->type == EXPR_VALUE || expr->binary.right->type == EXPR_COLUMN)) {
            strcopy(col_name, MAX_COLUMN_NAME_LEN, expr->binary.left->column_name);
            *op = expr->binary.op;

            if (expr->binary.right->type == EXPR_VALUE) {
                *value = &expr->binary.right->value;
                return true;
            }
        }
    }

    return false;
}

PlanNode *optimize_select(const char *table_name, Expr *where_clause) {
    if (!table_name)
        return NULL;

    collect_table_stats(table_name);

    PlanNode *best_plan = NULL;
    double best_cost = 1e9;

    PlanNode *seq_plan = create_seq_scan_plan(table_name, where_clause);
    if (seq_plan && seq_plan->cost < best_cost) {
        best_cost = seq_plan->cost;
        best_plan = seq_plan;
    }

    if (where_clause) {
        char col_name[MAX_COLUMN_NAME_LEN];
        OperatorType op;
        Value *value;

        if (is_simple_indexable_predicate(where_clause, (char *)table_name, col_name, &op,
                                          &value)) {
            Index *index = find_index_by_table_column(table_name, col_name);
            if (index) {
                PlanNode *idx_plan =
                    create_index_scan_plan(table_name, index, where_clause, op, value);
                if (idx_plan && idx_plan->cost < best_cost) {
                    if (best_plan == seq_plan) {
                        free(seq_plan);
                        seq_plan = NULL;
                    } else if (best_plan) {
                        free(best_plan);
                    }
                    best_plan = idx_plan;
                } else if (idx_plan) {
                    free(idx_plan);
                }
            }
        }
    }

    if (best_plan) {
        log_msg(LOG_INFO, "optimize_select: Best plan for '%s' cost=%.2f rows=%d", table_name,
                best_plan->cost, best_plan->estimated_rows);
        if (seq_plan && seq_plan != best_plan) {
            free(seq_plan);
        }
    } else if (seq_plan) {
        free(seq_plan);
    }

    return best_plan;
}

double estimate_selectivity(const TableStats *stats, int col_idx, OperatorType op,
                            const Value *value __attribute__((unused))) {
    if (!stats || !stats->has_stats || col_idx < 0 || col_idx >= MAX_COLUMNS) {
        return 0.1;
    }

    double distinct = stats->distinct_values[col_idx];
    double total_rows = stats->total_rows;

    if (total_rows == 0)
        return 0.1;

    switch (op) {
    case OP_EQUALS:
        return distinct > 0 ? 1.0 / distinct : 0.1;
    case OP_NOT_EQUALS:
        return distinct > 1 ? (distinct - 1.0) / distinct : 0.1;
    case OP_LESS:
    case OP_LESS_EQUAL:
    case OP_GREATER:
    case OP_GREATER_EQUAL:
        return 0.3; // Range queries typically select ~30%
    case OP_LIKE:
        return 0.1; // LIKE is less selective
    default:
        return 0.1;
    }
}

void free_plan(PlanNode *plan) {
    if (!plan)
        return;

    if (plan->left)
        free_plan(plan->left);
    if (plan->right)
        free_plan(plan->right);

    if (plan->type == PLAN_INDEX_SCAN && plan->plan.index_scan.search_key) {
        free_value(plan->plan.index_scan.search_key);
        free(plan->plan.index_scan.search_key);
    }

    free(plan);
}

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
        strcopy(g_stats[g_stats_count].table_name, sizeof(g_stats[g_stats_count].table_name),
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
