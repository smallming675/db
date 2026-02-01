#include "db.h"
#include "table.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "logger.h"
#include "utils.h"

static TableStats g_stats[MAX_TABLES];
static int g_stats_count = 0;

static double estimate_seq_scan_cost(uint8_t table_id) {
    TableStats *stats = get_table_stats(table_id);
    if (!stats) {
        return 1000.0;
    }

    double base_cost = stats->total_rows * 1.0;
    double io_cost = stats->total_rows * 0.1;

    return base_cost + io_cost;
}

static double estimate_index_scan_cost(uint8_t table_id, Index *index, OperatorType op,
                                       const Value *value) {
    TableStats *stats = get_table_stats(table_id);
    if (!stats) {
        return 500.0;
    }

    double selectivity = 0.1;
    if (alist_length(&index->columns) > 0) {
        uint16_t *col_id_ptr = (uint16_t *)alist_get(&index->columns, 0);
        if (col_id_ptr) {
            selectivity = estimate_selectivity(stats, *col_id_ptr, op, value);
        }
    }

    double result_rows = stats->total_rows * selectivity;
    double index_cost = result_rows * 0.5;
    double table_access_cost = result_rows * 1.0;

    return index_cost + table_access_cost;
}

PlanNode *create_seq_scan_plan(uint8_t table_id, Expr *where_clause) {
    PlanNode *plan = malloc(sizeof(PlanNode));
    if (!plan)
        return NULL;

    plan->type = PLAN_SEQ_SCAN;
    plan->left = NULL;
    plan->right = NULL;

    plan->plan.seq_scan.where_clause = where_clause;
    plan->plan.seq_scan.table_id = table_id;

    plan->cost = estimate_seq_scan_cost(table_id);

    TableStats *stats = get_table_stats(table_id);
    plan->estimated_rows = stats ? stats->total_rows : 1000;

    return plan;
}

static PlanNode *create_index_scan_plan(uint8_t table_id, Index *index, Expr *where_clause,
                                        OperatorType op, const Value *value) {
    PlanNode *plan = malloc(sizeof(PlanNode));
    if (!plan)
        return NULL;

    plan->type = PLAN_INDEX_SCAN;
    plan->left = NULL;
    plan->right = NULL;

    plan->plan.index_scan.index = index;
    plan->plan.index_scan.where_clause = where_clause;
    plan->plan.index_scan.table_id = table_id;
    plan->plan.index_scan.op = op;

    if (value) {
        plan->plan.index_scan.search_key = malloc(sizeof(Value));
        *plan->plan.index_scan.search_key = copy_value(value);
    } else {
        plan->plan.index_scan.search_key = NULL;
    }

    plan->cost = estimate_index_scan_cost(table_id, index, op, value);

    TableStats *stats = get_table_stats(table_id);
    double base_rows = stats ? stats->total_rows : 1000;
    double selectivity = 0.1;

    if (alist_length(&index->columns) > 0 && stats) {
        uint16_t *col_id_ptr = (uint16_t *)alist_get(&index->columns, 0);
        if (col_id_ptr) {
            selectivity = estimate_selectivity(stats, *col_id_ptr, op, value);
        }
    }

    plan->estimated_rows = base_rows * selectivity;

    return plan;
}

static bool is_simple_indexable_predicate(Expr *expr, uint8_t table_id, uint16_t *column_id,
                                          OperatorType *op, Value **value) {
    if (!expr)
        return false;

    if (expr->type == EXPR_BINARY_OP) {
        if (expr->binary.left->type == EXPR_COLUMN &&
            (expr->binary.right->type == EXPR_VALUE || expr->binary.right->type == EXPR_COLUMN)) {
            if (expr->binary.left->column.table_id == table_id) {
                *column_id = expr->binary.left->column.column_id;
                *op = expr->binary.op;

                if (expr->binary.right->type == EXPR_VALUE) {
                    *value = &expr->binary.right->value;
                    return true;
                }
            }
        }
    }

    return false;
}

PlanNode *optimize_select(uint8_t table_id, Expr *where_clause) {
    collect_table_stats(table_id);

    PlanNode *best_plan = NULL;
    double best_cost = 1e9;

    PlanNode *seq_plan = create_seq_scan_plan(table_id, where_clause);
    if (seq_plan && seq_plan->cost < best_cost) {
        best_cost = seq_plan->cost;
        best_plan = seq_plan;
    }

    if (where_clause) {
        uint16_t col_id;
        OperatorType op;
        Value *value;

        if (is_simple_indexable_predicate(where_clause, table_id, &col_id, &op,
                                          &value)) {
            Index *index = find_index_by_table_column(table_id, col_id);
            if (index) {
                PlanNode *idx_plan =
                    create_index_scan_plan(table_id, index, where_clause, op, value);
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
        Table *table = get_table_by_id(table_id);
        log_msg(LOG_INFO, "optimize_select: Best plan for '%s' cost=%.2f rows=%d",
                table ? table->name : "unknown",
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

TableStats *get_table_stats(uint8_t table_id) {
    for (int i = 0; i < g_stats_count; i++) {
        if (g_stats[i].table_id == table_id) {
            return &g_stats[i];
        }
    }
    return NULL;
}

void collect_table_stats(uint8_t table_id) {
    for (int i = 0; i < g_stats_count; i++) {
        if (g_stats[i].table_id == table_id) {
            return;
        }
    }

    if (g_stats_count < MAX_TABLES) {
        g_stats[g_stats_count].table_id = table_id;
        g_stats[g_stats_count].total_rows = 1000; // Default estimate
        g_stats[g_stats_count].has_stats = false;

        for (int i = 0; i < MAX_COLUMNS; i++) {
            g_stats[g_stats_count].distinct_values[i] = 100;
        }

        g_stats_count++;
        log_msg(LOG_INFO, "Collected stats for table_id=%d", table_id);
    }
}
