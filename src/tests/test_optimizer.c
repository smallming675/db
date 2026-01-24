#include <string.h>
#include <assert.h>
#include "db.h"
#include "logger.h"
#include "utils.h"

void test_table_stats_functionality(void) {
    log_msg(LOG_INFO, "Testing TableStats functionality...");

    TableStats stats;
    string_copy(stats.table_name, sizeof(stats.table_name), "test_table");
    stats.total_rows = 1000;
    stats.has_stats = true;

    for (int i = 0; i < MAX_COLUMNS; i++) {
        stats.distinct_values[i] = 100;
    }

    assert(stats.total_rows == 1000);
    assert(stats.has_stats == true);
    assert(strcmp(stats.table_name, "test_table") == 0);
    assert(stats.distinct_values[0] == 100);

    log_msg(LOG_INFO, "TableStats functionality tests passed");
}

void test_optimizer_support_functions(void) {
    log_msg(LOG_INFO, "Testing optimizer support functions...");
    collect_table_stats("test_users");
    collect_table_stats("test_orders");

    TableStats *stats = get_table_stats("test_users");
    assert(stats != NULL);

    double selectivity = estimate_selectivity(stats, 0, OP_EQUALS, NULL);
    assert(selectivity > 0.0 && selectivity <= 1.0);

    Index *index = find_index_by_table_column("test_users", "id");
    assert(index == NULL);

    log_msg(LOG_INFO, "Optimizer support function tests passed");
}

void test_plan_node_structures(void) {
    log_msg(LOG_INFO, "Testing PlanNode structures...");

    SeqScanPlan seq_plan;
    string_copy(seq_plan.table_name, sizeof(seq_plan.table_name), "users");
    seq_plan.table_id = 1;
    seq_plan.where_clause = NULL;

    assert(strcmp(seq_plan.table_name, "users") == 0);
    assert(seq_plan.table_id == 1);
    assert(seq_plan.where_clause == NULL);

    IndexScanPlan idx_plan;
    string_copy(idx_plan.table_name, sizeof(idx_plan.table_name), "orders");
    idx_plan.table_id = 2;
    idx_plan.index = NULL;
    idx_plan.where_clause = NULL;
    idx_plan.op = OP_EQUALS;
    idx_plan.search_key = NULL;

    assert(strcmp(idx_plan.table_name, "orders") == 0);
    assert(idx_plan.table_id == 2);
    assert(idx_plan.op == OP_EQUALS);

    PlanNode plan;
    plan.type = PLAN_SEQ_SCAN;
    plan.left = NULL;
    plan.right = NULL;
    plan.cost = 100.0;
    plan.estimated_rows = 1000;
    plan.plan.seq_scan = seq_plan;

    assert(plan.type == PLAN_SEQ_SCAN);
    assert(plan.cost == 100.0);
    assert(plan.estimated_rows == 1000);
    assert(strcmp(plan.plan.seq_scan.table_name, "users") == 0);

    log_msg(LOG_INFO, "PlanNode structure tests passed");
}
