#include "executor.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "arraylist.h"
#include "db.h"
#include "executor_internal.h"
#include "logger.h"
#include "table.h"
#include "values.h"

QueryResult* g_last_result = NULL;
ArrayList g_aggregate_results;
bool g_in_aggregate_context = false;

QueryResult* get_last_query_result(void) { return g_last_result; }

void set_last_query_result(QueryResult* result) { g_last_result = result; }

static bool has_aggregate_expr(ASTNode* ast) {
    if (!ast || ast->type != AST_SELECT) return false;
    SelectNode* select = &ast->select;
    int count = alist_length(&select->expressions);
    for (int i = 0; i < count; i++) {
        Expr** expr_ptr = (Expr**)alist_get(&select->expressions, i);
        if (expr_ptr && *expr_ptr && (*expr_ptr)->type == EXPR_AGGREGATE_FUNC) {
            return true;
        }
    }
    return false;
}

void free_query_result(QueryResult* result) {
    if (!result) return;
    alist_destroy(&result->column_names);
    alist_destroy(&result->values);
    alist_destroy(&result->rows);
    free(result);
}

void exec_ast(ASTNode* ast) {
    if (!ast) {
        log_msg(LOG_WARN, "exec_ast: Called with NULL AST");
        return;
    }

    log_msg(LOG_DEBUG, "exec_ast: executing AST");

    g_in_aggregate_context = false;
    alist_init(&g_aggregate_results, sizeof(Value), NULL);

    ASTNode* current = ast;
    while (current) {
        switch (current->type) {
            case AST_CREATE_TABLE:
                exec_create_table_ast(current);
                break;
            case AST_INSERT_ROW:
                exec_insert_row_ast(current);
                break;
            case AST_SELECT: {
                SelectNode* select = &current->select;
                bool has_agg = has_aggregate_expr(current);
                bool has_join = (select->join_type != JOIN_NONE && select->join_table_id >= 0);
                uint8_t result_table_id = select->table_id;

                if (has_join) {
                    result_table_id = exec_join_ast(current);
                    if (result_table_id == 0) {
                        log_msg(LOG_ERROR, "exec_ast: JOIN failed");
                        return;
                    }
                }

                if (select->where_clause) {
                    exec_filter_ast(current, result_table_id);
                }

                if (has_agg) {
                    exec_aggregate_ast(current, result_table_id);
                }

                exec_project_ast(current, result_table_id);
                break;
            }
            case AST_DROP_TABLE:
                exec_drop_table_ast(current);
                break;
            case AST_UPDATE_ROW:
                exec_update_row_ast(current);
                break;
            case AST_DELETE_ROW:
                exec_delete_row_ast(current);
                break;
            case AST_CREATE_INDEX:
                exec_create_index_ast(current);
                break;
            case AST_DROP_INDEX:
                exec_drop_index_ast(current);
                break;
            default:
                log_msg(LOG_WARN, "exec_ast: Unknown AST node type: %d", current->type);
                break;
        }

        current = current->next;
    }

    log_msg(LOG_DEBUG, "exec_ast: AST execution completed");
}
