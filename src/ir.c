#include "db.h"
#include "logger.h"

IRNode* ast_to_ir(ASTNode* ast) {
    if (!ast) {
        log_msg(LOG_WARN, "ast_to_ir: called with NULL AST");
        return NULL;
    }

    log_msg(LOG_DEBUG, "ast_to_ir: Converting AST to IR");

    IRNode* ir = NULL;
    IRNode* tail = NULL;

    ASTNode* current = ast;
    while (current) {
        IRNode* new_ir = malloc(sizeof(IRNode));
        new_ir->next = NULL;

        switch (current->type) {
            case AST_CREATE_TABLE:
                new_ir->type = IR_CREATE_TABLE;
                new_ir->create_table.table_def = current->create_table.table_def;
                break;

            case AST_INSERT_ROW:
                new_ir->type = IR_INSERT_ROW;
                strcpy(new_ir->insert_row.table_name, current->insert.table_name);
                new_ir->insert_row.value_count = current->insert.value_count;
                for (int i = 0; i < current->insert.value_count; i++) {
                    new_ir->insert_row.values[i] = current->insert.values[i].value;
                }
                break;

            case AST_SELECT: {
                bool has_aggregates = false;
                for (int i = 0; i < current->select.expression_count; i++) {
                    if (current->select.expressions[i]->type == EXPR_AGGREGATE_FUNC) {
                        has_aggregates = true;
                        break;
                    }
                }

                if (has_aggregates) {
                    new_ir->type = IR_SCAN_TABLE;
                    strcpy(new_ir->scan_table.table_name, current->select.table_name);

                    IRNode* current_ir = new_ir;
                    if (current->select.where_clause) {
                        IRNode* filter_ir = malloc(sizeof(IRNode));
                        if (filter_ir) {
                            filter_ir->type = IR_FILTER;
                            strcpy(filter_ir->filter.table_name, current->select.table_name);
                            filter_ir->filter.filter_expr = current->select.where_clause;
                            filter_ir->next = NULL;
                            current_ir->next = filter_ir;
                            current_ir = filter_ir;
                        }
                    }

                    IRNode* aggregate_ir = malloc(sizeof(IRNode));
                    if (aggregate_ir) {
                        aggregate_ir->type = IR_AGGREGATE;
                        strcpy(aggregate_ir->aggregate.table_name, current->select.table_name);
                        if (current->select.expression_count > 0) {
                            Expr* agg_expr = current->select.expressions[0];
                            if (agg_expr->type == EXPR_AGGREGATE_FUNC) {
                                strcpy(aggregate_ir->aggregate.aggregate_func,
                                       agg_expr->aggregate.func_name);
                                aggregate_ir->aggregate.operand = agg_expr->aggregate.operand;
                                aggregate_ir->aggregate.distinct = agg_expr->aggregate.distinct;
                                aggregate_ir->aggregate.count_all = agg_expr->aggregate.count_all;
                            }
                        }
                        aggregate_ir->next = NULL;
                        current_ir->next = aggregate_ir;
                        current_ir = aggregate_ir;
                    }

                    IRNode* project_ir = malloc(sizeof(IRNode));
                    if (project_ir) {
                        project_ir->type = IR_PROJECT;
                        strcpy(project_ir->project.table_name, current->select.table_name);
                        project_ir->project.expression_count = current->select.expression_count;
                        for (int i = 0; i < current->select.expression_count; i++) {
                            project_ir->project.expressions[i] = current->select.expressions[i];
                        }
                        project_ir->next = NULL;
                        current_ir->next = project_ir;
                    }

                } else {
                    new_ir->type = IR_SCAN_TABLE;
                    strcpy(new_ir->scan_table.table_name, current->select.table_name);

                    IRNode* current_ir = new_ir;

                    if (current->select.where_clause) {
                        IRNode* filter_ir = malloc(sizeof(IRNode));
                        if (filter_ir) {
                            filter_ir->type = IR_FILTER;
                            strcpy(filter_ir->filter.table_name, current->select.table_name);
                            filter_ir->filter.filter_expr = current->select.where_clause;
                            filter_ir->next = NULL;
                            current_ir->next = filter_ir;
                            current_ir = filter_ir;
                        }
                    }

                    IRNode* project_ir = malloc(sizeof(IRNode));
                    if (project_ir) {
                        project_ir->type = IR_PROJECT;
                        strcpy(project_ir->project.table_name, current->select.table_name);
                        project_ir->project.expression_count = current->select.expression_count;
                        for (int i = 0; i < current->select.expression_count; i++) {
                            project_ir->project.expressions[i] = current->select.expressions[i];
                        }
                        project_ir->next = NULL;
                        current_ir->next = project_ir;
                    }
                }
            } break;

            case AST_DROP_TABLE:
                new_ir->type = IR_DROP_TABLE;
                strcpy(new_ir->drop_table.table_name, current->drop_table.table_name);
                break;

            case AST_UPDATE_ROW:
                new_ir->type = IR_UPDATE_ROW;
                strcpy(new_ir->update_row.table_name, current->update.table_name);
                new_ir->update_row.value_count = current->update.value_count;
                new_ir->update_row.where_clause = current->update.where_clause;
                for (int i = 0; i < current->update.value_count; i++) {
                    new_ir->update_row.values[i] = current->update.values[i];
                }
                break;

            case AST_DELETE_ROW:
                new_ir->type = IR_DELETE_ROW;
                strcpy(new_ir->delete_row.table_name, current->delete.table_name);
                new_ir->delete_row.where_clause = current->delete.where_clause;
                break;
        }

        if (!ir) {
            ir = new_ir;
            tail = new_ir;
        } else {
            tail->next = new_ir;
            tail = new_ir;
        }

        current = current->next;
    }

    log_msg(LOG_DEBUG, "ast_to_ir: AST to IR conversion completed");
    return ir;
}

void free_ir(IRNode* ir) {
    if (!ir) return;
    free_ir(ir->next);
    free(ir);
}
