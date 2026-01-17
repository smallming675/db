#include "db.h"
#include "logger.h"
#include "table.h"

extern Value copy_value(const Value* src);

static void free_expr_contents(Expr* expr) {
    if (!expr) return;
    switch (expr->type) {
        case EXPR_BINARY_OP:
            if (expr->binary.left) free_expr_contents(expr->binary.left);
            if (expr->binary.right) free_expr_contents(expr->binary.right);
            break;
        case EXPR_UNARY_OP:
            if (expr->unary.operand) free_expr_contents(expr->unary.operand);
            break;
        case EXPR_AGGREGATE_FUNC:
            if (expr->aggregate.operand) free_expr_contents(expr->aggregate.operand);
            break;
        case EXPR_SCALAR_FUNC:
            for (int i = 0; i < expr->scalar.arg_count; i++) {
                if (expr->scalar.args[i]) free_expr_contents(expr->scalar.args[i]);
            }
            break;
        default:
            break;
    }
}

static Expr* copy_expr(const Expr* src) {
    if (!src) return NULL;
    Expr* dst = malloc(sizeof(Expr));
    if (!dst) return NULL;
    *dst = *src;
    dst->alias[0] = '\0';
    switch (src->type) {
        case EXPR_BINARY_OP:
            dst->binary.left = copy_expr(src->binary.left);
            dst->binary.right = copy_expr(src->binary.right);
            break;
        case EXPR_UNARY_OP:
            dst->unary.operand = copy_expr(src->unary.operand);
            break;
        case EXPR_AGGREGATE_FUNC:
            dst->aggregate.operand = copy_expr(src->aggregate.operand);
            break;
        case EXPR_SCALAR_FUNC:
            dst->scalar.arg_count = src->scalar.arg_count;
            for (int i = 0; i < src->scalar.arg_count; i++) {
                dst->scalar.args[i] = copy_expr(src->scalar.args[i]);
            }
            break;
        case EXPR_SUBQUERY:
            dst->subquery.subquery = NULL;
            break;
        default:
            break;
    }
    return dst;
}

extern void free_expr_contents(Expr* expr);

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
            case AST_CREATE_TABLE: {
                new_ir->type = IR_CREATE_TABLE;
                strcpy(new_ir->create_table.table_name, current->create_table.table_name);
                alist_init(&new_ir->create_table.columns, sizeof(ColumnDef), NULL);
                int col_count = alist_length(&current->create_table.columns);
                for (int i = 0; i < col_count; i++) {
                    ColumnDef* col = (ColumnDef*)alist_get(&current->create_table.columns, i);
                    if (col) {
                        ColumnDef* new_col = (ColumnDef*)alist_append(&new_ir->create_table.columns);
                        if (new_col) *new_col = *col;
                    }
                }
                break;
            }

            case AST_INSERT_ROW: {
                new_ir->type = IR_INSERT_ROW;
                new_ir->insert_row.table_id = current->insert.table_id;
                alist_init(&new_ir->insert_row.values, sizeof(Value), (ArrayListFree)free_value);
                for (int i = 0; i < alist_length(&current->insert.values); i++) {
                    ColumnValue* cv = (ColumnValue*)alist_get(&current->insert.values, i);
                    if (cv) {
                        Value* val = (Value*)alist_append(&new_ir->insert_row.values);
                        if (val) *val = copy_value(&cv->value);
                    }
                }
                break;
            }

            case AST_SELECT: {
                int table_id = current->select.table_id;
                int expr_count = alist_length(&current->select.expressions);
                int join_table_id = current->select.join_table_id;
                bool has_join = (current->select.join_type != JOIN_NONE && join_table_id >= 0);

                bool has_aggregates = false;
                for (int i = 0; i < expr_count; i++) {
                    Expr** expr_ptr = (Expr**)alist_get(&current->select.expressions, i);
                    if (expr_ptr && *expr_ptr && (*expr_ptr)->type == EXPR_AGGREGATE_FUNC) {
                        has_aggregates = true;
                        break;
                    }
                }

                if (has_join) {
                    new_ir->type = IR_SCAN_TABLE;
                    new_ir->scan_table.table_id = table_id;

                    IRNode* current_ir = new_ir;

                    IRNode* join_ir = malloc(sizeof(IRNode));
                    if (join_ir) {
                        join_ir->type = IR_JOIN;
                        join_ir->join.left_table_id = table_id;
                        join_ir->join.right_table_id = join_table_id;
                        join_ir->join.type = current->select.join_type;
                        join_ir->join.condition = current->select.join_condition ? copy_expr(current->select.join_condition) : NULL;
                        join_ir->next = NULL;
                        current_ir->next = join_ir;
                        current_ir = join_ir;
                    }

                    if (has_aggregates) {
                        IRNode* aggregate_ir = malloc(sizeof(IRNode));
                        if (aggregate_ir) {
                            aggregate_ir->type = IR_AGGREGATE;
                            aggregate_ir->aggregate.table_id = table_id;
                            if (expr_count > 0) {
                                Expr** expr_ptr = (Expr**)alist_get(&current->select.expressions, 0);
                                Expr* agg_expr = expr_ptr ? *expr_ptr : NULL;
                                if (agg_expr && agg_expr->type == EXPR_AGGREGATE_FUNC) {
                                    aggregate_ir->aggregate.func_type = agg_expr->aggregate.func_type;
                                    aggregate_ir->aggregate.operand = copy_expr(agg_expr->aggregate.operand);
                                    agg_expr->aggregate.operand = NULL;
                                    aggregate_ir->aggregate.distinct = agg_expr->aggregate.distinct;
                                    aggregate_ir->aggregate.count_all = agg_expr->aggregate.count_all;
                                }
                            }
                            aggregate_ir->next = NULL;
                            current_ir->next = aggregate_ir;
                            current_ir = aggregate_ir;
                        }
                    }

                    IRNode* project_ir = malloc(sizeof(IRNode));
                    if (project_ir) {
                        project_ir->type = IR_PROJECT;
                        project_ir->project.table_id = table_id;
                        alist_init(&project_ir->project.expressions, sizeof(Expr*), NULL);
                        for (int i = 0; i < expr_count; i++) {
                            Expr** expr_ptr = (Expr**)alist_get(&current->select.expressions, i);
                            if (expr_ptr) {
                                Expr** dest = (Expr**)alist_append(&project_ir->project.expressions);
                                if (dest) *dest = *expr_ptr;
                            }
                        }
                        project_ir->project.limit = current->select.limit;
                        project_ir->next = NULL;
                        current_ir->next = project_ir;
                    }
                } else {
                    if (has_aggregates) {
                        new_ir->type = IR_SCAN_TABLE;
                        new_ir->scan_table.table_id = table_id;

                        IRNode* current_ir = new_ir;
                        if (current->select.where_clause) {
                            IRNode* filter_ir = malloc(sizeof(IRNode));
                            if (filter_ir) {
                                filter_ir->type = IR_FILTER;
                                filter_ir->filter.table_id = table_id;
                                filter_ir->filter.filter_expr = copy_expr(current->select.where_clause);
                                filter_ir->next = NULL;
                                current_ir->next = filter_ir;
                                current_ir = filter_ir;
                            }
                        }

                        IRNode* aggregate_ir = malloc(sizeof(IRNode));
                        if (aggregate_ir) {
                            aggregate_ir->type = IR_AGGREGATE;
                            aggregate_ir->aggregate.table_id = table_id;
                            if (expr_count > 0) {
                                Expr** expr_ptr = (Expr**)alist_get(&current->select.expressions, 0);
                                Expr* agg_expr = expr_ptr ? *expr_ptr : NULL;
                                if (agg_expr && agg_expr->type == EXPR_AGGREGATE_FUNC) {
                                    aggregate_ir->aggregate.func_type = agg_expr->aggregate.func_type;
                                    aggregate_ir->aggregate.operand = copy_expr(agg_expr->aggregate.operand);
                                    agg_expr->aggregate.operand = NULL;
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
                            project_ir->project.table_id = table_id;
                            alist_init(&project_ir->project.expressions, sizeof(Expr*), NULL);
                            for (int i = 0; i < expr_count; i++) {
                                Expr** expr_ptr = (Expr**)alist_get(&current->select.expressions, i);
                                if (expr_ptr) {
                                    Expr** dest = (Expr**)alist_append(&project_ir->project.expressions);
                                    if (dest) *dest = *expr_ptr;
                                }
                            }
                            project_ir->project.limit = current->select.limit;
                            project_ir->next = NULL;
                            current_ir->next = project_ir;
                        }

                    } else {
                        new_ir->type = IR_SCAN_TABLE;
                        new_ir->scan_table.table_id = table_id;

                        IRNode* current_ir = new_ir;

                        if (current->select.where_clause) {
                            IRNode* filter_ir = malloc(sizeof(IRNode));
                            if (filter_ir) {
                                filter_ir->type = IR_FILTER;
                                filter_ir->filter.table_id = table_id;
                                filter_ir->filter.filter_expr = copy_expr(current->select.where_clause);
                                filter_ir->next = NULL;
                                current_ir->next = filter_ir;
                                current_ir = filter_ir;
                            }
                        }

                        int order_by_count = alist_length(&current->select.order_by);
                        IRNode* sort_ir = NULL;
                        if (order_by_count > 0) {
                            log_msg(LOG_DEBUG, "ast_to_ir: Adding SORT node for %d order_by columns",
                                    order_by_count);
                            sort_ir = malloc(sizeof(IRNode));
                            if (sort_ir) {
                                sort_ir->type = IR_SORT;
                                sort_ir->sort.table_id = table_id;
                                alist_init(&sort_ir->sort.order_by, sizeof(Expr*), NULL);
                                alist_init(&sort_ir->sort.order_by_desc, sizeof(bool), NULL);
                                for (int i = 0; i < order_by_count; i++) {
                                    Expr** expr_ptr = (Expr**)alist_get(&current->select.order_by, i);
                                    if (expr_ptr) {
                                        Expr** dest = (Expr**)alist_append(&sort_ir->sort.order_by);
                                        if (dest) *dest = *expr_ptr;
                                    }
                                    bool* desc_ptr = (bool*)alist_get(&current->select.order_by_desc, i);
                                    bool* dest = (bool*)alist_append(&sort_ir->sort.order_by_desc);
                                    if (dest) *dest = desc_ptr ? *desc_ptr : false;
                                }
                                sort_ir->next = NULL;
                                current_ir->next = sort_ir;
                                current_ir = sort_ir;
                            }
                        }

                        IRNode* project_ir = malloc(sizeof(IRNode));
                        if (project_ir) {
                            project_ir->type = IR_PROJECT;
                            project_ir->project.table_id = table_id;
                            alist_init(&project_ir->project.expressions, sizeof(Expr*), NULL);
                            for (int i = 0; i < expr_count; i++) {
                                Expr** expr_ptr = (Expr**)alist_get(&current->select.expressions, i);
                                if (expr_ptr) {
                                    Expr** dest = (Expr**)alist_append(&project_ir->project.expressions);
                                    if (dest) *dest = *expr_ptr;
                                }
                            }
                            project_ir->project.limit = current->select.limit;
                            project_ir->next = NULL;
                            current_ir->next = project_ir;
                        }
                    }
                }
            } break;

            case AST_JOIN: {
                new_ir->type = IR_JOIN;
                new_ir->join.left_table_id = current->join.left_table_id;
                new_ir->join.right_table_id = current->join.right_table_id;
                new_ir->join.type = current->join.type;
                new_ir->join.condition = current->join.condition ? copy_expr(current->join.condition) : NULL;
                break;
            }

            case AST_DROP_TABLE: {
                new_ir->type = IR_DROP_TABLE;
                new_ir->drop_table.table_id = current->drop_table.table_id;
                break;
            }

            case AST_UPDATE_ROW: {
                new_ir->type = IR_UPDATE_ROW;
                new_ir->update_row.table_id = current->update.table_id;
                alist_init(&new_ir->update_row.values, sizeof(ColumnValue), NULL);
                for (int i = 0; i < alist_length(&current->update.values); i++) {
                    ColumnValue* cv = (ColumnValue*)alist_get(&current->update.values, i);
                    if (cv) {
                        ColumnValue* dest = (ColumnValue*)alist_append(&new_ir->update_row.values);
                        if (dest) *dest = *cv;
                    }
                }
                new_ir->update_row.where_clause = copy_expr(current->update.where_clause);
                break;
            }

            case AST_DELETE_ROW: {
                new_ir->type = IR_DELETE_ROW;
                new_ir->delete_row.table_id = current->delete.table_id;
                new_ir->delete_row.where_clause = copy_expr(current->delete.where_clause);
                break;
            }

            case AST_CREATE_INDEX: {
                new_ir->type = IR_CREATE_INDEX;
                new_ir->create_index.table_id = current->create_index.table_id;
                strcpy(new_ir->create_index.column_name, current->create_index.column_name);
                strcpy(new_ir->create_index.index_name, current->create_index.index_name);
                break;
            }

            case AST_DROP_INDEX: {
                new_ir->type = IR_DROP_INDEX;
                new_ir->drop_index.table_id = current->drop_index.table_id;
                strcpy(new_ir->drop_index.index_name, current->drop_index.index_name);
                break;
            }
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

    return ir;
}

void free_ir(IRNode* ir) {
    IRNode* current = ir;
    while (current) {
        IRNode* next = current->next;
        switch (current->type) {
            case IR_INSERT_ROW:
                alist_destroy(&current->insert_row.values);
                break;
            case IR_CREATE_TABLE:
                break;
            case IR_UPDATE_ROW:
                alist_destroy(&current->update_row.values);
                if (current->update_row.where_clause) {
                    free_expr_contents(current->update_row.where_clause);
                    free(current->update_row.where_clause);
                }
                break;
            case IR_DELETE_ROW:
                if (current->delete_row.where_clause) {
                    free_expr_contents(current->delete_row.where_clause);
                    free(current->delete_row.where_clause);
                }
                break;
            case IR_FILTER:
                if (current->filter.filter_expr) {
                    free_expr_contents(current->filter.filter_expr);
                    free(current->filter.filter_expr);
                }
                break;
            case IR_AGGREGATE:
                if (current->aggregate.operand) {
                    free_expr_contents(current->aggregate.operand);
                    free(current->aggregate.operand);
                }
                break;
            case IR_PROJECT:
                alist_destroy(&current->project.expressions);
                break;
            case IR_SORT:
                alist_destroy(&current->sort.order_by);
                alist_destroy(&current->sort.order_by_desc);
                break;
            default:
                break;
        }
        free(current);
        current = next;
    }
}
