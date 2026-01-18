#ifndef EXECUTOR_H
#define EXECUTOR_H

#include "db.h"

void exec_ast(ASTNode* ast);
QueryResult* exec_query(const char* sql);
void free_query_result(QueryResult* result);

#endif
