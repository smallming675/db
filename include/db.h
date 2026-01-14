#ifndef DBMS_H
#define DBMS_H

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_TABLES 32
#define MAX_TOKEN_LEN 256
#define MAX_TABLE_NAME_LEN 64
#define MAX_COLUMN_NAME_LEN 64
#define MAX_COLUMNS 32
#define MAX_ROWS 1000
#define MAX_STRING_LEN 256

typedef enum {
  TOKEN_KEYWORD,
  TOKEN_IDENTIFIER,
  TOKEN_STRING,
  TOKEN_NUMBER,
  TOKEN_OPERATOR,
  TOKEN_COMMA,
  TOKEN_SEMICOLON,
  TOKEN_LPAREN,
  TOKEN_RPAREN,
  TOKEN_EOF,
  TOKEN_ERROR,
  TOKEN_EQUALS,        
  TOKEN_NOT_EQUALS,    
  TOKEN_LESS,          
  TOKEN_LESS_EQUAL,    
  TOKEN_GREATER,       
  TOKEN_GREATER_EQUAL, 
  TOKEN_AND,
  TOKEN_OR,
  TOKEN_NOT,
  TOKEN_LIKE,
  TOKEN_AGGREGATE_FUNC, 
  TOKEN_DISTINCT       
} TokenType;

typedef struct {
  TokenType type;
  char value[MAX_TOKEN_LEN];
} Token;

typedef enum { TYPE_INT, TYPE_STRING, TYPE_FLOAT } DataType;

typedef struct {
  char name[MAX_COLUMN_NAME_LEN];
  DataType type;
  bool nullable;
} ColumnDef;

typedef struct {
  char name[MAX_TABLE_NAME_LEN];
  ColumnDef columns[MAX_COLUMNS];
  int column_count;
} TableDef;

typedef enum {
  AST_CREATE_TABLE,
  AST_INSERT_ROW,
  AST_SELECT,
  AST_DROP_TABLE,
  AST_UPDATE_ROW,
  AST_DELETE_ROW
} ASTType;

typedef struct {
  char value[MAX_STRING_LEN];
  DataType type;
} Value;

typedef struct {
  char column_name[MAX_COLUMN_NAME_LEN];
  Value value;
} ColumnValue;

typedef enum {
  EXPR_COLUMN,
  EXPR_VALUE,
  EXPR_BINARY_OP,
  EXPR_UNARY_OP,
  EXPR_AGGREGATE_FUNC 
} ExprType;

typedef enum {
  OP_EQUALS,
  OP_NOT_EQUALS,
  OP_LESS,
  OP_LESS_EQUAL,
  OP_GREATER,
  OP_GREATER_EQUAL,
  OP_AND,
  OP_OR,
  OP_NOT,
  OP_LIKE
} OperatorType;

typedef struct Expr {
  ExprType type;
  union {
    char column_name[MAX_COLUMN_NAME_LEN];
    Value value;
    struct {
      OperatorType op;
      struct Expr *left;
      struct Expr *right;
    } binary;
    struct {
      OperatorType op;
      struct Expr *operand;
    } unary;
    struct {
      char func_name[16];   
      struct Expr *operand; 
      bool distinct;       
      bool count_all;     
    } aggregate;         
  };
} Expr;

typedef struct {
  TableDef table_def;
} CreateTableNode;

typedef struct {
  char table_name[MAX_TABLE_NAME_LEN];
  ColumnValue values[MAX_COLUMNS];
  int value_count;
} InsertNode;

typedef struct {
  char table_name[MAX_TABLE_NAME_LEN];
  Expr *expressions[MAX_COLUMNS]; 
  int expression_count;          
  Expr *where_clause;
} SelectNode;

typedef struct {
  char table_name[MAX_TABLE_NAME_LEN];
} DropTableNode;

typedef struct {
  char table_name[MAX_TABLE_NAME_LEN];
  ColumnValue values[MAX_COLUMNS];
  int value_count;
  Expr *where_clause;
} UpdateNode;

typedef struct {
  char table_name[MAX_TABLE_NAME_LEN];
  Expr *where_clause;
} DeleteNode;

typedef struct ASTNode {
  ASTType type;
  union {
    CreateTableNode create_table;
    InsertNode insert;
    SelectNode select;
    DropTableNode drop_table;
    UpdateNode update;
    DeleteNode delete;
  };
  struct ASTNode *next;
} ASTNode;

typedef enum {
  IR_CREATE_TABLE,
  IR_INSERT_ROW,
  IR_SCAN_TABLE,
  IR_DROP_TABLE,
  IR_UPDATE_ROW,
  IR_DELETE_ROW,
  IR_FILTER,
  IR_AGGREGATE, 
  IR_PROJECT   
} IRType;

typedef struct {
  TableDef table_def;
} IRCreateTable;

typedef struct {
  char table_name[MAX_TABLE_NAME_LEN];
  Value values[MAX_COLUMNS];
  int value_count;
} IRInsertRow;

typedef struct {
  char table_name[MAX_TABLE_NAME_LEN];
} IRScanTable;

typedef struct {
  char table_name[MAX_TABLE_NAME_LEN];
} IRDropTable;

typedef struct {
  char table_name[MAX_TABLE_NAME_LEN];
  ColumnValue values[MAX_COLUMNS];
  int value_count;
  Expr *where_clause;
} IRUpdateRow;

typedef struct {
  char table_name[MAX_TABLE_NAME_LEN];
  Expr *where_clause;
} IRDeleteRow;

typedef struct {
  char table_name[MAX_TABLE_NAME_LEN];
  Expr *filter_expr;
} IRFilter;

typedef struct {
  char table_name[MAX_TABLE_NAME_LEN];
  char aggregate_func[16]; 
  Expr *operand;           
  bool distinct;           
  bool count_all;          
} IRAggregate;             

typedef struct {
  char table_name[MAX_TABLE_NAME_LEN];
  Expr *expressions[MAX_COLUMNS]; 
  int expression_count;
} IRProject; 

typedef struct IRNode {
  IRType type;
  union {
    IRCreateTable create_table;
    IRInsertRow insert_row;
    IRScanTable scan_table;
    IRDropTable drop_table;
    IRUpdateRow update_row;
    IRDeleteRow delete_row;
    IRFilter filter;
    IRAggregate aggregate; 
    IRProject project;     
  };
  struct IRNode *next;
} IRNode;

typedef struct {
  Value values[MAX_COLUMNS];
  bool is_null[MAX_COLUMNS];
} Row;

typedef struct {
  char name[MAX_TABLE_NAME_LEN];
  TableDef schema;
  Row rows[MAX_ROWS];
  int row_count;
} Table;


typedef struct {
  double sum;
  int count;
  Value min_val;
  Value max_val;
  bool has_min, has_max;
  bool is_distinct;
  char *seen_values[MAX_ROWS];
  int distinct_count;
} AggregateState;

Token *tokenize(const char *input);

ASTNode *parse(Token *tokens);
IRNode *ast_to_ir(ASTNode *ast);
void exec_ir(IRNode *ir);
void free_tokens(Token *tokens);
void free_ast(ASTNode *ast);
void free_ir(IRNode *ir);

#endif
