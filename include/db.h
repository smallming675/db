#ifndef DB_H
#define DB_H

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
  TOKEN_SCALAR_FUNC,
  TOKEN_DISTINCT,
  TOKEN_TIME,
  TOKEN_DATE,
  TOKEN_ORDER,
  TOKEN_BY,
  TOKEN_AS,
  TOKEN_EXISTS,
  TOKEN_IN
} TokenType;

typedef struct {
  TokenType type;
  char value[MAX_TOKEN_LEN];
} Token;

typedef enum {
  TYPE_INT,
  TYPE_STRING,
  TYPE_FLOAT,
  TYPE_TIME,
  TYPE_DATE,
  TYPE_NULL,
  TYPE_ERROR
} DataType;

typedef struct {
  int hour;
  int minute;
  int second;
} TimeValue;

typedef struct {
  int year;
  int month;
  int day;
} DateValue;

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
  PARSE_ERROR_NONE = 0,
  PARSE_ERROR_UNEXPECTED_TOKEN,
  PARSE_ERROR_MISSING_TOKEN,
  PARSE_ERROR_INVALID_SYNTAX,
  PARSE_ERROR_UNTERMINATED_STRING,
  PARSE_ERROR_INVALID_NUMBER,
  PARSE_ERROR_UNEXPECTED_END,
  PARSE_ERROR_TOO_MANY_COLUMNS,
  PARSE_ERROR_MAX
} ParseErrorCode;

typedef struct {
  ParseErrorCode code;
  char message[1024];
  char expected[512];
  char found[256];
  int line;
  int column;
  int token_index;
  char input[1024];
  char suggestion[512];
} ParseError;

typedef struct {
  char input[1024];
  Token *tokens;
  int token_count;
  int current_token_index;
  ParseError error;
  bool error_occurred;
} ParseContext;

typedef enum {
  AST_CREATE_TABLE,
  AST_INSERT_ROW,
  AST_SELECT,
  AST_DROP_TABLE,
  AST_UPDATE_ROW,
  AST_DELETE_ROW
} ASTType;

typedef struct {
  DataType type;
  union {
    int int_val;
    double float_val;
    char *char_val;
    TimeValue time_val;
    DateValue date_val;
  };
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
  EXPR_AGGREGATE_FUNC,
  EXPR_SCALAR_FUNC,
  EXPR_SUBQUERY
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
  OP_LIKE,
  OP_ADD,
  OP_SUBTRACT,
  OP_MULTIPLY,
  OP_DIVIDE,
  OP_MODULUS,
  OP_IN,
  OP_NOT_IN,
  OP_EXISTS
} OperatorType;

typedef enum { FUNC_COUNT, FUNC_SUM, FUNC_AVG, FUNC_MIN, FUNC_MAX } AggFuncType;

typedef enum {
  FUNC_ABS,
  FUNC_SQRT,
  FUNC_MOD,
  FUNC_POW,
  FUNC_ROUND,
  FUNC_FLOOR,
  FUNC_CEIL,
  FUNC_UPPER,
  FUNC_LOWER,
  FUNC_LEN,
  FUNC_MID,
  FUNC_LEFT,
  FUNC_RIGHT,
  FUNC_CONCAT
} ScalarFuncType;

typedef struct ASTNode ASTNode;

typedef struct Expr {
  ExprType type;
  char alias[MAX_COLUMN_NAME_LEN];
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
      AggFuncType func_type;
      struct Expr *operand;
      bool distinct;
      bool count_all;
    } aggregate;
    struct {
      ScalarFuncType func_type;
      struct Expr *args[3];
      int arg_count;
    } scalar;
    struct {
      ASTNode *subquery;
    } subquery;
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
  Expr *order_by[MAX_COLUMNS];
  bool order_by_desc[MAX_COLUMNS];
  int order_by_count;
  int limit;
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
  IR_PROJECT,
  IR_SORT
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
  AggFuncType func_type;
  Expr *operand;
  bool distinct;
  bool count_all;
} IRAggregate;

typedef struct {
  char table_name[MAX_TABLE_NAME_LEN];
  Expr *expressions[MAX_COLUMNS];
  int expression_count;
  int limit;
} IRProject;

typedef struct {
  char table_name[MAX_TABLE_NAME_LEN];
  Expr *order_by[MAX_COLUMNS];
  bool order_by_desc[MAX_COLUMNS];
  int order_by_count;
} IRSort;

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
    IRSort sort;
  };
  struct IRNode *next;
} IRNode;

typedef struct {
  Value* values;
  bool* is_null;
  int value_count;
  int value_capacity;
} Row;

typedef struct {
  char name[MAX_TABLE_NAME_LEN];
  TableDef schema;
  Row* rows;
  int row_count;
  int row_capacity;
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
} AggState;

Token *tokenize(const char *input);

void parse_error_init(ParseContext *ctx, const char *input, Token *tokens,
                      int token_count);
void parse_error_set(ParseContext *ctx, ParseErrorCode code,
                     const char *message, const char *expected,
                     const char *found, const char *suggestion);
void parse_error_report(ParseContext *ctx);
const char *parse_error_code_str(ParseErrorCode code);

ASTNode *parse_with_context(ParseContext *ctx, Token *tokens);
ASTNode *parse(Token *tokens);
ASTNode *parse_ex(const char *input, Token *tokens);
ParseContext *parse_get_context(void);
IRNode *ast_to_ir(ASTNode *ast);
void exec_ir(IRNode *ir);
void free_tokens(Token *tokens);
void free_ast(ASTNode *ast);
void free_ir(IRNode *ir);

#endif
