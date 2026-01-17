#ifndef DB_H
#define DB_H

#include <stdbool.h>
#include <stdint.h>
#include "arraylist.h"

#define MAX_TABLES 32
#define MAX_ROWS 1000
#define MAX_TOKEN_LEN 64
#define MAX_TABLE_NAME_LEN 64
#define MAX_COLUMN_NAME_LEN 64
#define MAX_STRING_LEN 256

#define COL_FLAG_NULLABLE      (1 << 0)
#define COL_FLAG_PRIMARY_KEY   (1 << 1)
#define COL_FLAG_UNIQUE        (1 << 2)
#define COL_FLAG_FOREIGN_KEY   (1 << 3)

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
  TOKEN_IN,
  TOKEN_PRIMARY,
  TOKEN_KEY,
  TOKEN_REFERENCES,
  TOKEN_NULL,
  TOKEN_UNIQUE,
  TOKEN_JOIN,
  TOKEN_INNER,
  TOKEN_LEFT
} TokenType;

typedef struct {
  const char* name;
  TokenType type;
} KeywordMap;

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
  unsigned int time_val;
} TimeValue;

typedef struct {
  unsigned int date_val;
} DateValue;

typedef struct {
  char name[MAX_COLUMN_NAME_LEN];
  char references_table[MAX_TABLE_NAME_LEN];
  char references_column[MAX_COLUMN_NAME_LEN];
  DataType type;
  unsigned int flags;
} ColumnDef;

typedef struct {
  char name[MAX_TABLE_NAME_LEN];
  ArrayList columns;
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

struct ASTNode;

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
      struct ASTNode *subquery;
    } subquery;
  };
} Expr;

typedef enum {
  JOIN_NONE,
  JOIN_INNER,
  JOIN_LEFT
} JoinType;

typedef struct {
  int left_table_id;
  int right_table_id;
  JoinType type;
  Expr* condition;
} JoinNode;

typedef enum {
  AST_CREATE_TABLE,
  AST_INSERT_ROW,
  AST_SELECT,
  AST_DROP_TABLE,
  AST_UPDATE_ROW,
  AST_DELETE_ROW,
  AST_CREATE_INDEX,
  AST_DROP_INDEX,
  AST_JOIN
} ASTType;

typedef struct {
  char table_name[MAX_TABLE_NAME_LEN];
  ArrayList columns;
} CreateTableNode;

typedef struct {
  uint8_t table_id;
  ArrayList values;
} InsertNode;

typedef struct {
  uint8_t table_id;
  ArrayList expressions;
  Expr *where_clause;
  ArrayList order_by;
  ArrayList order_by_desc;
  int order_by_count;
  int limit;
  JoinType join_type;
  int join_table_id;
  char join_table_name[MAX_TABLE_NAME_LEN];
  Expr *join_condition;
} SelectNode;

typedef struct {
  uint8_t table_id;
} DropTableNode;

typedef struct {
  uint8_t table_id;
  char column_name[MAX_COLUMN_NAME_LEN];
  char index_name[MAX_TABLE_NAME_LEN];
} CreateIndexNode;

typedef struct {
  uint8_t table_id;
  char index_name[MAX_TABLE_NAME_LEN];
} DropIndexNode;

typedef struct {
  uint8_t table_id;
  ArrayList values;
  Expr *where_clause;
} UpdateNode;

typedef struct {
  uint8_t table_id;
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
    CreateIndexNode create_index;
    DropIndexNode drop_index;
    JoinNode join;
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
  IR_SORT,
  IR_CREATE_INDEX,
  IR_DROP_INDEX,
  IR_JOIN
} IRType;

typedef struct {
  char table_name[MAX_TABLE_NAME_LEN];
  ArrayList columns;
} IRCreateTable;

typedef struct {
  uint8_t table_id;
  ArrayList values;
} IRInsertRow;

typedef struct {
  uint8_t table_id;
} IRScanTable;

typedef struct {
  uint8_t table_id;
} IRDropTable;

typedef struct {
  uint8_t table_id;
  ArrayList values;
  Expr *where_clause;
} IRUpdateRow;

typedef struct {
  uint8_t table_id;
  Expr *where_clause;
} IRDeleteRow;

typedef struct {
  uint8_t table_id;
  Expr *filter_expr;
} IRFilter;

typedef struct {
  uint8_t table_id;
  AggFuncType func_type;
  Expr *operand;
  bool distinct;
  bool count_all;
} IRAggregate;

typedef struct {
  uint8_t table_id;
  ArrayList expressions;
  int limit;
} IRProject;

typedef struct {
  uint8_t table_id;
  ArrayList order_by;
  ArrayList order_by_desc;
} IRSort;

typedef struct {
  uint8_t table_id;
  char column_name[MAX_COLUMN_NAME_LEN];
  char index_name[MAX_TABLE_NAME_LEN];
} IRCreateIndex;

typedef struct {
  uint8_t table_id;
  char index_name[MAX_TABLE_NAME_LEN];
} IRDropIndex;

typedef struct {
  int left_table_id;
  int right_table_id;
  JoinType type;
  Expr* condition;
} IRJoin;

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
    IRCreateIndex create_index;
    IRDropIndex drop_index;
    IRJoin join;
  };
  struct IRNode *next;
} IRNode;

typedef struct Row {
  Value *values;
  int value_count;
  int value_capacity;
} Row;

typedef struct {
  char name[MAX_TABLE_NAME_LEN];
  uint8_t table_id;
  TableDef schema;
  ArrayList rows;
} Table;

typedef struct IndexEntry {
  Value key;
  int row_index;
  struct IndexEntry *next;
} IndexEntry;

typedef struct {
  char index_name[MAX_TABLE_NAME_LEN];
  char table_name[MAX_TABLE_NAME_LEN];
  char column_name[MAX_COLUMN_NAME_LEN];
  IndexEntry **buckets;
  int bucket_count;
  int entry_count;
} Index;

typedef struct {
  double sum;
  int count;
  Value min_val;
  Value max_val;
  bool has_min, has_max;
  bool is_distinct;
  ArrayList seen_values;
  int distinct_count;
} AggState;

int time_hour(unsigned int time_val);
int time_minute(unsigned int time_val);
int time_second(unsigned int time_val);
int date_year(unsigned int date_val);
int date_month(unsigned int date_val);
int date_day(unsigned int date_val);

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

void exec_create_index(const IRNode *current);
void exec_drop_index(const IRNode *current);
void index_table_column(const char *table_name, const char *column_name,
                        const char *index_name);
void drop_index_by_name(const char *index_name);
Index *find_index(const char *index_name);
int hash_value(const Value *value, int bucket_count);
void clear_query_result(void);

typedef struct {
  Value *values;
  int *rows;
  int row_count;
  int col_count;
  char **column_names;
} QueryResult;

QueryResult *exec_query(const char *sql);
void free_query_result(QueryResult *result);

#endif
