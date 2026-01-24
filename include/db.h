#ifndef DB_H
#define DB_H

#include <stdbool.h>
#include <stdint.h>
#include "arraylist.h"

#define MAX_TABLES 32
#define MAX_TOKEN_LEN 64
#define MAX_TABLE_NAME_LEN 64
#define MAX_COLUMN_NAME_LEN 64
#define MAX_STRING_LEN 256
#define MAX_COLUMNS 32

#define COL_FLAG_NULLABLE (1 << 0)
#define COL_FLAG_PRIMARY_KEY (1 << 1)
#define COL_FLAG_UNIQUE (1 << 2)
#define COL_FLAG_FOREIGN_KEY (1 << 3)
#define COL_FLAG_CHECK (1 << 4)

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
    TOKEN_LEFT,
    TOKEN_STRICT
} TokenType;

typedef enum { INDEX_TYPE_HASH, INDEX_TYPE_BTREE } IndexType;

typedef struct {
    const char *name;
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
    TYPE_BOOLEAN,
    TYPE_DECIMAL,
    TYPE_BLOB,
    TYPE_TIME,
    TYPE_DATE,
    TYPE_NULL,
    TYPE_ERROR
} DataType;

typedef struct {
    char name[MAX_COLUMN_NAME_LEN];
    char references_table[MAX_TABLE_NAME_LEN];
    char references_column[MAX_COLUMN_NAME_LEN];
    DataType type;
    unsigned int flags;
    struct Expr *check_expr;
} ColumnDef;

typedef struct {
    ArrayList columns;           /* ColumnDef* */
    ArrayList check_constraints; /* Expr* */
    bool strict;
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
    PARSE_ERROR_TABLE_NOT_FOUND,
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
        long long int_val;
        double float_val;
        bool bool_val;
        struct {
            int precision;
            int scale;
            long long value;
        } decimal_val;
        struct {
            size_t length;
            unsigned char *data;
        } blob_val;
        char *char_val;
        unsigned int time_val;
        unsigned int date_val;
    };
} Value;

typedef struct {
    char column_name[MAX_COLUMN_NAME_LEN];
    int8_t column_idx;
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

typedef enum {
    FUNC_COUNT,
    FUNC_SUM,
    FUNC_AVG,
    FUNC_MIN,
    FUNC_MAX,
    FUNC_STDDEV,
    FUNC_VARIANCE
} AggFuncType;

typedef enum { AGG_PLAIN, AGG_DISTINCT, AGG_MIN, AGG_MAX } AggType;

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
    FUNC_CONCAT,
    FUNC_COALESCE,
    FUNC_NULLIF,
    FUNC_CASE,
    FUNC_TIME_HOUR,
    FUNC_TIME_MINUTE,
    FUNC_TIME_SECOND,
    FUNC_DATE_YEAR,
    FUNC_DATE_MONTH,
    FUNC_DATE_DAY
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

typedef enum { JOIN_NONE, JOIN_INNER, JOIN_LEFT } JoinType;

typedef struct {
    int left_table_id;
    int right_table_id;
    JoinType type;
    Expr *condition;
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

typedef enum { PLAN_SEQ_SCAN, PLAN_INDEX_SCAN } PlanType;

typedef struct {
    char table_name[MAX_TABLE_NAME_LEN];
    ArrayList columns; /* ColumnDef* */
    bool strict;
} CreateTableNode;

typedef struct {
    uint8_t table_id;
    ArrayList value_rows; /* ArrayList<ArrayList<Value>> */
    ArrayList columns;    /* ColumnValue* */
} InsertNode;

typedef struct {
    uint8_t table_id;
    ArrayList expressions; /* Expr* */
    Expr *where_clause;
    ArrayList order_by;      /* Expr* */
    ArrayList order_by_desc; /* Expr* */
    int order_by_count;
    int limit;
    JoinType join_type;
    int join_table_id;
    char join_table_name[MAX_TABLE_NAME_LEN];
    Expr *join_condition;
    bool distinct;
} SelectNode;

typedef struct {
    uint8_t table_id;
} DropTableNode;

typedef struct {
    uint8_t table_id;
    int column_idx;
    char index_name[MAX_TABLE_NAME_LEN];
} CreateIndexNode;

typedef struct {
    uint8_t table_id;
    char index_name[MAX_TABLE_NAME_LEN];
} DropIndexNode;

typedef struct {
    uint8_t table_id;
    ArrayList values; /* ColumnValue* */
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

typedef ArrayList Row;

typedef struct {
    char name[MAX_TABLE_NAME_LEN];
    uint8_t table_id;
    TableDef schema;
    ArrayList rows; /* Row* (ArrayList<Value>) */
} Table;

typedef struct IndexEntry {
    Value key;
    int row_index;
    struct IndexEntry *next;
} IndexEntry;

typedef struct BTreeNode {
    Value *keys;
    int *row_indices;
    struct BTreeNode **children;
    struct BTreeNode *parent;
    int key_count;
    bool is_leaf;
} BTreeNode;

typedef struct {
    char index_name[MAX_TABLE_NAME_LEN];
    char table_name[MAX_TABLE_NAME_LEN];
    char column_name[MAX_COLUMN_NAME_LEN];
    char column_names[MAX_COLUMNS][MAX_COLUMN_NAME_LEN];
    int column_count;
    IndexType type;
    IndexEntry **buckets;
    int bucket_count;
    int entry_count;
    union {
        struct {
            BTreeNode *root;
            int order;
        } btree;
        IndexEntry **hash_buckets;
    } data;
} Index;

typedef struct {
    AggType type;
    union {
        struct {
            ArrayList seen_values; /* Value* */
            int distinct_count;
        } distinct;
        struct {
            Value min_val;
            bool has_min;
        } min;
        struct {
            Value max_val;
            bool has_max;
        } max;
    } data;
    double sum;
    uint32_t count;
} AggState;

typedef struct {
    uint32_t row_count;
    uint32_t distinct_count;
    double avg_width;
    bool has_stats;
    Value min_val;
    Value max_val;
} ColumnStats;

typedef struct {
    char table_name[MAX_TABLE_NAME_LEN];
    uint32_t total_rows;
    uint32_t distinct_values[MAX_COLUMNS];
    Value min_values[MAX_COLUMNS];
    Value max_values[MAX_COLUMNS];
    ColumnStats *column_stats;
    int column_count;
    bool has_stats;
} TableStats;

typedef struct SeqScanPlan {
    char table_name[MAX_TABLE_NAME_LEN];
    uint8_t table_id;
    Expr *where_clause;
} SeqScanPlan;

typedef struct IndexScanPlan {
    char table_name[MAX_TABLE_NAME_LEN];
    uint8_t table_id;
    Index *index;
    Expr *where_clause;
    OperatorType op;
    Value *search_key;
} IndexScanPlan;

typedef struct PlanNode {
    PlanType type;
    struct PlanNode *left;
    struct PlanNode *right;
    double cost;
    uint32_t estimated_rows;
    union {
        SeqScanPlan seq_scan;
        IndexScanPlan index_scan;
    } plan;
} PlanNode;

typedef struct {
    ArrayList values;       /* Value* */
    ArrayList rows;         /* int* */
    ArrayList column_names; /* char* */
    int col_count;
} QueryResult;

int time_hour(unsigned int time_val);
int time_minute(unsigned int time_val);
int time_second(unsigned int time_val);
int date_year(unsigned int date_val);
int date_month(unsigned int date_val);
int date_day(unsigned int date_val);

Value scalar_coalesce(Value *args, int arg_count);
Value scalar_nullif(Value *arg1, Value *arg2);
Value scalar_case(Value *condition, Value *then_val, Value *else_val);

void agg_init(AggState *state, AggFuncType func_type, bool distinct);
void agg_add_value(AggState *state, Value *value);
Value agg_get_result(AggState *state);
void agg_cleanup(AggState *state);

TableStats *get_table_stats(const char *table_name);
void collect_table_stats(const char *table_name);
double estimate_selectivity(const TableStats *stats, int col_idx, OperatorType op,
                            const Value *value);
Index *find_index_by_table_column(const char *table_name, const char *column_name);

PlanNode *optimize_select(const char *table_name, Expr *where_clause);
PlanNode *create_seq_scan_plan(const char *table_name, Expr *where_clause);
void free_plan(PlanNode *plan);

void index_table_columns(const char *table_name, const char *columns[], int col_count,
                         const char *index_name, IndexType type);
Index *find_index(const char *index_name);
ArrayList *btree_find_equals(Index *index, const Value *key);
void assert_double_almost_eq(double expected, double actual, double tolerance, const char *message);

bool value_equals(const Value *a, const Value *b);
int compare_values(const Value *a, const Value *b);
void update_column_stats(TableStats *stats, int col_idx, const Value *value);
void init_stat(void);

Token *tokenize(const char *input);

void parse_error_report(ParseContext *ctx);

ASTNode *parse_with_context(ParseContext *ctx, Token *tokens);
ASTNode *parse(Token *tokens);
ASTNode *parse_ex(const char *input, Token *tokens);
ParseContext *parse_get_context(void);
void exec_ast(ASTNode *ast);
void free_tokens(Token *tokens);
void free_ast(ASTNode *ast);

void index_table_column(const char *table_name, const char *column_name, const char *index_name);
void drop_index_by_name(const char *index_name);
Index *find_index(const char *index_name);
int hash_value(const Value *value, int bucket_count);
void clear_query_result(void);

QueryResult *exec_query(const char *sql);
void free_query_result(QueryResult *result);

#endif
