#include "test_util.h"
#include "db.h"
#include "logger.h"

void test_real_type_and_comments(void) {
    log_msg(LOG_INFO, "Testing REAL type and multiline comments...");

    reset_database();

    exec("/* This is a multiline comment */ CREATE TABLE test_real (id INT, value REAL);");
    exec("INSERT INTO test_real VALUES (1, 3.14159); /* Another comment */");
    exec("INSERT INTO test_real VALUES (2, -2.71828);");

    exec("INSERT INTO test_real VALUES (3, 0.0);");

    Table *table = find_table_by_name("test_real");
    assert_ptr_not_null(table, "Table should exist");
    assert_int_eq(3, alist_length(&table->rows), "Table should have 3 rows");

    ColumnDef *col = (ColumnDef *)alist_get(&table->schema.columns, 1);
    assert_ptr_not_null(col, "Second column should exist");
    assert_int_eq(TYPE_FLOAT, col->type, "Second column should be REAL type");

    Row *row = (Row *)alist_get(&table->rows, 0);
    Value *val = (Value *)alist_get(row, 1);
    assert_int_eq(TYPE_FLOAT, val->type, "First row value should be REAL");
    assert_float_eq(3.14159, val->float_val, 0.00001, "First value should be pi");

    row = (Row *)alist_get(&table->rows, 1);
    val = (Value *)alist_get(row, 1);
    assert_int_eq(TYPE_FLOAT, val->type, "Second row value should be REAL (type 2)");
    assert_float_eq(-2.71828, val->float_val, 0.00001, "Second value should be -e");

    exec("INSERT INTO test_real VALUES (4, 1.41421); -- Square root of 2");

    log_msg(LOG_INFO, "REAL type and comments tests passed");
}

void test_primary_key_definitions(void) {
    log_msg(LOG_INFO, "Running PRIMARY KEY definition tests...");
    reset_database();

    exec("CREATE TABLE simple_pk (id INT PRIMARY KEY);");
    Table *table = find_table_by_name("simple_pk");
    assert_ptr_not_null(table, "Table should exist");
    assert_int_eq(1, alist_length(&table->schema.columns), "Table should have 1 column");

    ColumnDef *id_col = (ColumnDef *)alist_get(&table->schema.columns, 0);
    assert_ptr_not_null(id_col, "ID column should exist");
    assert_true((id_col->flags & COL_FLAG_PRIMARY_KEY), "ID column should be PRIMARY KEY");

    log_msg(LOG_INFO, "Simple PRIMARY KEY test passed");

    exec("CREATE TABLE pk_inline (id INT PRIMARY KEY, name STRING);");
    table = find_table_by_name("pk_inline");
    assert_ptr_not_null(table, "Table should exist");
    assert_int_eq(2, alist_length(&table->schema.columns), "Table should have 2 columns");

    id_col = (ColumnDef *)alist_get(&table->schema.columns, 0);
    ColumnDef *name_col = (ColumnDef *)alist_get(&table->schema.columns, 1);
    assert_ptr_not_null(id_col, "ID column should exist");
    assert_ptr_not_null(name_col, "Name column should exist");
    assert_true((id_col->flags & COL_FLAG_PRIMARY_KEY), "ID column should be PRIMARY KEY");
    assert_false((name_col->flags & COL_FLAG_PRIMARY_KEY), "Name column should not be PRIMARY KEY");
    assert_true((id_col->flags & COL_FLAG_UNIQUE), "PRIMARY KEY implies UNIQUE");

    exec("CREATE TABLE pk_constraint (ref_id INT, name STRING, PRIMARY KEY (ref_id));");
    table = find_table_by_name("pk_constraint");
    assert_ptr_not_null(table, "Constraint table should exist");
    assert_int_eq(2, alist_length(&table->schema.columns), "Table should have 2 columns");

    ColumnDef *ref_col = (ColumnDef *)alist_get(&table->schema.columns, 0);
    assert_ptr_not_null(ref_col, "Ref column should exist");
    assert_true((ref_col->flags & COL_FLAG_PRIMARY_KEY), "Ref column should be PRIMARY KEY");

    exec("CREATE TABLE pk_multi (id INT PRIMARY KEY, value INT, PRIMARY KEY (id), name STRING "
         "UNIQUE);");
    table = find_table_by_name("pk_multi");
    assert_ptr_not_null(table, "Multi-key table should exist");
    assert_int_eq(3, alist_length(&table->schema.columns), "Table should have 3 columns");

    bool found_id_pk = false, found_value_pk = false;
    for (int i = 0; i < alist_length(&table->schema.columns); i++) {
        ColumnDef *col = (ColumnDef *)alist_get(&table->schema.columns, i);
        if (strcmp(col->name, "id") == 0) {
            assert_true((col->flags & COL_FLAG_PRIMARY_KEY), "ID column should be PRIMARY KEY");
            found_id_pk = true;
        }
        if (strcmp(col->name, "value") == 0) {
            assert_true((col->flags & COL_FLAG_PRIMARY_KEY), "Value column should be PRIMARY KEY");
            found_value_pk = true;
        }
    }
    assert_true(found_id_pk, "Should find ID PRIMARY KEY");
    assert_true(found_value_pk, "Should find VALUE PRIMARY KEY");

    log_msg(LOG_INFO, "Primary key definition tests passed");
}
