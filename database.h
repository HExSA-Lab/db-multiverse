#ifndef __DATABASE_H__
#define __DATABASE_H__

#include "common.h"

typedef enum table_type
{
	COLUMN,
	ROW
} table_type_t;

typedef unsigned long row_id_t;
typedef unsigned long column_id_t;

typedef struct column {
    column_id_t col_id;
    const char * nm;
} column_t;

typedef struct row {
    row_id_t row_id;
    column_t * columns;
} row_t;

typedef struct row_table {
    row_t * rows;
    size_t num;
} row_table_t;

typedef uint32_t val_t;

typedef struct column_chunk {
	size_t chunk_size;
	val_t *data;
} column_chunk_t;

typedef struct table_chunk {
	column_chunk_t ** columns;
} table_chunk_t;

typedef struct col_table {
	size_t num_chunks;
	size_t num_cols;
	size_t num_rows;
	table_chunk_t ** chunks;
} col_table_t;

typedef col_table_t* (*op_implementation_t) ();

void print_table_info (col_table_t *t);
col_table_t *create_col_table (size_t num_chunks, size_t chunk_size, size_t num_cols, unsigned int domain_size);
void free_col_table (col_table_t *t);
void copy_row(col_table_t *src, size_t src_row, col_table_t *dst, size_t dst_row);
val_t get_cell(col_table_t *src, size_t row, size_t column);
column_chunk_t *create_col_chunk(size_t chunksize);
void free_col_chunk (column_chunk_t *c);
size_t get_chunk_size(col_table_t *t);

#endif
