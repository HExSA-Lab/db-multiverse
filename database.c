#include "database.h"
#include "timing.h"

val_t
randVal(unsigned int domain_size)
{
	return (val_t) rand() % domain_size;
}

void
print_table_info (col_table_t *t)
{
	printf("Table with %lu columns, %lu rows, and %lu chunks\n", t->num_cols, t->num_rows, t->num_chunks);
}

col_table_t *
create_col_table (size_t num_chunks, size_t chunk_size, size_t num_cols, unsigned int domain_size)
{
	col_table_t * t = NEW(col_table_t);
	t->num_chunks = num_chunks;
	t->num_cols = num_cols;
	t->num_rows = t->num_chunks * chunk_size;

	t->chunks = NEWPA(table_chunk_t, num_chunks);
	MALLOC_CHECK_NO_MES(t->chunks);

	TIMEIT("create col table",
		for(size_t i = 0; i < num_chunks; i++)
		{
			table_chunk_t *tc = NEW(table_chunk_t);
			MALLOC_CHECK(tc, "table chunks");

			t->chunks[i] = tc;
			tc->columns = NEWPA(column_chunk_t, num_cols);
			MALLOC_CHECK_NO_MES(tc->columns);

			for (size_t j = 0; j < num_cols; j++)
			{
				column_chunk_t *c = NEW(column_chunk_t);
				MALLOC_CHECK(c, "column chunks");

				tc->columns[j] = c;
				c->chunk_size = chunk_size;
				c->data = NEWA(val_t, chunk_size);
				MALLOC_CHECK(c->data, "chunk data");

				for(size_t k = 0; k < chunk_size; k++)
				{
					c->data[k] = randVal(domain_size);
				}
			}
		}
	);

	INFO("created table: ");
	print_table_info(t);

	return t;
}

inline void
copy_row(col_table_t *src, size_t src_row, col_table_t *dst, size_t dst_row) {
	size_t src_chunk     = src_row / get_chunk_size(src);
	size_t src_chunk_row = src_row % get_chunk_size(src);
	size_t dst_chunk     = dst_row / get_chunk_size(dst);
	size_t dst_chunk_row = dst_row % get_chunk_size(dst);

	for(size_t column = 0; column < src->num_cols; ++column) {
		dst->chunks[dst_chunk]->columns[column]->data[dst_chunk_row] =
		src->chunks[src_chunk]->columns[column]->data[src_chunk_row];
	}
}

inline val_t
get_cell(col_table_t *src, size_t row, size_t column) {
	size_t chunk     = row / get_chunk_size(src);
	size_t chunk_row = row % get_chunk_size(src);
	return src->chunks[chunk]->columns[column]->data[chunk_row];
}

inline void
free_col_chunk (column_chunk_t *c)
{
	free(c->data);
	free(c);
}

void
free_col_table (col_table_t *t)
{
	for(size_t i = 0; i < t->num_chunks; i++) {
		table_chunk_t * tc = t->chunks[i];

		for (size_t j = 0; j < t->num_cols; j++)
		{
			column_chunk_t *c = tc->columns[j];
			free_col_chunk(c);
		}
		free(tc);
	}

	free(t->chunks);
	free(t);
}

inline column_chunk_t *
create_col_chunk(size_t chunksize)
{
	column_chunk_t *result = NEW(column_chunk_t);
	MALLOC_CHECK_NO_MES(result);

	result->chunk_size = chunksize;
	result->data = NEWA(val_t, chunksize);
	MALLOC_CHECK_NO_MES(result->data);

	return result;
}

inline size_t
get_chunk_size(col_table_t *t) {
	return t->chunks[0]->columns[0]->chunk_size;
}
