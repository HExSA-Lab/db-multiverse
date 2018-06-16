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
