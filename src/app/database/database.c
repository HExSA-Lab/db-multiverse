#ifdef __NAUTILUS__
	#include <nautilus/libccompat.h>
#else
	#include <string.h>
#endif

#include "app/database/database.h"
#include "app/database/rand.h"

void
print_table_info (col_table_t *t) {
	printf("Table with %lu columns, %lu rows, and %lu chunks\n", t->num_cols, t->num_rows, t->num_chunks);
}

col_table_t *
create_col_table (size_t num_chunks, size_t chunk_size, size_t num_cols, unsigned int domain_size) {
	col_table_t * t = NEW(col_table_t);
	t->num_chunks = num_chunks;
	t->num_cols = num_cols;
	t->num_rows = t->num_chunks * chunk_size;

	t->chunks = NEWPA(table_chunk_t, num_chunks);
	MALLOC_CHECK_NO_MES(t->chunks);

	for(size_t i = 0; i < num_chunks; i++) {
		table_chunk_t *tc = NEW(table_chunk_t);
		MALLOC_CHECK(tc, "table chunks");

		t->chunks[i] = tc;
		tc->columns = NEWPA(column_chunk_t, num_cols);
		MALLOC_CHECK_NO_MES(tc->columns);

		for (size_t j = 0; j < num_cols; j++) {
			column_chunk_t *c = NEW(column_chunk_t);
			MALLOC_CHECK(c, "column chunks");

			tc->columns[j] = c;
			c->chunk_size = chunk_size;
			c->data = NEWA(val_t, chunk_size);
			MALLOC_CHECK(c->data, "chunk data");

			for(size_t k = 0; k < chunk_size; k++) {
				c->data[k] = rand_next(domain_size);
			}
		}
	}
	return t;
}

inline void
free_col_chunk (column_chunk_t *c) {
	my_free(c->data);
	my_free(c);
}

inline void
free_table_chunk(table_chunk_t *tc, size_t num_cols) {
	for (size_t j = 0; j < num_cols; j++) {
		column_chunk_t *c = tc->columns[j];
		free_col_chunk(c);
	}
	my_free(tc->columns);
	my_free(tc);
}

void
free_col_table (col_table_t *t)
{
	for(size_t i = 0; i < t->num_chunks; i++) {
		free_table_chunk(t->chunks[i], t->num_cols);
	}
	my_free(t->chunks);
	my_free(t);
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

col_table_t *
create_col_table_like (col_table_t *in) {
	size_t chunk_size = get_chunk_size(in);

	col_table_t * out = NEW(col_table_t);
	MALLOC_CHECK(out, "table");

	out->num_chunks = in->num_chunks;
	out->num_cols = in->num_cols;
	out->num_rows = in->num_chunks * get_chunk_size(in);

	out->chunks = NEWPA(table_chunk_t, out->num_chunks);
	MALLOC_CHECK(out->chunks, "chunks array");

	for(size_t chunk_no = 0; chunk_no < out->num_chunks; chunk_no++) {
		out->chunks[chunk_no] = NEW(table_chunk_t);
		MALLOC_CHECK(out->chunks[chunk_no], "chunk");

		out->chunks[chunk_no]->columns = NEWPA(column_chunk_t, out->num_cols);
		MALLOC_CHECK(out->chunks[chunk_no]->columns, "columns array");

		for (size_t col = 0; col < out->num_cols; col++) {
			out->chunks[chunk_no]->columns[col] = NEW(column_chunk_t);
			MALLOC_CHECK(out->chunks[chunk_no]->columns[col], "column");

			out->chunks[chunk_no]->columns[col]->chunk_size = chunk_size;

			out->chunks[chunk_no]->columns[col]->data = NEWA(val_t, chunk_size);
			MALLOC_CHECK(out->chunks[chunk_no]->columns[col]->data, "column data");
		}
	}

	return out;
}

col_table_t *
copy_col_table (col_table_t *in) {
	col_table_t * out = create_col_table_like(in);
	copy_col_table_noalloc(in, out);
	return out;
}

inline void
copy_col_table_noalloc(col_table_t* in, col_table_t* out) {
	for(size_t chunk_no = 0; chunk_no < out->num_chunks; chunk_no++) {
		copy_table_chunk(*in->chunks[chunk_no], *out->chunks[chunk_no], in->num_cols);
	}
}

inline void
copy_table_chunk(table_chunk_t in_chunk, table_chunk_t out_chunk, size_t num_cols) {
	size_t chunk_size = in_chunk.columns[0]->chunk_size;

	for (size_t col = 0; col < num_cols; col++) {
		memcpy(out_chunk.columns[col]->data,
			   in_chunk.columns[col]->data,
			   chunk_size * sizeof(val_t));
	}
}

table_chunk_t
new_copy_table_chunk(table_chunk_t in_chunk, size_t num_cols) {
	size_t chunk_size = in_chunk.columns[0]->chunk_size;

	table_chunk_t out_chunk;

	out_chunk.columns = NEWPA(column_chunk_t, num_cols);
	MALLOC_NO_RET(out_chunk.columns, "columns array");

	for (size_t col = 0; col < num_cols; col++) {
		out_chunk.columns[col] = NEW(column_chunk_t);
		MALLOC_NO_RET(out_chunk.columns[col], "column");

		out_chunk.columns[col]->chunk_size = chunk_size;

		out_chunk.columns[col]->data = NEWA(val_t, chunk_size);
		MALLOC_NO_RET(out_chunk.columns[col]->data, "column data");

		copy_table_chunk(in_chunk, out_chunk, num_cols);
	}
	return out_chunk;
}


void
print_strided_db(col_table_t* db) {
	size_t chunk_size = get_chunk_size(db);
	size_t num_cols = db->num_cols;
	size_t num_chunks = db->num_chunks;
	size_t chunk_skip = num_chunks / 10;
#define n_offsets 2
	size_t offsets[n_offsets] = {0, chunk_size / 2};

	for(size_t chunk_no = 0; chunk_no < num_chunks; chunk_no += chunk_skip) {
		column_chunk_t** cols = db->chunks[chunk_no]->columns;
		for(size_t i = 0; i < n_offsets; ++i) {
			size_t offset = offsets[i];
			size_t row = chunk_no * chunk_size + offset;
			printf("row %6ld (%4ld, %3ld): ", row, chunk_no, offset);
			for(size_t col = 0; col < num_cols; ++col) {
				printf("%2d ", cols[col]->data[offset]);
			}
			printf("\n");
		}
	}
}

void
print_full_db(col_table_t* db) {
	size_t chunk_size = get_chunk_size(db);
	size_t num_cols = db->num_cols;
	size_t row = 0;

	for(size_t chunk_no = 0; chunk_no < db->num_chunks; ++chunk_no) {
		column_chunk_t** cols = db->chunks[chunk_no]->columns;
		for(size_t offset = 0; offset < chunk_size; ++offset, ++row) {
			printf("row %6ld (%4ld, %3ld): ", row, chunk_no, offset);
			for(size_t col = 0; col < num_cols; ++col) {
				printf("%2d ", cols[col]->data[offset]);
			}
			printf("\n");
		}
	}
}

void print_db(col_table_t* db) {
	if(db->num_rows < 300) {
		print_full_db(db);
	} else {
		print_strided_db(db);
	}
}

void print_chunk(table_chunk_t chunk, size_t chunk_start, size_t chunk_size, size_t num_cols) {
	column_chunk_t** cols = chunk.columns;
	for(size_t offset = 0; offset < chunk_size; ++offset) {
		printf("row %6ld (this, %3ld): ", offset + chunk_start, offset);
		for(size_t col = 0; col < num_cols; ++col) {
			printf("%2d ", cols[col]->data[offset]);
		}
		printf("\n");
	}
}
