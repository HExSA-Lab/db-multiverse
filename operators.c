#ifdef __USER
#include <assert.h>
#include <string.h>
#endif

#ifdef __NAUTILUS__
#include <nautilus/libccompat.h>
#endif

#include "test/common.h"
#include "test/timing.h"
#include "test/operators.h"

// function declarations
col_table_t *projection(col_table_t *t, size_t *pos, size_t num_proj);
col_table_t *selection_const (col_table_t *t, size_t col, val_t val);
col_table_t *basic_rowise_selection_const (col_table_t *t, size_t col, val_t val);
col_table_t *scatter_gather_selection_const (col_table_t *t, size_t col, val_t val);
col_table_t * mergesort(col_table_t *in, size_t col, size_t);
col_table_t * countingsort(col_table_t *in, size_t col, size_t domain_size);
col_table_t * mergecountingsort(col_table_t *in, size_t col, size_t domain_size);
col_table_t *countingmergesort(col_table_t *in, size_t col, size_t domain_size);

// information about operator implementations
op_implementation_info_t impl_infos[] = {
		{
				SELECTION_CONST,
				{ "row", "col", "scattergather", NULL, NULL },
				{ basic_rowise_selection_const, selection_const, scatter_gather_selection_const, NULL, NULL },
				3
		},
		{
				SELECTION_ATT,
				{ NULL, NULL, NULL, NULL, NULL },
				{ NULL, NULL, NULL, NULL, NULL },
				0
		},
		{
				PROJECTION,
				{ "inplace", NULL, NULL, NULL, NULL },
				{ projection, NULL, NULL, NULL, NULL },
				1
		},
		{
				SORT,
				{"mergesort", "countingsort", "mergecountingsort", "countingmergesort", NULL},
				{ mergesort ,  countingsort ,  mergecountingsort ,  countingmergesort , NULL},
				4
				// TODO: look into glibc/Python sort
				// http://sourceware.org/git/?p=glibc.git;a=blob;f=stdlib/qsort.c;h=264a06b8a924a1627b3c0fd507a3e2ca38dbc8a0;hb=HEAD
				// http://sourceware.org/git/?p=glibc.git;a=blob;f=stdlib/msort.c;h=266c2538c07e86d058359d47388fe21cbfdb525a;hb=HEAD
		},
};

op_implementation_t default_impls[] = {
		basic_rowise_selection_const, // SELECTION_CONST
		NULL, // SELECTION_ATT
		projection, // PROJECTION
		countingmergesort,
};

const char * op_names[] = {
		[SELECTION_CONST] = "selection_const",
		[SELECTION_ATT] = "selection_att",
		[PROJECTION] = "projection",
		[SORT] = "sort",
};

// projection only changes the schema
col_table_t *
projection(col_table_t *t, size_t *pos, size_t num_proj)
{
	INFO("PROJECTION on columns ");
	for (size_t i = 0; i < num_proj; i++)
	{
		INFO("%lu%s", pos[i], (i < num_proj - 1) ? ", " : " ");
	}
	INFO("over ");
	print_table_info(t);

		TIMEIT("time: project",
		for(size_t i = 0; i < t->num_chunks; i++)
		{
			table_chunk_t *tc = t->chunks[i];
			column_chunk_t **new_columns = NEWPA(column_chunk_t, num_proj);

			MALLOC_CHECK_NO_MES(new_columns);

			unsigned char *colRetained = NEWA(unsigned char, t->num_cols);

			MALLOC_CHECK_NO_MES(colRetained);

			for(size_t j = 0; j < t->num_cols; j++)
				colRetained[j] = 0;

			for(size_t j = 0; j < num_proj; j++)
			{
				new_columns[j] = tc->columns[pos[j]];
				colRetained[pos[j]] = 1;
			}

			for(size_t j = 0; j < t->num_cols; j++)
			{
				if(colRetained[j] == 0)
				{
					//DEBUG("Droping col %ld\n", j);
					column_chunk_t *c = tc->columns[j];
					free(c->data);
					free(c);
				}
			}

			free(tc->columns);
			tc->columns = new_columns;

			free(colRetained);
		}
		t->num_cols = num_proj;
		);

	INFO(" => output table: ");
	print_table_info(t);

	return t;
}

#define UNROLL_SIZE 1028

col_table_t *
selection_const (col_table_t *t, size_t col, val_t val)
{
	col_table_t *r = NEW(col_table_t);
	column_chunk_t *c_chunk;
	table_chunk_t *t_chunk;
	size_t total_results = 0;
	size_t chunk_size = t->chunks[0]->columns[0]->chunk_size;
	size_t out_chunks = 1;

	INFO("SELECTION[col] on column %lu = %u",  col, val);
	INFO(" over ");
	print_table_info(t);

	r->num_cols = t->num_cols;
	r->num_chunks = 1;
	r->chunks = NEWPA(table_chunk_t, t->num_chunks);

	MALLOC_CHECK_NO_MES(r->chunks);

	t_chunk =  NEW(table_chunk_t);
	MALLOC_CHECK_NO_MES(t_chunk);

	t_chunk->columns = NEWPA(column_chunk_t, t->num_cols);
	MALLOC_CHECK_NO_MES(t_chunk->columns);

	for(size_t i = 0; i < t->num_cols; i++)
	{
		t_chunk->columns[i] = NEW(column_chunk_t);
		MALLOC_CHECK_NO_MES(t_chunk->columns[i]);
		c_chunk = t_chunk->columns[i];
		c_chunk->data = NEWA(val_t, t->chunks[0]->columns[0]->chunk_size);
		MALLOC_CHECK_NO_MES(c_chunk->data);
	}
	r->chunks[out_chunks - 1] = t_chunk;
	size_t out_pos = 0;

	for(size_t i = 0; i < t->num_chunks; i++)
	{
		table_chunk_t *tc = t->chunks[i];
		column_chunk_t *c = tc->columns[col];
		val_t *outdata = NULL, *outstart = NULL;
		val_t *indata = c->data;
		val_t *instart = indata;

		for(size_t j = 0; j < t->num_cols; j++)
		{
			outdata = t_chunk->columns[j]->data + out_pos;
			outstart = outdata;
			size_t count_results = (j == 0);

			while((indata + UNROLL_SIZE < instart + chunk_size) && (outdata + UNROLL_SIZE < outstart + - out_pos + chunk_size))
			{
				// tight loop with compile time constant of iterations
				for(int k = 0; k < UNROLL_SIZE; k++)
				{
					int match = (*indata == val);
					*outdata = *indata;
					indata++;
					outdata += match;
					total_results += match * count_results;
				}
			}
		}

		out_pos = outdata - outstart;

		while(indata < instart + chunk_size)
		{
			if (out_pos == chunk_size)
			{
				t_chunk =  NEW(table_chunk_t);
				MALLOC_CHECK_NO_MES(t_chunk);

				t_chunk->columns = NEWPA(column_chunk_t, t->num_cols);
				MALLOC_CHECK_NO_MES(t_chunk->columns);

				for(size_t j = 0; j < t->num_cols; j++)
				{
					t_chunk->columns[j] = NEW(column_chunk_t);
					MALLOC_CHECK_NO_MES(t_chunk->columns[j]);

					c_chunk = t_chunk->columns[j];
					c_chunk->data = NEWA(val_t, chunk_size);
					MALLOC_CHECK_NO_MES(c_chunk->data);
				}
				r->chunks[r->num_chunks] = t_chunk;
				r->num_chunks++;
				out_pos = 0;
			}

			int match = (*indata == val);
			indata++;

			if (match)
			{
				total_results++;
				for(size_t j = 0; j < t->num_cols; j++)
				{
					outdata = t_chunk->columns[j]->data + out_pos;
					*outdata = *indata;
					outdata += match;
				}
				out_pos++;
			}

		}

	}

	r->num_rows = total_results;

	INFO(" => output table: ");
	print_table_info(r);

	free_col_table(t);

    return r;
}

col_table_t *
basic_rowise_selection_const (col_table_t *t, size_t col, val_t val)
{
	col_table_t *r = NEW(col_table_t);
	column_chunk_t *c_chunk;
	table_chunk_t *t_chunk;
	size_t total_results = 0;
	size_t chunk_size = t->chunks[0]->columns[0]->chunk_size;
	size_t out_chunks = 1;

	INFO("SELECTION[rowwise] on column %lu = %u",  col, val);
	printf(" over ");
	print_table_info(t);

	r->num_cols = t->num_cols;
	r->num_chunks = 1;
	r->chunks = NEWPA(table_chunk_t, t->num_chunks);
	MALLOC_CHECK_NO_MES(r->chunks);

	t_chunk =  NEW(table_chunk_t);
	MALLOC_CHECK_NO_MES(t_chunk);

	t_chunk->columns = NEWPA(column_chunk_t, t->num_cols);
	MALLOC_CHECK_NO_MES(t_chunk->columns);

	for(size_t i = 0; i < t->num_cols; i++)
	{
		t_chunk->columns[i] = NEW(column_chunk_t);
		MALLOC_CHECK_NO_MES(t_chunk->columns[i]);
		c_chunk = t_chunk->columns[i];
		c_chunk->data = NEWA(val_t, t->chunks[0]->columns[0]->chunk_size);
		MALLOC_CHECK_NO_MES(c_chunk->data);
	}
	r->chunks[out_chunks - 1] = t_chunk;
	size_t out_pos = 0;

	TIMEIT("time: select",
		for(size_t i = 0; i < t->num_chunks; i++)
		{
			table_chunk_t *tc = t->chunks[i];
			column_chunk_t *c = tc->columns[col];
			val_t *outdata;
			val_t *indata = c->data;
			val_t *instart = indata;

			while(indata < instart + chunk_size)
			{
				if (out_pos == chunk_size)
				{
					t_chunk =  NEW(table_chunk_t);
					MALLOC_CHECK_NO_MES(t_chunk);

					t_chunk->columns = NEWPA(column_chunk_t, t->num_cols);
					MALLOC_CHECK_NO_MES(t_chunk->columns);

					for(size_t j = 0; j < t->num_cols; j++)
					{
						t_chunk->columns[j] = NEW(column_chunk_t);
						MALLOC_CHECK_NO_MES(t_chunk->columns[j]);

						c_chunk = t_chunk->columns[j];
						c_chunk->data = NEWA(val_t, chunk_size);
						MALLOC_CHECK_NO_MES(c_chunk->data);
					}
					r->chunks[r->num_chunks] = t_chunk;
					r->num_chunks++;
					out_pos = 0;
				}

				uint8_t match = (*indata == val);
				indata++;

				if (match)
				{
					total_results++;
					for(size_t j = 0; j < t->num_cols; j++)
					{
						outdata = t_chunk->columns[j]->data + out_pos;
						*outdata = *indata;
						outdata += match;
					}
					out_pos++;
				}

			}

		}
	);

	r->num_rows = total_results;

	INFO(" => output table: ");
	print_table_info(r);

	free_col_table(t);

    return r;
}

col_table_t *
scatter_gather_selection_const (col_table_t *t, size_t col, val_t val)
{
	table_chunk_t *t_chunk;
	size_t total_results = 0;
	size_t chunk_size = t->chunks[0]->columns[0]->chunk_size;
	size_t out_chunks = 1;
	column_chunk_t **idx;
	column_chunk_t **cidx;

	INFO("SELECTION[scatter-gather] on column %lu = %u",  col, val);
	printf(" over ");
	print_table_info(t);

	// create index columns
	//TODO introduce better data type to represent offsets (currently column chunk is one fixed large integer type)
	TIMEIT("index creation",
		idx = NEWPA(column_chunk_t, t->num_chunks);
		MALLOC_CHECK_NO_MES(idx);
		cidx = NEWPA(column_chunk_t, t->num_chunks);
		MALLOC_CHECK_NO_MES(cidx);
		for(size_t i = 0; i < t->num_chunks; i++)
		{
			idx[i] = create_col_chunk(chunk_size);
			cidx[i] = create_col_chunk(chunk_size);
		}
	);

	// scatter based on condition
	TIMEIT("scatter",
		unsigned int outpos = 0;
		unsigned int outchunk = 0;
		val_t *idx_c = idx[0]->data;
		val_t *cidx_c = cidx[0]->data;

		for(size_t i = 0; i < t->num_chunks; i++)
		{
			table_chunk_t *tc = t->chunks[i];
			column_chunk_t *c = tc->columns[col];
			val_t *indata = c->data;

			for(size_t j = 0; j < chunk_size; j++)
			{
				int match = (*indata++ == val);
				idx_c[outpos] = j;
				cidx_c[outpos] = i;
				outpos += match;

				if (outpos == chunk_size)
				{
					outpos = 0;
					outchunk++;
					idx_c = idx[outchunk]->data;
					cidx_c = cidx[outchunk]->data;
					total_results += chunk_size;
				}
			}
		}
		total_results += outpos;

		// fill remainder with [0,0] to simplify processing
		while(++outpos < chunk_size)
		{
			idx_c[outpos] = 0;
			cidx_c[outpos] = 0;
		}
	);

	// create output table
	out_chunks = total_results / chunk_size + (total_results % chunk_size == 0 ? 0 : 1);

	col_table_t *r = NEW(col_table_t);
	MALLOC_CHECK_NO_MES(r);
	r->num_cols = t->num_cols;
	r->num_rows = total_results;
	r->num_chunks = out_chunks;
	r->chunks = NEWPA(table_chunk_t, out_chunks);
	MALLOC_CHECK_NO_MES(r->chunks);

	TIMEIT("malloc output table",
		for(size_t i = 0; i < out_chunks; i++)
		{
			t_chunk =  NEW(table_chunk_t);
			MALLOC_CHECK_NO_MES(t_chunk);

			t_chunk->columns = NEWPA(column_chunk_t, t->num_cols);
			MALLOC_CHECK_NO_MES(t_chunk->columns);

			for(size_t j = 0; j < t->num_cols; j++)
			{
				t_chunk->columns[j] = create_col_chunk(chunk_size);
			}
			r->chunks[i] = t_chunk;
		}
	);

	// gather based on index column one column at a time
	TIMEIT("gather",
		for(size_t j = 0; j < out_chunks; j++)
		{
			val_t *idx_c = idx[j]->data;
			val_t *cidx_c = cidx[j]->data;

			for(size_t i = 0; i < t->num_cols; i++)
			{
				val_t *outdata = r->chunks[j]->columns[i]->data;
				for(size_t k = 0; k < chunk_size; k++)
				{
					outdata[k] = t->chunks[cidx_c[k]]->columns[i]->data[idx_c[k]];
				}
			}
		}
	);

	TIMEIT("free",
		for(size_t i = 0; i < out_chunks; i++)
		{
			free_col_chunk(idx[i]);
			free_col_chunk(cidx[i]);
		}
		free(idx);
		free(cidx);
	);

	INFO(" => output table: ");
	print_table_info(r);

	free_col_table(t);

    return r;
}

void mergesort_helper(col_table_t *in, size_t start, size_t stop, col_table_t *out, size_t col);

col_table_t *
mergesort(col_table_t *in, size_t col, __attribute__((unused)) size_t domain_size) {
	col_table_t* out = copy_col_table (in);

	TIMEIT("time: sort",
		   mergesort_helper(in, 0, in->num_rows, out, col);
	);

	free_col_table(in);

	return out;
}

void
compute_offset(size_t chunk_size, size_t row,
			   size_t *chunk_no, size_t *chunk_offset) {
	*chunk_no     = row / chunk_size;
	*chunk_offset = row % chunk_size;
}

void
load_row(col_table_t *table, size_t chunk_size, size_t row, size_t column,
		 size_t *chunk_no, size_t *chunk_offset, table_chunk_t *chunk, val_t **chunk_col, val_t *val, bool *done) {

	// chunk_no and chunk_offset are always set such that they point to the row'th row.
	// If chunk is provided, the chunk is set.
	// If col is provided, col and val are set based on column.

	compute_offset(chunk_size, row, chunk_no, chunk_offset);
	if (*chunk_no < table->num_chunks) {
		*chunk = *table->chunks[*chunk_no];
		*chunk_col = chunk->columns[column]->data;
		*val = (*chunk_col)[*chunk_offset];
	} else {
		*done = true;
	}
}

void
load_next_row(col_table_t *table, size_t chunk_size, size_t column,
			  size_t *chunk_no, size_t *chunk_offset, table_chunk_t *chunk, val_t **col, val_t *val) {
	// chunk_no and chunk_offset are always set such that they point to the row'th row.
	// If chunk is provided, the chunk is set (you care about all columns).
	// If col is provided, col and val are set based on column (you care about just one column).
	// In the case that the chunk does not change after incrementing row (i.e. chunk_offset < chunk_size), this probably might be fast-ish.

	bool done = false;

	++*chunk_offset;
	if (*chunk_offset < chunk_size) {
		// this branch is most common by far
		// do nothing. Yay!
	} else {
		// reset chunk offset and move to next chunk
		*chunk_offset = 0;
		++*chunk_no;
		if (*chunk_no == table->num_chunks) {
			done = true;
		} else {
			*chunk = *table->chunks[*chunk_no];
			*col = chunk->columns[column]->data;
		}
	}
	if (!done) {
		// this might hit the cache?
		*val = (*col)[*chunk_offset];
	}
}

// TODO: macro
void
copy_row(table_chunk_t src, size_t src_offset, table_chunk_t dst, size_t dst_offset, size_t num_cols) {
	for(size_t column = 0; column < num_cols; ++column) {
		dst.columns[column]->data[dst_offset] =
		src.columns[column]->data[src_offset];
	}
}

void merge(col_table_t *in, size_t start, size_t mid, size_t stop, col_table_t *out, size_t col);

void
mergesort_helper(col_table_t *in, size_t start, size_t stop, col_table_t *out, size_t col) {
	// sorts in[start:stop] and puts the result in out[start:stop]
	// PRECONDITION: multiset(in[start:stop]) == multiset(out[start:stop])

	if (stop - start >= 2) {
		size_t mid = (start + stop) / 2;

		// recurse
		// Remember out[start:mid] has the same elements as in[start:mid].
		mergesort_helper(out, start, mid , in, col); // in[start:mid] is sorted
		mergesort_helper(out, mid  , stop, in, col); // in[stop :mid] is sorted

		// merge in[start:mid] with in[mid:stop], placing the result in out[start:stop], as desired.
		merge(in, start, mid, stop, out, col);
	}
}

void
merge(col_table_t *in, size_t start, size_t mid, size_t stop, col_table_t *out, size_t col) {
	size_t chunk_size = get_chunk_size(in);
	size_t num_cols = in->num_cols;

	bool run1_empty = false;
	bool run2_empty = false;

	size_t run1_chunk_no;
	size_t run1_chunk_offset;
	table_chunk_t run1_chunk;
	val_t *run1_col;
	val_t run1_val;
	load_row(in, chunk_size, start, col,
			 &run1_chunk_no, &run1_chunk_offset, &run1_chunk, &run1_col, &run1_val, &run1_empty);

	size_t run2_chunk_no;
	size_t run2_chunk_offset;
	table_chunk_t run2_chunk;
	val_t *run2_col;
	val_t run2_val;
	{
		// This is not theoretically necessary, but GCC doesn't know that
		run2_chunk.columns = NULL;
		run2_col = NULL;
		run2_val = 0;
	}
	load_row(in, chunk_size, mid, col,
			 &run2_chunk_no, &run2_chunk_offset, &run2_chunk, &run2_col, &run2_val, &run2_empty);

	size_t start_chunk_no;
	size_t start_chunk_offset;
	compute_offset(chunk_size, start,
				   &start_chunk_no, &start_chunk_offset);

	size_t mid_chunk_no;
	size_t mid_chunk_offset;
	compute_offset(chunk_size, mid,
				   &mid_chunk_no, &mid_chunk_offset);

	size_t stop_chunk_no;
	size_t stop_chunk_offset;
	compute_offset(chunk_size, stop,
				   &stop_chunk_no, &stop_chunk_offset);

	size_t out_chunk_no = start_chunk_no;
	size_t out_chunk_offset = start_chunk_offset;
	for (; out_chunk_no < out->num_chunks; out_chunk_no++) {
		// out_chunk_no < out->num_chunks is just an upper bound for valid records. We often terminate early.
		// but I want to assert we have a valid record before the next statment:
		table_chunk_t out_chunk = *out->chunks[out_chunk_no];

		for (; out_chunk_offset < chunk_size && !(run1_empty && run2_empty); out_chunk_offset++) {
			if (!run1_empty && (run2_empty || run1_val < run2_val)) {
				/* DEBUG("copy %ld (run1) to %ld\n", */
				/* 	  run1_chunk_no * chunk_size + run1_chunk_offset, */
				/* 	   out_chunk_no * chunk_size +  out_chunk_offset); */
				copy_row(run1_chunk, run1_chunk_offset, out_chunk, out_chunk_offset, num_cols);
				load_next_row(in, chunk_size, col,
							  &run1_chunk_no, &run1_chunk_offset, &run1_chunk, &run1_col, &run1_val);
				// update whether or not run1 is empty
				run1_empty = (run1_chunk_no == mid_chunk_no && run1_chunk_offset == mid_chunk_offset);
			} else {
				/* DEBUG("copy %ld (run2) to %ld\n", */
				/* 	  run2_chunk_no * chunk_size + run2_chunk_offset, */
				/* 	   out_chunk_no * chunk_size +  out_chunk_offset); */
				copy_row(run2_chunk, run2_chunk_offset, out_chunk, out_chunk_offset, num_cols);
				load_next_row(in, chunk_size, col,
							  &run2_chunk_no, &run2_chunk_offset, &run2_chunk, &run2_col, &run2_val);
				// update whether or not run2 is empty
				run2_empty = (run2_chunk_no == stop_chunk_no && run2_chunk_offset == stop_chunk_offset);
			}
		}
		out_chunk_offset = 0;
	}
	return;
}

void countingsort_helper(col_table_t *in, size_t start, size_t stop, col_table_t *out, size_t col, size_t domain_size);

col_table_t *
countingsort(col_table_t *in, size_t col, size_t domain_size)
{
	INFO("SORT\n");
	INFO(" => input table:");
	print_table_info(in);

	col_table_t* out = create_col_table_like (in);

	TIMEIT("time: sort",
		   countingsort_helper(in, 0, in->num_rows, out, col, domain_size);
	);

	free_col_table(in);

	return out;
}

void
countingsort_helper(col_table_t *in, size_t start, size_t stop, col_table_t *out, size_t col, size_t domain_size) {
	size_t num_cols = in->num_cols;
	size_t chunk_size = get_chunk_size(in);

	// TOOD: write an initial scan to make this safe and memory efficient
	// CAVEAT: I assume no single domain element contains more than some fraction of the whole array.
	// This helps the array fit in memory
	size_t row_count = MAX((stop - start) / 20, 5);
	// but at least five long (for sorting small arrays)

	// each element of the row will be a pair of size_t's (table_chunk_t chunk, size_t chunk_offset);
	// Note that copying a table_chunk_t is just copying a pointer to the column-data.
	size_t row_size = row_count * (sizeof(table_chunk_t) + sizeof(size_t));

	// one row for every domain-element
	void *array = malloc(domain_size * row_size);
	MALLOC_NO_RET(array, "array");
	//DEBUG("array %p to %p, %ld rows, each of %ld size\n", array, array + domain_size * row_size, row_count, row_size);

	// array_starts holds the first position of each row.
	void **array_starts = malloc(domain_size * sizeof(size_t));
	MALLOC_NO_RET(array_starts, "array_starts");
	void **array_ends = malloc(domain_size * sizeof(size_t));
	MALLOC_NO_RET(array_ends, "array_ends");
	for(size_t i = 0; i < domain_size; ++i) {
		// row_size is measured in bytes, so cast to char**
		array_starts[i] = &((char*) array)[row_size * i];
		array_ends  [i] = &((char*) array)[row_size * i];
	}

	size_t in_chunk_no;
	size_t in_chunk_offset;
	compute_offset(chunk_size, start,
				   &in_chunk_no, &in_chunk_offset);

	size_t stop_chunk_no;
	size_t stop_chunk_offset;
	compute_offset(chunk_size, stop,
				   &stop_chunk_no, &stop_chunk_offset);

	for(; in_chunk_no < in->num_chunks; in_chunk_no++) {
		// out_chunk_no < out->num_chunks is just an upper bound for valid records. We often terminate early.
		// but I want to assert we have a valid record before the next statment:
		table_chunk_t in_chunk = *in->chunks[in_chunk_no];
		val_t *col_data = in->chunks[in_chunk_no]->columns[col]->data;
		//DEBUG("in %ld\n", in_chunk_no);

		for(; in_chunk_offset < chunk_size && !(in_chunk_no == stop_chunk_no && in_chunk_offset == stop_chunk_offset); in_chunk_offset++) {
			val_t domain_elem = col_data[in_chunk_offset];
			assert(domain_elem < domain_size);

			*(table_chunk_t*) array_ends[domain_elem] = in_chunk;
			array_ends[domain_elem] += sizeof(table_chunk_t);
			*(size_t*) array_ends[domain_elem] = in_chunk_offset;
			array_ends[domain_elem] += sizeof(size_t);
		}

		in_chunk_offset = 0;
	}

	val_t cur_domain = 0;

	size_t out_chunk_no;
	size_t out_chunk_offset;

	compute_offset(chunk_size, start,
				   &out_chunk_no, &out_chunk_offset);

	for(; out_chunk_no < out->num_chunks; out_chunk_no++) {
		// out_chunk_no < out->num_chunks is just an upper bound for valid records. We often terminate early.
		// but I want to assert we have a valid record before the next statment:
		table_chunk_t out_chunk = *out->chunks[out_chunk_no];
		//DEBUG("out %ld\n", out_chunk_no);

		for(; out_chunk_offset < chunk_size && !(out_chunk_no == stop_chunk_no && out_chunk_offset == stop_chunk_offset); out_chunk_offset++) {
			// If we have hit the frontier for this domain element, go on to the next one.
			// Since I will run out of array elements at the same time as the out-position equals the stop-position,
			// I shouldn't end up reading past the end of array_end
			while(array_starts[cur_domain] == array_ends[cur_domain]) {
				//DEBUG("domain element %d\n", cur_domain);
				cur_domain++;
			}

			table_chunk_t lookup_chunk = *(table_chunk_t*) array_starts[cur_domain];
			array_starts[cur_domain] += sizeof(table_chunk_t);
			size_t lookup_chunk_offset = *(size_t*) array_starts[cur_domain];
			array_starts[cur_domain] += sizeof(size_t);

			copy_row(lookup_chunk, lookup_chunk_offset, out_chunk, out_chunk_offset, num_cols);
		}

		out_chunk_offset = 0;
	}

	free(array);
	free(array_starts);
	free(array_ends);
}

void
merge_counting_sort_helper(col_table_t *in, size_t start, size_t stop, col_table_t *out, size_t col, size_t domain_size);

col_table_t *
mergecountingsort(col_table_t *in, size_t col, size_t domain_size)
{
	INFO("SORT\n");
	INFO(" => input table:");
	print_table_info(in);

	col_table_t* out = copy_col_table (in);

	TIMEIT("time: sort",
		   merge_counting_sort_helper(in, 0, in->num_rows, out, col, domain_size);
	);

	free_col_table(in);

	return out;
}

const size_t switch_to_other_sort = 10;

void
merge_counting_sort_helper(col_table_t *in, size_t start, size_t stop, col_table_t *out, size_t col, size_t domain_size) {
	// sorts in[start:stop] and puts the result in out[start:stop]
	// PRECONDITION: multiset(in[start:stop]) == multiset(out[start:stop])

	size_t len = stop - start;
	if (len >= switch_to_other_sort) {
		size_t mid = (start + stop) / 2;

		// recurse
		// Remember out[start:mid] has the same elements as in[start:mid].
		merge_counting_sort_helper(out, start, mid , in, col, domain_size); // in[start:mid] is sorted
		merge_counting_sort_helper(out, mid  , stop, in, col, domain_size); // in[stop :mid] is sorted

		// merge in[start:mid] with in[mid:stop], placing the result in out[start:stop], as desired.
		merge(in, start, mid, stop, out, col);

	} else if (len >= 2) {
		countingsort_helper(in, start, stop, out, col, domain_size);
	}
}

void countingsort_intrachunk(table_chunk_t in_chunk, size_t start, size_t stop, table_chunk_t out_chunk, size_t num_cols, size_t col, size_t domain_size);
void merge_intrachunk(table_chunk_t in_chunk, size_t start, size_t mid, size_t stop, table_chunk_t out_chunk, size_t col, size_t num_cols);

col_table_t *
countingmergesort(col_table_t *in, size_t col, size_t domain_size)
{
	INFO("SORT\n");
	INFO(" => input table:");
	print_table_info(in);

	col_table_t *out = create_col_table_like(in);

	size_t chunk_size = get_chunk_size(in);
	size_t sub_chunk = 1024 * 1024;
	size_t num_chunks = in->num_chunks;
	size_t num_cols = in->num_cols;
	size_t num_rows = in->num_rows;

	TIMEIT("time: sort",
		   TIMEIT("time: countingsort",
				  for(size_t chunk_no = 0; chunk_no < in->num_chunks; ++chunk_no) {
					  table_chunk_t in_chunk = *in->chunks[chunk_no];
					  table_chunk_t out_chunk = *out->chunks[chunk_no];
					  for (size_t start = 0; start < chunk_size; start += sub_chunk) {
						  size_t stop = MIN(start + sub_chunk, chunk_size);
						  countingsort_intrachunk(in_chunk, start, stop, out_chunk, num_cols, col, domain_size);
					  }
				  }
				  // make the output of counting-sort the input for merging
				  col_table_t *tmp;
				  SWAP(in, out, tmp);
				  );
		   TIMEIT("time: intra-chunk merging",
				  // in is sorted withinin subchunks of size sub_chunk
				  for(size_t chunk_no = 0; chunk_no < in->num_chunks; ++chunk_no) {
					  table_chunk_t in_chunk = *in->chunks[chunk_no];
					  table_chunk_t out_chunk = *out->chunks[chunk_no];
					  for(size_t width = sub_chunk; width < chunk_size; width *= 2) {
						  // in is sorted within subchunks of size width
						  for(size_t start = 0; start < chunk_size; start += 2 * width) {
							  size_t mid = MIN(start + width, chunk_size);
							  size_t stop = MIN(start + 2 * width, chunk_size);
							  merge_intrachunk(in_chunk, start, mid, stop, out_chunk, col, num_cols);
						  }
						  // out is sorted wihtin subchunks of size 2 * width
						  col_table_t *tmp;
						  SWAP(in, out, tmp);
						  // in is sorted within subchunks of size 2 * width
					  }
				  }
				  // in is sorted withinin chunks
				  );
		   TIMEIT("time: inter-chunk merging",
				  for(size_t width = chunk_size; width < num_chunks * chunk_size; width *= 2) {
					  // in is sorted into runs of size width
					  for(size_t start = 0; start < num_rows; start += 2 * width) {
						  size_t mid = MIN(start + width, num_rows);
						  size_t stop = MIN(start + 2 * width, num_rows);
						  //DEBUG("merge in[%ld:%ld], in[%ld:%ld] -> out[%ld:%ld]\n", start, mid, mid, stop, start, stop);
						  merge(in, start, mid, stop, out, col);
						  // 2 runs in in merge into 1 run in out
					  }
					  // out is sorted into runs of size 2 * width
					  col_table_t *tmp;
					  SWAP(in, out, tmp);
					  // in is sorted into runs of size 2 * width
				  }
				  );
	);

	//in is sorted
	col_table_t *tmp;
	SWAP(in, out, tmp);

	// there is no telling if this is the original 'in' because swapping happens
	// just promise to use this like,
	//
	//     t = countingmergesort(t, ...)
	//     // do stuff with t
	//
	// and not like
	//
	//    countingmergesort(t, ...)
	//     // do stuff with possibly invalid t
	//
	// so that the original pointer gets overwritten with whatever gets returned
	free_col_table(in);

	return out;
}

void countingsort_intrachunk(table_chunk_t in_chunk, size_t start, size_t stop, table_chunk_t out_chunk, size_t num_cols, size_t col, size_t domain_size) {
	size_t row_count = stop - start;
	if (row_count > 1) {
		// one row for every domain-element
		/* table_chunk_t *chunk_array = NEWA(size_t, table_chunk_t); */
		/* MALLOC_NO_RET(chunk_array, "chunk_array"); */
		size_t *offset_array = NEWA(size_t, row_count * domain_size);
		MALLOC_NO_RET(offset_array, "offset_array");

		//DEBUG("array %p to %p, %ld rows, %ld cols, of size %ld bytes (%ld total)\n",
		//array, array + domain_size * row_size, domain_size, row_count, sizeof(size_t), domain_size * row_size);

		// array_starts holds the first position of each row.
		size_t **array_starts = NEWA(size_t*, domain_size);
		size_t **array_ends   = NEWA(size_t*, domain_size);
		for(size_t i = 0; i < domain_size; ++i) {
			// row_size is measured in bytes, so cast to char**
			array_starts[i] = &offset_array[row_count * i];
			array_ends  [i] = &offset_array[row_count * i];
			//DEBUG("domain elem %ld starts at   %p\n", i, array_starts[i]);
		}

		val_t *col_data = in_chunk.columns[col]->data;
		// Filling the domain-element bucket with chunk_offsets
		for(size_t chunk_offset = start; chunk_offset < stop; chunk_offset++) {
			val_t domain_elem = col_data[chunk_offset];
			*array_ends[domain_elem] = chunk_offset;
			array_ends[domain_elem] += sizeof(size_t*);
		}

		// Draining the domain-element buckets into output chunk
		// TODO: use one pointer instead of one for every row for array_start
		// TODO: loop over domain instead
		val_t cur_domain = 0;
		for(size_t out_chunk_offset = start; out_chunk_offset < stop; out_chunk_offset++) {

			// Skip to domain bucket which is non-empty
			while(array_starts[cur_domain] == array_ends[cur_domain]) {
				cur_domain++;
				//DEBUG("domain element %d\n", cur_domain);
			}

			size_t in_chunk_offset = *array_starts[cur_domain];
			array_starts[cur_domain] += sizeof(size_t);

			copy_row(in_chunk, in_chunk_offset, out_chunk, out_chunk_offset, num_cols);
		}

		free(offset_array);
		free(array_starts);
		free(array_ends);
	}
}

void
merge_intrachunk(table_chunk_t in_chunk, size_t start, size_t mid, size_t stop, table_chunk_t out_chunk, size_t col, size_t num_cols) {
	// TODO: indirect sort
	bool run1_empty = false;
	bool run2_empty = false;
	val_t *data = in_chunk.columns[col]->data;
	size_t run1 = start;
	size_t run2 = mid;
	val_t run1_val = data[run1];
	val_t run2_val = data[run2];
	for(size_t out_chunk_offset = start; out_chunk_offset < stop; out_chunk_offset++) {
		if (!run1_empty && (run2_empty || run1_val < run2_val)) {
			copy_row(in_chunk, run1, out_chunk, out_chunk_offset, num_cols);
			run1++;
			run1_val = data[run1];
			run1_empty = (run1 == mid);
		} else {
			copy_row(in_chunk, run2, out_chunk, out_chunk_offset, num_cols);
			run2++;
			run2_val = data[run2];
			run2_empty = (run2 == stop);
		}
	}
}

size_t* domain_count(col_table_t *in, size_t col, size_t domain_size) {
	size_t *domain_counts = malloc(domain_size * sizeof(size_t));
	for(size_t domain_elem = 0; domain_elem < domain_size; ++domain_elem) {
		domain_counts[domain_elem] = 0;
	}

	for(size_t chunk_no = 0; chunk_no < in->num_chunks; ++chunk_no) {
		val_t *data = in->chunks[chunk_no]->columns[col]->data;
		for(size_t chunk_offset = 0; chunk_offset < get_chunk_size(in); ++chunk_offset) {
			assert(data[chunk_offset] < domain_size);
			domain_counts[data[chunk_offset]]++;
		}
	}
	return domain_counts;
}

bool
check_sorted(col_table_t *result, size_t col, size_t domain_size, col_table_t *copy) {
	val_t last = 0;
	size_t chunk_size = get_chunk_size(result);
	for(size_t chunk_no = 0; chunk_no < result->num_chunks; ++chunk_no) {
		val_t *data = result->chunks[chunk_no]->columns[col]->data;
		for(size_t chunk_offset = 0; chunk_offset < chunk_size; ++chunk_offset) {
			if(data[chunk_offset] < last) {
				ERROR("Out of order");
				return false;
			}
			last = data[chunk_offset];
		}
	}

	size_t* count1 = domain_count(result, col, domain_size);
	size_t* count2 = domain_count(copy, col, domain_size);
	bool same = 0 == memcmp(count1, count2, domain_size * sizeof(size_t));
	if (!same) {
		ERROR("Non-bijective");
	}

	free(count1);
	free(count2);

	return same;
}
