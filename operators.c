#include <stdbool.h>
#include "common.h"
#include "timing.h"
#include "operators.h"

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
				{"mergesort", NULL, NULL, NULL, NULL},
				{mergesort  , NULL, NULL, NULL, NULL},
				1
		},
};

op_implementation_t default_impls[] = {
		basic_rowise_selection_const, // SELECTION_CONST
		NULL, // SELECTION_ATT
		projection, // PROJECTION
		mergesort,
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
		printf("%lu%s", pos[i], (i < num_proj - 1) ? ", " : " ");
	}
	printf("over ");
	print_table_info(t);

	TIMEIT("main loop",
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
					column_chunk_t *c = tc->columns[j];
					free(c->data);
					free(c);
				}
			}

			free(tc->columns);
			tc->columns = new_columns;
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

	TIMEIT("main loop",
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

col_table_t *
mergesort(col_table_t *in, size_t col)
{
	INFO("SORT\n");
	INFO(" => input table:");
	print_table_info(in);

 	col_table_t * out = NEW(col_table_t);
	out->num_chunks = in->num_chunks;
	out->num_cols = in->num_cols;
	out->num_rows = in->num_chunks * get_chunk_size(in);
	out->chunks = NEWPA(table_chunk_t, out->num_chunks);
	MALLOC_CHECK_NO_MES(out->chunks);

	TIMEIT("create col table",
		for(size_t i = 0; i < out->num_chunks; i++)
		{
			table_chunk_t *tc = NEW(table_chunk_t);
			MALLOC_CHECK(tc, "table chunks");

			out->chunks[i] = tc;
			tc->columns = NEWPA(column_chunk_t, out->num_cols);
			MALLOC_CHECK_NO_MES(tc->columns);

			for (size_t j = 0; j < out->num_cols; j++)
			{
				column_chunk_t *c = NEW(column_chunk_t);
				MALLOC_CHECK(c, "column chunks");

				tc->columns[j] = c;
				c->chunk_size = get_chunk_size(in);
				c->data = NEWA(val_t, c->chunk_size);
				MALLOC_CHECK(c->data, "chunk data");
			}
		}
	);

	TIMEIT("actual sort",
		   mergesort_helper(in, 0, in->num_rows, out, col);
	);

	free_col_table(in);

	return out;
}

void
load_row(col_table_t *table, size_t chunk_size, size_t row, size_t column,
		 size_t *chunk_no, size_t *chunk_offset, table_chunk_t *chunk, val_t *val) {
	// chunk and val are optional, but they must both be present or absent together.

	*chunk_no     = row / chunk_size;
	*chunk_offset = row % chunk_size;
	if (chunk) {
		*chunk = *table->chunks[*chunk_no];
	}
	if (val) {
		*val = chunk->columns[column]->data[*chunk_offset];
	}
}

void
load_next_row(col_table_t *table, size_t chunk_size, size_t column,
			  size_t *chunk_no, size_t *chunk_offset, table_chunk_t *chunk, val_t *val, bool *done) {
	// loads the chunk containing the next row
	// chunk and val are optional, but they must both be present or absent together.

	++*chunk_offset;
	if (*chunk_offset < chunk_size) {
		// do nothing. Yay!
	} else {
		// reset chunk offset and move to next chunk
		*chunk_offset = 0;
		++*chunk_no;
		if (*chunk_no == table->num_chunks) {
			*done = true;
		} else {
			if (chunk) {
				*chunk = *table->chunks[*chunk_no];
			}
		}
	}
	if (val && !*done) {
		// this might hit the cache now? or not because of the column indirection
		*val = chunk->columns[column]->data[*chunk_offset];
	}
}

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
	// TODO: combine with a different sorting algorithm for small sizes (size of about a cachline or less)
	// TODO: implement radix sort
	// TODO: look into glibc/Python sort
	// TODO: copy_row and get_cell could take a pointer to the column_chunk
	if (stop - start >= 2) {
		size_t mid = (start + stop) / 2;

		// recurse
		mergesort_helper(in, start, mid , out, col);
		mergesort_helper(in, mid  , stop, out, col);

		// merge in[start:mid] and in[mid:stop] into out
		// I will call in[start:mid] run1
		// and in[mid:stop] run2
		merge(in, start, mid, stop, out, col);
	}
}

void
merge(col_table_t *in, size_t start, size_t mid, size_t stop, col_table_t *out, size_t col) {
	size_t in_chunk_size = get_chunk_size(in);
	size_t out_chunk_size = get_chunk_size(out);
	size_t num_cols = in->num_cols;

	size_t run1_chunk_no;
	size_t run1_chunk_offset;
	table_chunk_t run1_chunk;
	val_t run1_val;
	load_row(in, in_chunk_size, start, col,
			 &run1_chunk_no, &run1_chunk_offset, &run1_chunk, &run1_val);

	size_t run2_chunk_no;
	size_t run2_chunk_offset;
	table_chunk_t run2_chunk;
	val_t run2_val;
	load_row(in, in_chunk_size, start, col,
			 &run2_chunk_no, &run2_chunk_offset, &run2_chunk, &run2_val);

	size_t start_chunk_no;
	size_t start_chunk_offset;
	load_row(in, in_chunk_size, start, col,
			 &start_chunk_no, &start_chunk_offset, NULL, NULL);

	size_t mid_chunk_no;
	size_t mid_chunk_offset;
	load_row(in, in_chunk_size, mid, col,
			 &mid_chunk_no, &mid_chunk_offset, NULL, NULL);

	size_t stop_chunk_no;
	size_t stop_chunk_offset;
	load_row(in, in_chunk_size, stop, col,
			 &stop_chunk_no, &stop_chunk_offset, NULL, NULL);

	bool run1_empty = false;
	bool run2_empty = false;

	size_t out_chunk_no = start_chunk_no;
	size_t out_chunk_offset = start_chunk_offset;
	for (; out_chunk_no < out->num_chunks; out_chunk_no++) {
		// out_chunk_no < out->num_chunks is just an upper bound for valid records. We often terminate early.
		// but I want to assert we have a valid record before the next statment:
		table_chunk_t out_chunk = *out->chunks[out_chunk_no];

		for (; out_chunk_offset < out_chunk_size; out_chunk_offset++) {
			// iterate over the whole chunk

			if (run1_empty && run2_empty) {
				// but stop if we get out of data (most common path)
				goto done;
			}

			if (!run1_empty && (run2_empty || run1_val < run2_val)) {
				copy_row(run1_chunk, run1_chunk_offset, out_chunk, out_chunk_offset, num_cols);
				load_next_row(in, in_chunk_size, col,
							  &run1_chunk_no, &run1_chunk_offset, &run1_chunk, &run1_val, &run1_empty);
				// update whether or not run1 is empty
				run1_empty = (run1_chunk_no == mid_chunk_no && run1_chunk_offset == mid_chunk_offset);
			} else {
				copy_row(run2_chunk, run2_chunk_offset, out_chunk, out_chunk_offset, num_cols);
				load_next_row(in, in_chunk_size, col,
							  &run2_chunk_no, &run2_chunk_offset, &run2_chunk, &run2_val, &run2_empty);
				// update whether or not run2 is empty
				run2_empty = (run2_chunk_no == stop_chunk_no && run2_chunk_offset == stop_chunk_offset);
			}
		}
		out_chunk_offset = 0;
	}
 done: return;
}
