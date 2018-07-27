/* #define DEBUG_MERGE 0 */
/* #define DEBUG_MERGE_CHUNK 127 */
/* #define DEBUG_MERGE_OFFSET_MIN 54 */
/* #define DEBUG_MERGE_OFFSET_MAX 57 */

#ifdef __NAUTILUS__
	#include <nautilus/libccompat.h>
#else
	#include <assert.h>
	#include <string.h>
#endif

#include "database/my_malloc.h"
#include "database/common.h"
#include "database/operators.h"
#include "database/bitvec.h"

// function declarations
col_table_t *projection(col_table_t *t, size_t *pos, size_t num_proj);
col_table_t *selection_const (col_table_t *t, size_t col, val_t val);
col_table_t *basic_rowise_selection_const (col_table_t *t, size_t col, val_t val);
col_table_t *scatter_gather_selection_const (col_table_t *t, size_t col, val_t val);
/* col_table_t * mergesort(col_table_t *in, size_t col, size_t); */
/* col_table_t * countingsort(col_table_t *in, size_t col, size_t domain_size); */
/* col_table_t * mergecountingsort(col_table_t *in, size_t col, size_t domain_size); */
col_table_t *countingmergesort(col_table_t *in, size_t col, size_t domain_size);
bool check_sorted_helper(col_table_t *result, size_t start_row, size_t stop_row, size_t sort_col);
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
				{ NULL ,  NULL ,  NULL ,  countingmergesort , NULL},
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
projection(col_table_t *t, size_t *pos, size_t num_proj) {
	for(size_t i = 0; i < t->num_chunks; i++) {
		table_chunk_t *tc = t->chunks[i];
		column_chunk_t **new_columns = NEWPA(column_chunk_t, num_proj);

		MALLOC_CHECK_NO_MES(new_columns);

		unsigned char *colRetained = NEWA(unsigned char, t->num_cols);

		MALLOC_CHECK_NO_MES(colRetained);

		for(size_t j = 0; j < t->num_cols; j++)
			colRetained[j] = 0;

		for(size_t j = 0; j < num_proj; j++) {
			new_columns[j] = tc->columns[pos[j]];
			colRetained[pos[j]] = 1;
		}

		for(size_t j = 0; j < t->num_cols; j++) {
			if(colRetained[j] == 0) {
				//DEBUG("Droping col %ld\n", j);
				column_chunk_t *c = tc->columns[j];
				my_free(c->data);
				my_free(c);
			}
		}

		my_free(tc->columns);
		tc->columns = new_columns;

		my_free(colRetained);
	}
	t->num_cols = num_proj;

	return t;
}

#define UNROLL_SIZE 1028

col_table_t *
selection_const (col_table_t *t, size_t col, val_t val) {
	col_table_t *r = NEW(col_table_t);
	column_chunk_t *c_chunk;
	table_chunk_t *t_chunk;
	size_t total_results = 0;
	size_t chunk_size = t->chunks[0]->columns[0]->chunk_size;
	size_t out_chunks = 1;

	r->num_cols = t->num_cols;
	r->num_chunks = 1;
	r->chunks = NEWPA(table_chunk_t, t->num_chunks);

	MALLOC_CHECK_NO_MES(r->chunks);

	t_chunk =  NEW(table_chunk_t);
	MALLOC_CHECK_NO_MES(t_chunk);

	t_chunk->columns = NEWPA(column_chunk_t, t->num_cols);
	MALLOC_CHECK_NO_MES(t_chunk->columns);

	for(size_t i = 0; i < t->num_cols; i++) {
		t_chunk->columns[i] = NEW(column_chunk_t);
		MALLOC_CHECK_NO_MES(t_chunk->columns[i]);
		c_chunk = t_chunk->columns[i];
		c_chunk->data = NEWA(val_t, t->chunks[0]->columns[0]->chunk_size);
		MALLOC_CHECK_NO_MES(c_chunk->data);
	}
	r->chunks[out_chunks - 1] = t_chunk;
	size_t out_pos = 0;

	for(size_t i = 0; i < t->num_chunks; i++) {
		table_chunk_t *tc = t->chunks[i];
		column_chunk_t *c = tc->columns[col];
		val_t *outdata = NULL, *outstart = NULL;
		val_t *indata = c->data;
		val_t *instart = indata;

		for(size_t j = 0; j < t->num_cols; j++) {
			outdata = t_chunk->columns[j]->data + out_pos;
			outstart = outdata;
			size_t count_results = (j == 0);

			while((indata + UNROLL_SIZE < instart + chunk_size) && (outdata + UNROLL_SIZE < outstart + - out_pos + chunk_size)) {
				// tight loop with compile time constant of iterations
				for(int k = 0; k < UNROLL_SIZE; k++) {
					int match = (*indata == val);
					*outdata = *indata;
					indata++;
					outdata += match;
					total_results += match * count_results;
				}
			}
		}

		out_pos = outdata - outstart;

		while(indata < instart + chunk_size) {
			if (out_pos == chunk_size) {
				t_chunk =  NEW(table_chunk_t);
				MALLOC_CHECK_NO_MES(t_chunk);

				t_chunk->columns = NEWPA(column_chunk_t, t->num_cols);
				MALLOC_CHECK_NO_MES(t_chunk->columns);

				for(size_t j = 0; j < t->num_cols; j++) {
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

			if (match) {
				total_results++;
				for(size_t j = 0; j < t->num_cols; j++) {
					outdata = t_chunk->columns[j]->data + out_pos;
					*outdata = *indata;
					outdata += match;
				}
				out_pos++;
			}

		}

	}

	r->num_rows = total_results;

	free_col_table(t);

    return r;
}

col_table_t *
basic_rowise_selection_const (col_table_t *t, size_t col, val_t val) {
	col_table_t *r = NEW(col_table_t);
	column_chunk_t *c_chunk;
	table_chunk_t *t_chunk;
	size_t total_results = 0;
	size_t chunk_size = t->chunks[0]->columns[0]->chunk_size;
	size_t out_chunks = 1;

	r->num_cols = t->num_cols;
	r->num_chunks = 1;
	r->chunks = NEWPA(table_chunk_t, t->num_chunks);
	MALLOC_CHECK_NO_MES(r->chunks);

	t_chunk =  NEW(table_chunk_t);
	MALLOC_CHECK_NO_MES(t_chunk);

	t_chunk->columns = NEWPA(column_chunk_t, t->num_cols);
	MALLOC_CHECK_NO_MES(t_chunk->columns);

	for(size_t i = 0; i < t->num_cols; i++) {
		t_chunk->columns[i] = NEW(column_chunk_t);
		MALLOC_CHECK_NO_MES(t_chunk->columns[i]);
		c_chunk = t_chunk->columns[i];
		c_chunk->data = NEWA(val_t, t->chunks[0]->columns[0]->chunk_size);
		MALLOC_CHECK_NO_MES(c_chunk->data);
	}
	r->chunks[out_chunks - 1] = t_chunk;
	size_t out_pos = 0;

	for(size_t i = 0; i < t->num_chunks; i++) {
		table_chunk_t *tc = t->chunks[i];
		column_chunk_t *c = tc->columns[col];
		val_t *outdata;
		val_t *indata = c->data;
		val_t *instart = indata;

		while(indata < instart + chunk_size) {
			if (out_pos == chunk_size) {
				t_chunk =  NEW(table_chunk_t);
				MALLOC_CHECK_NO_MES(t_chunk);

				t_chunk->columns = NEWPA(column_chunk_t, t->num_cols);
				MALLOC_CHECK_NO_MES(t_chunk->columns);

				for(size_t j = 0; j < t->num_cols; j++) {
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

			if(match) {
				total_results++;
				for(size_t j = 0; j < t->num_cols; j++) {
					outdata = t_chunk->columns[j]->data + out_pos;
					*outdata = *indata;
					outdata += match;
				}
				out_pos++;
			}

		}

	}

	r->num_rows = total_results;

	free_col_table(t);

    return r;
}

col_table_t *
scatter_gather_selection_const (col_table_t *t, size_t col, val_t val) {
	table_chunk_t *t_chunk;
	size_t total_results = 0;
	size_t chunk_size = t->chunks[0]->columns[0]->chunk_size;
	size_t out_chunks = 1;
	column_chunk_t **idx;
	column_chunk_t **cidx;

	// create index columns
	//TODO introduce better data type to represent offsets (currently column chunk is one fixed large integer type)
	idx = NEWPA(column_chunk_t, t->num_chunks);
	MALLOC_CHECK_NO_MES(idx);
	cidx = NEWPA(column_chunk_t, t->num_chunks);
	MALLOC_CHECK_NO_MES(cidx);
	for(size_t i = 0; i < t->num_chunks; i++) {
		idx[i] = create_col_chunk(chunk_size);
		cidx[i] = create_col_chunk(chunk_size);
	}

	// scatter based on condition
	unsigned int outpos = 0;
	unsigned int outchunk = 0;
	val_t *idx_c = idx[0]->data;
	val_t *cidx_c = cidx[0]->data;

	for(size_t i = 0; i < t->num_chunks; i++) {
		table_chunk_t *tc = t->chunks[i];
		column_chunk_t *c = tc->columns[col];
		val_t *indata = c->data;

		for(size_t j = 0; j < chunk_size; j++) {
			int match = (*indata++ == val);
			idx_c[outpos] = j;
			cidx_c[outpos] = i;
			outpos += match;

			if (outpos == chunk_size) {
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
	while(++outpos < chunk_size) {
		idx_c[outpos] = 0;
		cidx_c[outpos] = 0;
	}

	// create output table
	out_chunks = total_results / chunk_size + (total_results % chunk_size == 0 ? 0 : 1);

	col_table_t *r = NEW(col_table_t);
	MALLOC_CHECK_NO_MES(r);
	r->num_cols = t->num_cols;
	r->num_rows = total_results;
	r->num_chunks = out_chunks;
	r->chunks = NEWPA(table_chunk_t, out_chunks);
	MALLOC_CHECK_NO_MES(r->chunks);

	for(size_t i = 0; i < out_chunks; i++) {
		t_chunk =  NEW(table_chunk_t);
		MALLOC_CHECK_NO_MES(t_chunk);

		t_chunk->columns = NEWPA(column_chunk_t, t->num_cols);
		MALLOC_CHECK_NO_MES(t_chunk->columns);

		for(size_t j = 0; j < t->num_cols; j++) {
			t_chunk->columns[j] = create_col_chunk(chunk_size);
		}
		r->chunks[i] = t_chunk;
	}

	// gather based on index column one column at a time
	for(size_t j = 0; j < out_chunks; j++) {
		val_t *idx_c = idx[j]->data;
		val_t *cidx_c = cidx[j]->data;

		for(size_t i = 0; i < t->num_cols; i++) {
			val_t *outdata = r->chunks[j]->columns[i]->data;
			for(size_t k = 0; k < chunk_size; k++) {
				outdata[k] = t->chunks[cidx_c[k]]->columns[i]->data[idx_c[k]];
			}
		}
	}

	for(size_t i = 0; i < out_chunks; i++) {
		free_col_chunk(idx[i]);
		free_col_chunk(cidx[i]);
	}
	my_free(idx);
	my_free(cidx);

	free_col_table(t);

    return r;
}

static inline void
compute_offset(size_t chunk_size, size_t row,
			   size_t *chunk_no, size_t *chunk_offset) {
	*chunk_no     = row / chunk_size;
	*chunk_offset = row % chunk_size;
}

static inline void
load_row(col_table_t *table, size_t chunk_size, size_t row, size_t column,
		 size_t *chunk_no, size_t *chunk_offset, table_chunk_t *chunk, val_t **chunk_col, val_t *val) {

	// chunk_no and chunk_offset are always set such that they point to the row'th row.
	// If chunk is provided, the chunk is set.
	// If col is provided, col and val are set based on column.

	compute_offset(chunk_size, row, chunk_no, chunk_offset);
	if (*chunk_no < table->num_chunks) {
		*chunk = *table->chunks[*chunk_no];
		*chunk_col = chunk->columns[column]->data;
		*val = (*chunk_col)[*chunk_offset];
	}
}

static inline void
load_next_row(col_table_t *table, size_t chunk_size, size_t column,
			  size_t *chunk_no, size_t *chunk_offset, table_chunk_t *chunk, val_t **col, val_t *val) {
	++*chunk_offset;
	if (__builtin_expect(*chunk_offset < chunk_size, 1)) {
		++*col;
		*val = **col;
	} else {
		// reset chunk offset and move to next chunk
		*chunk_offset = 0;
		++*chunk_no;
		if (__builtin_expect(*chunk_no < table->num_chunks, 1)) {
			*chunk = *table->chunks[*chunk_no];
			*col = chunk->columns[column]->data;
			*val = **col;
		}
	}
}

static inline void
copy_row(table_chunk_t src, size_t src_offset, table_chunk_t dst, size_t dst_offset, size_t num_cols) {
	for(size_t column = 0; column < num_cols; ++column) {
		dst.columns[column]->data[dst_offset] =
		src.columns[column]->data[src_offset];
	}
}

void
merge(col_table_t *in, size_t start, size_t mid, size_t stop, col_table_t *out, size_t sort_col) {
	#ifdef DEBUG_MERGE
	print_db(in);
	#endif

	size_t chunk_size = get_chunk_size(in);
	size_t num_cols = in->num_cols;

	size_t run_chunk_no[2];
	size_t run_chunk_offset[2];
	table_chunk_t run_chunk[2];
	val_t *run_col[2];
	val_t run_val[2];
	load_row(in, chunk_size, start, sort_col,
	         &run_chunk_no[0], &run_chunk_offset[0], &run_chunk[0], &run_col[0], &run_val[0]);
	load_row(in, chunk_size, mid, sort_col,
	         &run_chunk_no[1], &run_chunk_offset[1], &run_chunk[1], &run_col[1], &run_val[1]);
	//assert(run_chunk_offset[0] == 0);
	//assert(run_chunk_offset[1] == 0);

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

	size_t last_run_chunk_no[2] = {
		mid_chunk_no,
		stop_chunk_no,
	};
	size_t last_run_chunk_offset[2] = {
		mid_chunk_offset,
		stop_chunk_offset,
	};

	size_t first_out_chunk_offset = start_chunk_offset;
	bit_vec_t bit_chunk;
	bv_init(&bit_chunk, chunk_size);
	uint8_t which_not_empty = 3;

	for (size_t out_chunk_no = start_chunk_no; out_chunk_no < out->num_chunks; out_chunk_no++) {
		// out_chunk_no < out->num_chunks is just an upper bound for valid records. We often terminate early.
		// but I want to assert we have a valid record before the next statment:
		table_chunk_t out_chunk = *out->chunks[out_chunk_no];

		// init bit-vector to 0 for the output chunk
		bv_reset(&bit_chunk);
		bit_vec_iter_t bit_chunk_iter;

		// save initial run1, run2, and out positions
		size_t first_run_chunk_no[2];
		size_t first_run_chunk_offset[2];
		for(uint8_t i = 0; i < 2; ++i) {
			first_run_chunk_no[i] = run_chunk_no[i];
			first_run_chunk_offset[i] = run_chunk_offset[i];
		}

		// set bit-vector based on which is bigger
		bv_iter_init(&bit_chunk_iter, &bit_chunk);
		bv_iter_skip(&bit_chunk_iter, first_out_chunk_offset);

		// only if both runs still have stuff left to go
		if(which_not_empty == 3) {
			#ifdef DEBUG_MERGE
				size_t out_chunk_offset = 0;
			#endif
			for (; bv_iter_has_next(&bit_chunk_iter); bv_iter_next(&bit_chunk_iter)) {

				bit_t src_bit = run_val[0] > run_val[1];

				#ifdef DEBUG_MERGE
				if (out_chunk_no == DEBUG_MERGE_CHUNK) {
					printf("%lu\n", out_chunk_offset);
				}
				if (out_chunk_no == DEBUG_MERGE_CHUNK &&
				    DEBUG_MERGE_OFFSET_MIN <= out_chunk_offset &&
				    out_chunk_offset <= DEBUG_MERGE_OFFSET_MAX) {
					printf("out.chunks[%lu].offset[%lu] = %u = run[%u].chunks[%lu].offset[%lu] = max(run0.chunk[%lu].offset[%lu], run1.chunk[%lu].offset[%lu]) (col %lu)\n",
						   out_chunk_no, out_chunk_offset,
						   run_val[src_bit],
						   src_bit, run_chunk_no[src_bit], run_chunk_offset[src_bit],
						   run_chunk_no[0], run_chunk_offset[0],
						   run_chunk_no[1], run_chunk_offset[1],
						   sort_col);
				}
				++out_chunk_offset;
				#endif

				// marking bv_chunk_iter to which src had the lower value
				bv_iter_set(&bit_chunk_iter, src_bit);

				// and advancing our pointer in the run which had the lower value
				load_next_row(in, chunk_size, sort_col,
				              &run_chunk_no[src_bit], &run_chunk_offset[src_bit],
				              &run_chunk[src_bit], &run_col[src_bit], &run_val[src_bit]
				);
	
				// and checking to see if the run which had the lower valu eis out
				if(__builtin_expect(run_chunk_no    [src_bit] == last_run_chunk_no    [src_bit] &&
				                    run_chunk_offset[src_bit] == last_run_chunk_offset[src_bit], 0)) {
					#ifdef DEBUG_MERGE
						printf("run%d done\n", src_bit);
					#endif
					bv_iter_next(&bit_chunk_iter);
					which_not_empty = !src_bit;
					break;
				}
			}
		}

		// set rest of in bit-vector
		if(which_not_empty != 3) {
			#ifdef DEBUG_MERGE
			if(out_chunk_no == DEBUG_MERGE_CHUNK) {
				printf("Setting last %lu to %d\n", bit_chunk_iter.n_bits, which_not_empty);
			}
			#endif
			bv_iter_set_rest(&bit_chunk_iter, which_not_empty);
		}
		#ifdef DEBUG_MERGE
			if(out_chunk_no == DEBUG_MERGE_CHUNK) {
				bv_print(&bit_chunk);
			}
		#endif

		// merge run[0] and run[1] into out based on bit-vector
		for(size_t this_col = 0; this_col < num_cols; ++this_col) {
			for(uint8_t i = 0; i < 2; ++i) {
				if( first_run_chunk_no[i] <  last_run_chunk_no[i] ||
				   (first_run_chunk_no[i] == last_run_chunk_no[i] && first_run_chunk_offset[i] < last_run_chunk_offset[i])) {
					// if first_run_____[i] is a valid (less than last_run____[*])
					run_chunk_no[i] = first_run_chunk_no[i];
					run_chunk_offset[i] = first_run_chunk_offset[i];
					run_chunk[i] = *in->chunks[run_chunk_no[i]];
					run_col[i] = run_chunk[i].columns[this_col]->data + run_chunk_offset[i];
					run_val[i] = *run_col[i];
				}
			}

			bv_iter_init(&bit_chunk_iter, &bit_chunk);
			val_t* out_chunk_col = &out_chunk.columns[this_col]->data[first_out_chunk_offset];

			#ifdef DEBUG_MERGE
			size_t out_chunk_offset = 0;
			#endif
			for(    ;
			        __builtin_expect(bv_iter_has_next(&bit_chunk_iter), 1);
			        ++out_chunk_col, bv_iter_next(&bit_chunk_iter)) {

				bit_t src_bit = bv_iter_get(&bit_chunk_iter);
				*out_chunk_col = run_val[src_bit];

				#ifdef DEBUG_MERGE
				if(this_col == sort_col &&
				   out_chunk_no == DEBUG_MERGE_CHUNK &&
				   DEBUG_MERGE_OFFSET_MIN <= out_chunk_offset &&
				   out_chunk_offset <= DEBUG_MERGE_OFFSET_MAX) {
					printf("out.chunks[%lu].offset[%lu] = %u = run[%u].chunks[%lu].offset[%lu] (col %lu)\n",
					       out_chunk_no, out_chunk_offset,
					       run_val[src_bit],
					       src_bit, run_chunk_no[src_bit], run_chunk_offset[src_bit],
					       this_col
					);
				}
				++out_chunk_offset;
				#endif

				load_next_row(in, chunk_size, this_col,
				              &run_chunk_no[src_bit], &run_chunk_offset[src_bit],
				              &run_chunk[src_bit], &run_col[src_bit], &run_val[src_bit]
				);
			}
		}

		for(uint8_t i = 0; i < 2; ++i) {
			run_col[i] = run_chunk[i].columns[sort_col]->data + run_chunk_offset[i];
			run_val[i] = *run_col[i];
		}

		first_out_chunk_offset = 0;
	}
	assert(check_sorted_helper(out, start, stop, sort_col));
}

/*
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

col_table_t *
mergesort(col_table_t *in, size_t col, __attribute__((unused)) size_t domain_size) {
	col_table_t* out = copy_col_table (in);

	mergesort_helper(in, 0, in->num_rows, out, col);

	free_col_table(in);

	return out;
}


void countingsort_helper(col_table_t *in, size_t start, size_t stop, col_table_t *out, size_t col, size_t domain_size);

col_table_t *
countingsort(col_table_t *in, size_t col, size_t domain_size)
{
	INFO("SORT\n");
	INFO(" => input table:");
	print_table_info(in);

	col_table_t* out = create_col_table_like (in);

	countingsort_helper(in, 0, in->num_rows, out, col, domain_size);

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
	void **array_starts = NEWA(void*, domain_size);
	MALLOC_NO_RET(array_starts, "array_starts");
	void **array_ends   = NEWA(void*, domain_size);
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

	my_free(array);
	my_free(array_starts);
	my_free(array_ends);
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

col_table_t *
mergecountingsort(col_table_t *in, size_t col, size_t domain_size)
{
	col_table_t* out = copy_col_table (in);

	merge_counting_sort_helper(in, 0, in->num_rows, out, col, domain_size);

	free_col_table(in);

	return out;
}
*/

void countingsort_intrachunk(table_chunk_t in_chunk, size_t start, size_t stop, table_chunk_t out_chunk, size_t num_cols, size_t sort_col, size_t domain_size, size_t* offset_array, size_t** array_starts, size_t** array_ends) {
	size_t row_count = stop - start;
	if (row_count > 1) {
		// one row for every domain-element
		/* table_chunk_t *chunk_array = NEWA(size_t, table_chunk_t); */
		/* MALLOC_NO_RET(chunk_array, "chunk_array"); */

		// array_starts holds the first position of each row.
		for(size_t i = 0; i < domain_size; ++i) {
			// row_size is measured in bytes, so cast to char**
			array_starts[i] = &offset_array[row_count * i];
			array_ends  [i] = &offset_array[row_count * i];
			//DEBUG("domain elem %ld starts at   %p\n", i, array_starts[i]);
		}


		val_t *col_data = in_chunk.columns[sort_col]->data;
		// Filling the domain-element bucket with chunk_offsets
		for(size_t chunk_offset = start; chunk_offset < stop; chunk_offset++) {
			val_t domain_elem = col_data[chunk_offset];
			*array_ends[domain_elem] = chunk_offset;
			++array_ends[domain_elem];
		}

		// Drain the domain-element buckets into output chunk
		for(size_t this_col = 0; this_col < num_cols; ++this_col) {
			val_t*  in_col_data =  in_chunk.columns[this_col]->data;
			val_t* out_col_data = out_chunk.columns[this_col]->data + start;

			for(size_t cur_domain = 0; cur_domain < domain_size; ++cur_domain) {
				for(size_t* offset_ptr = array_starts[cur_domain]; offset_ptr < array_ends[cur_domain]; ++offset_ptr, ++out_col_data) {
					*out_col_data = in_col_data[*offset_ptr];
				}
			}
		}
	}
}

/*
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
*/

col_table_t *
countingmergesort(col_table_t *in, size_t col, size_t domain_size)
{
	col_table_t *out = create_col_table_like(in);

	size_t chunk_size = get_chunk_size(in);
	// this makes countingsort_intrachunk the whole chunk
	// and thus eliminates the need for merge_intrachunk
	size_t sub_chunk = chunk_size;
	size_t num_chunks = in->num_chunks;
	size_t num_cols = in->num_cols;
	size_t num_rows = in->num_rows;

	{
		size_t*  offset_array = NEWA(size_t, sub_chunk * domain_size);
		MALLOC_NO_RET(offset_array, "offset_array");
		size_t** array_starts = NEWA(size_t*, domain_size);
		MALLOC_NO_RET(array_starts, "array_starts");
		size_t** array_ends   = NEWA(size_t*, domain_size);
		MALLOC_NO_RET(array_ends, "array_ends");

		for(size_t chunk_no = 0; chunk_no < in->num_chunks; ++chunk_no) {
			table_chunk_t in_chunk = *in->chunks[chunk_no];
			table_chunk_t out_chunk = *out->chunks[chunk_no];
			for (size_t start = 0; start < chunk_size; start += sub_chunk) {
				size_t stop = MIN(start + sub_chunk, chunk_size);
				countingsort_intrachunk(in_chunk, start, stop, out_chunk, num_cols,
										col, domain_size, offset_array, array_starts, array_ends);
			}
		}

		my_free(offset_array);
		my_free(array_starts);
		my_free(array_ends);
	}

	// make the output of counting-sort the input for merging
	{
		col_table_t *tmp;
		SWAP(in, out, tmp);
	}

	/*
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
	*/

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

	//in is sorted
	{
		col_table_t *tmp;
		SWAP(in, out, tmp);
	}

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

size_t* domain_count(col_table_t *in, size_t col, size_t domain_size) {
	size_t *domain_counts = NEWA(size_t, domain_size);
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
check_sorted_helper(col_table_t *result, size_t start_row, size_t stop_row, size_t sort_col) {
	size_t chunk_size = get_chunk_size(result);
	size_t start_chunk_no    , stop_chunk_no    ;
	size_t start_chunk_offset, stop_chunk_offset;
	table_chunk_t _chunk;
	val_t *_col;
	val_t _val;
	load_row(result, chunk_size, start_row, sort_col,
	         &start_chunk_no, &start_chunk_offset, &_chunk, &_col, &_val);
	load_row(result, chunk_size,  stop_row, sort_col,
	         & stop_chunk_no, & stop_chunk_offset, &_chunk, &_col, &_val);

	size_t chunk_offset = start_chunk_offset;
	val_t last = 0;
	for(size_t chunk_no = start_chunk_no;
	    chunk_no < stop_chunk_no || (chunk_no == stop_chunk_no && chunk_offset < stop_chunk_offset);
	    ++chunk_no) {
		val_t *data = result->chunks[chunk_no]->columns[sort_col]->data;
		for(;
		    (chunk_no < stop_chunk_no && chunk_offset < chunk_size) || (chunk_no == stop_chunk_no && chunk_offset < stop_chunk_offset);
		    ++chunk_offset) {
			if(data[chunk_offset] < last) {
				#ifdef VERBOSE
				{
					printf("\n");
					printf("%u = data.chunks[%lu].offset[%lu]\n", last, chunk_no, chunk_offset - 1);
					printf("%u = data.chunks[%lu].offset[%lu]\n", data[chunk_offset], chunk_no, chunk_offset);
					printf("Out of order\nNew:\n");
					print_db(result);
				}
				#endif

				return false;
			}
			last = data[chunk_offset];
		}
		chunk_offset = 0;
	}

	return true;
}

bool
check_sorted(col_table_t *result, size_t sort_col, size_t domain_size, col_table_t *copy) {
	size_t chunk_size = get_chunk_size(result);
	size_t num_rows = chunk_size * result->num_chunks;

	bool sorted = check_sorted_helper(result, 0, num_rows, sort_col);
	if(!sorted) {
		return false;
	}

	if(copy) {
		size_t* count1 = domain_count(result, sort_col, domain_size);
		size_t* count2 = domain_count(copy, sort_col, domain_size);
		bool same = 0 == memcmp(count1, count2, domain_size * sizeof(size_t));
		if (!same) {
			#ifdef VERBOSE
			{
				printf("Not the same as the original\nNew:\n");
				print_db(result);
				printf("Old:\n");
				print_db(copy);
			}
			#endif
			return false;
		}

		my_free(count1);
		my_free(count2);
	}

	return true;
}
