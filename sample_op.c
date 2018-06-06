/*
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * 
 * Copyright (c) 2018, Kyle C. Hale <khale@cs.iit.edu>
 * 
 * This is an incredibly dumb scan of a table using some filter function. No
 * column indeces are involved. This is purely for experimentation purposes.
 * 
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <getopt.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>

#define INFO(fmt, args...)  printf("DB-MV: " fmt, ##args)
#define DEBUG(fmt, args...) fprintf(stderr, "DB-MB DBG: " fmt, ##args)
#define ERROR(fmt, args...) fprintf(stderr, "DB-MB ERR: " fmt, ##args)

#define VERSION_STRING "0.0.1"

#define DEFAULT_TRIALS   1
#define DEFAULT_THROWOUT 1
#define DEFAULT_EXP_STR "scan"
#define DEFAULT_EXP      EXP_SCAN

#define NEW(typ) ((typ *) malloc(sizeof(typ)))
#define NEWA(typ,size) ((typ *) malloc(sizeof(typ) * size))
#define NEWPA(typ,size) ((typ **) malloc(sizeof(typ*) * size))

#define MALLOC_CHECK(pointer,mes) \
	do { \
		if (!(pointer)) { \
			ERROR("Could not allocate " #pointer  " (%s) in %s:%u\n", mes, __FUNCTION__, __LINE__); \
        	return NULL; \
		} \
	} while(0)

#define MALLOC_CHECK_NO_MES(pointer) MALLOC_CHECK(pointer,"")

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

typedef enum operator {
	SELECTION_CONST = 0,
	SELECTION_ATT,
	PROJECTION,
	NUM_OPS
} operator_t;

typedef col_table_t* (*op_implementation_t) ();

#define MAX_IMPL 5

typedef struct op_implementation_info {
	operator_t op;
	char *names[MAX_IMPL];
	op_implementation_t implementations[MAX_IMPL];
	size_t num_impls;
} op_implementation_info_t;



typedef struct exp_options {
	size_t num_chunks;
	size_t num_cols;
	size_t chunksize;
	table_type_t table_type;
	unsigned int domainsize;
	unsigned int repetitions;
	op_implementation_t *impls;
} exp_options_t;

// function declarations
static exp_options_t defaultOptions (void);
static void print_options (void);
static void print_table_info (col_table_t *t);
static col_table_t *createColTable (size_t num_chunks, size_t chunk_size, size_t num_cols);
static void free_col_table (col_table_t *t);
static col_table_t *projection(col_table_t *t, size_t *pos, size_t num_proj);
static col_table_t *selection_const (col_table_t *t, size_t col, val_t val);
static col_table_t *basic_rowise_selection_const (col_table_t *t, size_t col, val_t val);
static col_table_t *scatter_gather_selection_const (col_table_t *t, size_t col, val_t val);
static void usage (char * prog);
static void version (void);
static void  driver (void);
static void set_impl_op(operator_t op, char *impl_name);

// local global vars ;-)
static exp_options_t options;

// information about operator implementations
static op_implementation_info_t impl_infos[] = {
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
		}
};
static op_implementation_t default_impls[] = {
		basic_rowise_selection_const, // SELECTION_CONST
		NULL, // SELECTION_ATT
		projection // PROJECTION
};
static char * op_names[] = {
		[SELECTION_CONST] = "selection_const",
		[SELECTION_ATT] = "selection_att",
		[PROJECTION] = "projection",
};

static exp_options_t
defaultOptions (void)
{
	exp_options_t o;

	o.num_chunks = 10;
	o.num_cols   = 10;
	o.chunksize = 1024 * 1024; // 4M values
	o.table_type = COLUMN;
	o.domainsize = 100;
	o.repetitions = 1;
	o.impls = default_impls;
//	NEWA(op_implementation_t, NUM_OPS);
//	if (! o.impls)
//	{
//		INFO("could not allocate memory for options");
//		exit(1);
//	}
//	for(int i = 0; i < NUM_OPS; i++)
//	{
//		o.impls[i] = default_impls[i];
//	}

	return o;
}

static void
print_options (void)
{
	printf(" # repetitions=%u\n", options.repetitions);
    printf(" # numchunks=%lu\n", options.num_chunks);
    printf(" # chunksize=%lu\n", options.chunksize);
    printf(" # numcols=%lu\n", options.num_cols);
    printf(" # tabletype=%u (0=columnar, 1=row)\n", options.table_type);
    printf(" # domainsize=%u\n", options.domainsize);
    for(int i = 0; i < NUM_OPS; i++)
    {
    	op_implementation_info_t info = impl_infos[i];
    	op_implementation_t impl = options.impls[i];
    	char *impl_name = "";

    	for(int j = 0; j < info.num_impls; j++)
    	{
    		if(impl == info.implementations[j])
    		{
    			impl_name = info.names[j];
    	    	printf(" # implementation for %s is %s\n", op_names[i], impl_name);
    		}
    	}
    }
}

static inline val_t
randVal(void)
{
	return (val_t) rand() % options.domainsize;
}

static void
print_table_info (col_table_t *t)
{
	printf("Table with %lu columns, %lu rows, and %lu chunks\n", t->num_cols, t->num_rows, t->num_chunks);
}

static col_table_t *
createColTable (size_t num_chunks, size_t chunk_size, size_t num_cols)
{
	col_table_t * t = NEW(col_table_t);
	t->num_chunks = num_chunks;
	t->num_cols = num_cols;
	t->num_rows = t->num_chunks * chunk_size;
	t->chunks = NEWPA(table_chunk_t, num_chunks);

	MALLOC_CHECK_NO_MES(t->chunks);

	for(int i = 0; i < num_chunks; i++)
	{
		table_chunk_t *tc = NEW(table_chunk_t);
		MALLOC_CHECK(tc, "table chunks");

		t->chunks[i] = tc;
		tc->columns = NEWPA(column_chunk_t, num_cols);
		MALLOC_CHECK_NO_MES(tc->columns);

		for (int j = 0; j < num_cols; j++)
		{
			column_chunk_t *c = NEW(column_chunk_t);
			MALLOC_CHECK(c, "column chunks");

			tc->columns[j] = c;
			c->chunk_size = chunk_size;
			c->data = NEWA(val_t, chunk_size);
			MALLOC_CHECK(c->data, "chunk data");

			for(int k = 0; k < chunk_size; k++)
			{
				c->data[k] = randVal();
			}
		}
	}

	INFO("created table: ");
	print_table_info(t);

	return t;
}

static void
free_col_table (col_table_t *t)
{
	for(int i = 0; i < t->num_chunks; i++) {
		table_chunk_t * tc = t->chunks[i];

		for (int j = 0; j < t->num_cols; j++)
		{
			column_chunk_t *c = tc->columns[j];
			free(c->data);
			free(c);
		}
		free(tc);
	}

	free(t->chunks);
	free(t);
}

// projection only changes the schema
static col_table_t *
projection(col_table_t *t, size_t *pos, size_t num_proj)
{
	INFO("PROJECTION on columns ");
	for (int i = 0; i < num_proj; i++)
	{
		printf("%lu%s", pos[i], (i < num_proj - 1) ? ", " : " ");
	}
	printf("over ");
	print_table_info(t);

	for(int i = 0; i < t->num_chunks; i++)
	{
		table_chunk_t *tc = t->chunks[i];
		column_chunk_t **new_columns = NEWPA(column_chunk_t, num_proj);

		MALLOC_CHECK_NO_MES(new_columns);

		unsigned char *colRetained = NEWA(unsigned char, t->num_cols);

		MALLOC_CHECK_NO_MES(colRetained);

		for(int j = 0; j < t->num_cols; j++)
			colRetained[j] = 0;

		for(int j = 0; j < num_proj; j++)
		{
			new_columns[j] = tc->columns[pos[j]];
			colRetained[pos[j]] = 1;
		}

		for(int j = 0; j < t->num_cols; j++)
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


	INFO(" => output table: ");
	print_table_info(t);

	return t;
}

#define UNROLL_SIZE 1028

static col_table_t *
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

	for(int i = 0; i < t->num_cols; i++)
	{
		t_chunk->columns[i] = NEW(column_chunk_t);
		MALLOC_CHECK_NO_MES(t_chunk->columns[i]);
		c_chunk = t_chunk->columns[i];
		c_chunk->data = NEWA(val_t, t->chunks[0]->columns[0]->chunk_size);
		MALLOC_CHECK_NO_MES(c_chunk->data);
	}
	r->chunks[out_chunks - 1] = t_chunk;
	size_t out_pos = 0;

	for(int i = 0; i < t->num_chunks; i++)
	{
		table_chunk_t *tc = t->chunks[i];
		column_chunk_t *c = tc->columns[col];
		val_t *outdata, *outstart;
		val_t *indata = c->data;
		val_t *instart = indata;

		for(int j = 0; j < t->num_cols; j++)
		{
			outdata = t_chunk->columns[j]->data + out_pos;
			outstart = outdata;
			size_t count_results = (j == 0);

			while((indata + UNROLL_SIZE < instart + chunk_size) && (outdata + UNROLL_SIZE < outstart + - out_pos + chunk_size))
			{
				// tight loop with compile time constant of iterations
				for(int j = 0; j < UNROLL_SIZE; j++)
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

				for(int i = 0; i < t->num_cols; i++)
				{
					t_chunk->columns[i] = NEW(column_chunk_t);
					MALLOC_CHECK_NO_MES(t_chunk->columns[i]);

					c_chunk = t_chunk->columns[i];
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
				for(int j = 0; j < t->num_cols; j++)
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

static col_table_t *
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

	for(int i = 0; i < t->num_cols; i++)
	{
		t_chunk->columns[i] = NEW(column_chunk_t);
		MALLOC_CHECK_NO_MES(t_chunk->columns[i]);
		c_chunk = t_chunk->columns[i];
		c_chunk->data = NEWA(val_t, t->chunks[0]->columns[0]->chunk_size);
		MALLOC_CHECK_NO_MES(c_chunk->data);
	}
	r->chunks[out_chunks - 1] = t_chunk;
	size_t out_pos = 0;

	for(int i = 0; i < t->num_chunks; i++)
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

				for(int i = 0; i < t->num_cols; i++)
				{
					t_chunk->columns[i] = NEW(column_chunk_t);
					MALLOC_CHECK_NO_MES(t_chunk->columns[i]);

					c_chunk = t_chunk->columns[i];
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
				for(int j = 0; j < t->num_cols; j++)
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

static inline column_chunk_t *
createColChunk(size_t chunksize)
{
	column_chunk_t *result = NEW(column_chunk_t);
	MALLOC_CHECK_NO_MES(result);

	result->chunk_size = chunksize;
	result->data = NEWA(val_t, chunksize);
	MALLOC_CHECK_NO_MES(result->data);

	return result;
}

static col_table_t *
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
	idx = NEWPA(column_chunk_t, t->num_chunks);
	MALLOC_CHECK_NO_MES(idx);
	cidx = NEWPA(column_chunk_t, t->num_chunks);
	MALLOC_CHECK_NO_MES(cidx);
	for(int i = 0; i < t->num_chunks; i++)
	{
		idx[i] = createColChunk(chunk_size);
		cidx[i] = createColChunk(chunk_size);
	}

	// scatter based on condition
	unsigned int outpos = 0;
	unsigned int outchunk = 0;
	val_t *idx_c = idx[0]->data;
	val_t *cidx_c = cidx[0]->data;

	for(int i = 0; i < t->num_chunks; i++)
	{
		table_chunk_t *tc = t->chunks[i];
		column_chunk_t *c = tc->columns[col];
		val_t *indata = c->data;

		for(int j = 0; j < chunk_size; j++)
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

	// create output table
	out_chunks = total_results / chunk_size + (total_results % chunk_size == 0 ? 0 : 1);

	col_table_t *r = NEW(col_table_t);
	MALLOC_CHECK_NO_MES(r);
	r->num_cols = t->num_cols;
	r->num_rows = total_results;
	r->num_chunks = out_chunks;
	r->chunks = NEWPA(table_chunk_t, out_chunks);
	MALLOC_CHECK_NO_MES(r->chunks);

	for(int i = 0; i < out_chunks; i++)
	{
		t_chunk =  NEW(table_chunk_t);
		MALLOC_CHECK_NO_MES(t_chunk);

		t_chunk->columns = NEWPA(column_chunk_t, t->num_cols);
		MALLOC_CHECK_NO_MES(t_chunk->columns);

		for(int i = 0; i < t->num_cols; i++)
		{
			t_chunk->columns[i] = createColChunk(chunk_size);
		}
		r->chunks[i] = t_chunk;
	}

	// gather based on index column one column at a time
	for(int i = 0; i < t->num_cols; i++)
	{
		for(int j = 0; j < out_chunks; j++)
		{
			val_t *outdata = r->chunks[j]->columns[i]->data;
			val_t *idx_c = idx[j]->data;
			val_t *cidx_c = cidx[j]->data;

			for(int k = 0; k < chunk_size; k++)
			{
				outdata[k] = t->chunks[cidx_c[k]]->columns[i]->data[idx_c[k]];
			}
		}
	}

	//TODO free

	INFO(" => output table: ");
	print_table_info(r);

	free_col_table(t);

    return r;
}

static void
usage (char * prog)
{
    printf("Usage: %s [options]\n", prog);
    printf("\nOptions:\n");

    printf("  -t, --trials <trial count> : number of experiments to run (default=%d)\n", DEFAULT_TRIALS);
    printf("  -k, --throwout <throwout count> : number of iterations to throw away (default=%d)\n", DEFAULT_THROWOUT);
    printf("  -h, ---help : display this message\n");
    printf("  -v, --version : display the version number and exit\n");
    printf("  -u, --numchunks : number of table chunks (default=%lu)\n", defaultOptions().num_chunks);
    printf("  -n, --chunksize : number of rows in each chunk (default=%lu)\n", defaultOptions().chunksize);
    printf("  -c, --numcols : number of columns in the table (default=%lu)\n", defaultOptions().num_cols);
    printf("  -y, --tabletype : type of table to be used (0=columnar, 1=row) (default=%u)\n", defaultOptions().table_type);
    printf("  -d, --domainsize : a table's attribute values are sampled uniform random from [0, domainsize -1] (default=%u)\n", defaultOptions().domainsize);
    printf("  -i, --implementations : a list of key=value pairs selecting operator implementations\n");

    printf("\nExperiments (default=%s):\n", "scan");
    printf("  --scan\n");

    printf("\n");
}


static void
version (void)
{
    printf("simple operator experiment (table scan):\n");
    printf("version %s\n\n", VERSION_STRING);
}

typedef unsigned long time_int;

static inline unsigned long
my_gettime (void)
{
	struct timeval st;
	unsigned long result;

	gettimeofday(&st, NULL);
	result = 1000000 * st.tv_sec + st.tv_usec;

	return result;
}


static void 
driver (void)
{
	size_t proj[] = { 0, 1, 2, 3 };

	col_table_t *table = createColTable(options.num_chunks, options.chunksize, options.num_cols);
	INFO("created input table(s)\n");
	table = options.impls[PROJECTION](table, proj, 4);
	table = options.impls[SELECTION_CONST](table, 1, 1);
	free_col_table(table);
}



typedef enum exp_id {
    EXP_SCAN,
} exp_id_t;

static void
set_impl_op(operator_t op, char *impl_name)
{
	op_implementation_t op_impl = NULL;
	op_implementation_info_t info;

	INFO("select \"%s\" operator implementation \"%s\"\n", op_names[op], impl_name ? impl_name : "NULL");
	info = impl_infos[op];

	for(int i = 0; i < info.num_impls; i++)
	{
		if(strcmp(info.names[i], impl_name) == 0)
		{
			op_impl = info.implementations[i];
			options.impls[op] = op_impl;
		}
	}

	if (op_impl == NULL)
	{
		INFO("did not find implementation for operator \"%s\" named \"%s\"\n", op_names[op], impl_name);
		exit(1);
	}
}


int
main (int argc, char ** argv){
    int c;
    unsigned throwout = DEFAULT_THROWOUT;
	char * exp_str    = DEFAULT_EXP_STR;
	int exp_id        = DEFAULT_EXP;
	time_int * runtimes;
	char *subopts;
	char *subvalue;
	
	options = defaultOptions();
	srand(time(NULL));

    while (1) {

        int optidx = 0;

        static struct option lopts[] = {
            {"trials", required_argument, 0, 't'},
            {"throwout", required_argument, 0, 'k'},
            {"scan", no_argument, 0, 's'},
            {"help", no_argument, 0, 'h'},
            {"version", no_argument, 0, 'v'},
			{"numchunks", required_argument, 0, 'u'},
			{"chunksize", required_argument, 0, 'n'},
			{"numcols", required_argument, 0, 'c'},
			{"tabletype", required_argument, 0, 'y'},
			{"domainsize", required_argument, 0, 'd'},
			{"implementations", required_argument, 0, 'i'},
            {0, 0, 0, 0}
        };

        static const char *impl_opts[] =
        {
            [SELECTION_CONST] = "sel_const",
            [SELECTION_ATT] = "sel_att",
            [PROJECTION] = "projection",
            NULL
        };


        c = getopt_long(argc, argv, "t:k:shvu:n:c:y:d:i:", lopts, &optidx);

        if (c == -1) {
            break;
        }

        switch (c) {
            case 't':
                options.repetitions = atoi(optarg);
                break;
            case 'k':
                throwout = atoi(optarg);
                break;
            case 's':
                exp_id = EXP_SCAN;
                exp_str = "scan";
                break;
            case 'h':
                usage(argv[0]);
                exit(EXIT_SUCCESS);
            case 'v':
                version();
                exit(EXIT_SUCCESS);
            case '?':
                break;
            case 'u':
            	options.num_chunks = (size_t) atoi(optarg);
            	break;
            case 'n':
            	options.chunksize = (size_t) atoi(optarg);
            	break;
            case 'c':
            	options.num_cols = (size_t) atoi(optarg);
            	break;
            case 'y':
            	options.table_type = (table_type_t) atoi(optarg); //TODO check that exists
            	break;
            case 'd':
            	options.domainsize = (unsigned int) atoi(optarg);
            	break;
            case 'i':
            	subopts = optarg;
				while (*subopts != '\0')
				{
					char *saved = subopts;
					int subo = getsubopt(&subopts, (char **)impl_opts, &subvalue);
					switch(subo)
					{
						case SELECTION_CONST:
						case SELECTION_ATT:
						case PROJECTION:
							set_impl_op(subo, subvalue);
							break;
						default:
							/* Unknown suboption. */
							printf("Unknown operator implementation option \"%s\"\n", saved);
							exit(1);
					}
				}
				break;
            default:
                printf("?? getopt returned character code 0%o ??\n", c);
        }

    }

    INFO("# table scan experiment:\n");
    INFO("# Clocksource = clock_gettime(CLOCK_REALTIME)\n");
    INFO("# Experiment = %s\n", exp_str);
    INFO("# Output is in ns\n");
    INFO("# %d throwout\n", throwout);
    print_options();
    runtimes = NEWA(time_int, options.repetitions);
    
    for(int i = 0; i < options.repetitions; i++)
    {
    	time_int start = my_gettime();
    	driver();
    	time_int stop = my_gettime();
    	runtimes[i] = stop - start;
    }

    printf("\n\nRUNTIMES (sec): [");
    for(int i = 0; i < options.repetitions; i++)
    {
    	printf("%f%s", ((double) runtimes[i]) / 1000000, (i < options.repetitions - 1) ? ", ":  "");
    }
    printf("]\n\n");
    return 0;
}


