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
#include <time.h>
#include <sys/time.h>

#define INFO(fmt, args...)  printf("DB-MV: " fmt, ##args)
#define DEBUG(fmt, args...) fprintf(stderr, "DB-MB DBG: " fmt, ##args)
#define ERROR(fmt, args...) fprintf(stderr, "DB-MB ERR: " fmt, ##args)

#define VERSION_STRING "0.0.1"

#define DEFAULT_TRIALS   100
#define DEFAULT_THROWOUT 10
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

typedef struct exp_options {
	size_t num_chunks;
	size_t num_cols;
	size_t chunksize;
	table_type_t table_type;
	unsigned int domainsize;
	unsigned int repetitions;
} exp_options_t;

static exp_options_t options;

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

	INFO("SELECTION on column %lu = %u",  col, val);
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
					uint8_t match = (*indata == val);
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

static col_table_t *
basic_rowise_selection_const (col_table_t *t, size_t col, val_t val)
{
	col_table_t *r = NEW(col_table_t);
	column_chunk_t *c_chunk;
	table_chunk_t *t_chunk;
	size_t total_results = 0;
	size_t chunk_size = t->chunks[0]->columns[0]->chunk_size;
	size_t out_chunks = 1;

	INFO("SELECTION on column %lu = %u",  col, val);
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

static col_table_t *
scatter_gather_selection_const (col_table_t *t, size_t col, val_t val)
{
	col_table_t *r = NEW(col_table_t);
	column_chunk_t *c_chunk;
	table_chunk_t *t_chunk;
	size_t total_results = 0;
	size_t chunk_size = t->chunks[0]->columns[0]->chunk_size;
	size_t out_chunks = 1;
	column_chunk_t **idx;
	column_chunk_t **cidx;

	INFO("SELECTION on column %lu = %u",  col, val);
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
		c_chunk->data = NEWA(val_t, chunk_size);
		MALLOC_CHECK_NO_MES(c_chunk->data);
	}
	r->chunks[out_chunks - 1] = t_chunk;

	// create index columns

	// gather based on index column one column at a time
	for(int i = 0; i < t->num_chunks; i++)
	{

	}

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
	table = projection(table, proj, 4);
//	table = basic_rowise_selection_const(table, 1, 15);
	table = selection_const(table, 1, 15);
	free_col_table(table);
}



typedef enum exp_id {
    EXP_SCAN,
} exp_id_t;


int
main (int argc, char ** argv){
    int c;
    unsigned throwout = DEFAULT_THROWOUT;
	char * exp_str    = DEFAULT_EXP_STR;
	int exp_id        = DEFAULT_EXP;
	time_int * runtimes;
	
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
            {0, 0, 0, 0}
        };

        c = getopt_long(argc, argv, "t:k:shvu:n:c:y:d:", lopts, &optidx);

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

