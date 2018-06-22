#include <getopt.h>
#include <string.h>
#include <time.h>

#include "common.h"
#include "database.h"
#include "operators.h"
#include "timing.h"

typedef struct exp_options {
	size_t num_chunks;
	size_t num_cols;
	size_t chunksize;
	table_type_t table_type;
	unsigned int domain_size;
	unsigned int repetitions;
	op_implementation_t *impls;

    cntr_type_t cntr_type;
    void * cntr_arg;

} exp_options_t;


static const char * cntr_opts[] = 
{ 
    [COUNTER_TYPE_GTOD] = "gettimeofday",
    [COUNTER_TYPE_CGT]  = "clock_gettime",
    [COUNTER_TYPE_PERF] = "perf-counter",
    NULL
};

static exp_options_t defaultOptions (void);
static void print_options (exp_options_t options);
static void usage (char * prog);
static void version (void);
static uint64_t driver (exp_options_t options);
static void set_impl_op(operator_t op, char *impl_name, exp_options_t *options_ptr);

static exp_options_t
defaultOptions (void)
{
	exp_options_t o;

	o.num_chunks = 10;
	o.num_cols   = 10;
	o.chunksize = 1024 * 1024; // 4M values
	o.table_type = COLUMN;
	o.domain_size = 100;
	o.repetitions = 1;
	o.impls = default_impls;
    o.cntr_type = COUNTER_TYPE_GTOD;
    o.cntr_arg  = NULL;
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
print_options (exp_options_t options)
{
    INFO("# Clocksource = %s\n", cntr_opts[options.cntr_type]);
    if (options.cntr_type == COUNTER_TYPE_PERF) {
        perf_arg_t * arg = (perf_arg_t*)options.cntr_arg;
        INFO("#   perf event: %s\n", arg->name);
    }
	printf(" # repetitions=%u\n", options.repetitions);
    printf(" # numchunks=%lu\n", options.num_chunks);
    printf(" # chunksize=%lu\n", options.chunksize);
    printf(" # numcols=%lu\n", options.num_cols);
    printf(" # tabletype=%u (0=columnar, 1=row)\n", options.table_type);
    printf(" # domainsize=%u\n", options.domain_size);
    for(int i = 0; i < NUM_OPS; i++)
    {
    	op_implementation_info_t info = impl_infos[i];
    	op_implementation_t impl = options.impls[i];
    	char *impl_name = "";

    	for(size_t j = 0; j < info.num_impls; j++)
    	{
    		if(impl == info.implementations[j])
    		{
    			impl_name = info.names[j];
    	    	printf(" # implementation for %s is %s\n", op_names[i], impl_name);
    		}
    	}
		if (impl_name[0] == '\0') {
			printf(" # implementation for %s is unknown\n", op_names[i]);
		}
    }
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
    printf("  -d, --domainsize : a table's attribute values are sampled uniform random from [0, domainsize -1] (default=%u)\n", defaultOptions().domain_size);
    printf("  -i, --implementations : a list of key=value pairs selecting operator implementations\n");
    printf("  -l, --counter : counter type (gettimeofday, clock_gettime, or perf-counter, default=%s)\n", cntr_opts[defaultOptions().cntr_type]);
    printf("              Supported perf events (default is cpu-cycles):\n");
    printf("                  ctx-switches\n");
    printf("                  page-faults\n");
    printf("                  cpu-cycles\n");
    printf("                  retired-instrs\n");
    printf("                  branch-misses\n");
    printf("                  migrations\n");
    printf("                  cache-misses\n");
    printf("                  dtlb-load-misses\n");
    printf("                  itlb-load-misses\n");

    printf("\nExperiments (default=%s):\n", "scan");
    printf("  --scan\n");

    printf("\n");
}


static void
version (void)
{
    printf("simple operator experiment:\n");
    printf("version %s\n\n", VERSION_STRING);
}


static uint64_t
driver (exp_options_t options)
{
	#define PROJ_SIZE 4
	size_t proj[PROJ_SIZE] = { 0, 1, 2, 3 };

    counter_start();
	col_table_t *table = create_col_table(options.num_chunks, options.chunksize, options.num_cols, options.domain_size);
	INFO("created input table(s)\n");

    counter_stop();
    counter_reset();

    counter_start();

	col_table_t* copy = copy_col_table(table);
	table = options.impls[SORT](table, 2, options.domain_size);

	if (! check_sorted(table, 2, options.domain_size, copy)) {
		ERROR("Not actually sorted!!\n");
	}
	table = options.impls[PROJECTION](table, proj, PROJ_SIZE);
	table = options.impls[SELECTION_CONST](table, 1, 1);

    counter_stop();

	free_col_table(table);

    return counter_get();
}


static const char *perf_types[] = 
{
    [PERF_CTX_SWITCH]     = "ctx-switches",
    [PERF_PAGE_FAULTS]    = "page-faults",
    [PERF_CPU_CYCLES]     = "cpu-cycles",
    [PERF_INSTR_RETIRED]  = "retired-instrs",
    [PERF_BRANCH_MISSES]  = "branch-misses",
    [PERF_CPU_MIGRATIONS] = "migrations",
    [PERF_CACHE_MISSES]   = "cache-misses",
    [PERF_DTLB_LOAD_MISS] = "dtlb-load-misses",
    [PERF_ITLB_LOAD_MISS] = "itlb-load-misses",
    NULL
};


static void
set_perf_opt (exp_options_t * opts, const char * name)
{
    if (!name) {
        name = "cpu-cycles";
    }

    for (int i = 0; i < PERF_NUM_COUNTERS; i++) {
        if (strcmp(name, perf_types[i]) == 0) {
            perf_arg_t * arg;
            arg = NEWZ(perf_arg_t);
            MALLOC_CHECK_VOID(arg, "perf argument");
            arg->name = (void*)perf_types[i];
            arg->type = i;

            opts->cntr_arg = (void*)arg;

            break;
        }
    }

    if (!opts->cntr_arg) {
        ERROR("Unknown perf counter type (%s)\n", name);
        exit(EXIT_FAILURE);
    }
}


static void
set_impl_op(operator_t op, char *impl_name, exp_options_t *options_ptr)
{
	op_implementation_t op_impl = NULL;
	op_implementation_info_t info;

	INFO("select \"%s\" operator implementation \"%s\"\n", op_names[op], impl_name ? impl_name : "NULL");
	info = impl_infos[op];

	for(size_t i = 0; i < info.num_impls; i++)
	{
		if(strcmp(info.names[i], impl_name) == 0)
		{
			op_impl = info.implementations[i];
			options_ptr->impls[op] = op_impl;
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
	time_int * runtimes;
	char *subopts;
	char *subvalue;

	exp_options_t options = defaultOptions();
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
            {"counter", required_argument, 0, 'l'},
            {0, 0, 0, 0}
        };

        static const char *impl_opts[] =
        {
            [SELECTION_CONST] = "sel_const",
            [SELECTION_ATT] = "sel_att",
            [PROJECTION] = "projection",
            NULL
        };



        c = getopt_long(argc, argv, "t:k:shvu:n:c:y:d:i:l:", lopts, &optidx);

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
            	options.domain_size = (unsigned int) atoi(optarg);
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
							set_impl_op(subo, subvalue, &options);
							break;
						default:
							/* Unknown suboption. */
							printf("Unknown operator implementation option \"%s\"\n", saved);
							exit(EXIT_FAILURE);
					}
				}
				break;
            case 'l':
            	subopts = optarg;
				while (*subopts != '\0')
				{
					char *saved = subopts;
					int subo = getsubopt(&subopts, (char **)cntr_opts, &subvalue);
					switch(subo)
					{
                        case COUNTER_TYPE_GTOD:
                        case COUNTER_TYPE_CGT:
                            options.cntr_type = subo;
                            break;
                        case COUNTER_TYPE_PERF:
                            options.cntr_type = subo;
							set_perf_opt(&options, subvalue);
							break;
						default:
							/* Unknown suboption. */
							printf("Unknown counter type\"%s\"\n", saved);
							exit(EXIT_FAILURE);
					}
				}
				break;
            default:
                printf("?? getopt returned character code 0%o ??\n", c);
        }

    }

    INFO("# Experiment = %s\n", exp_str);
    INFO("# %d throwout\n", throwout);
    print_options(options);
    runtimes = NEWA(time_int, options.repetitions);

    counter_init(options.cntr_type, options.cntr_arg);
    
    for(size_t i = 0; i < options.repetitions; i++)
    {
    	runtimes[i] = driver(options);
    }

    printf("\n\nCOUNTER VALS: [");
    for(size_t i = throwout; i < options.repetitions; i++)
    {
    	printf("%lu%s", runtimes[i], (i < options.repetitions - 1) ? ", ":  "");
    }
    printf("]\n\n");

    counter_deinit();

    return 0;
}
