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

#define INFO(fmt, args...)  printf("DB-MV: " fmt, ##args)
#define DEBUG(fmt, args...) fprintf(stderr, "DB-MB DBG: " fmt, ##args)
#define ERROR(fmt, args...) fprintf(stderr, "DB-MB ERR: " fmt, ##args)

#define VERSION_STRING "0.0.1"

#define DEFAULT_TRIALS   100
#define DEFAULT_THROWOUT 10
#define DEFAULT_EXP_STR "scan"
#define DEFAULT_EXP      EXP_SCAN

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

typedef struct table {
    row_t * rows;
    size_t size;
} table_t;


static void
usage (char * prog)
{
    printf("Usage: %s [options]\n", prog);
    printf("\nOptions:\n");

    printf("  -t, --trials <trial count> : number of experiments to run (default=%d)\n", DEFAULT_TRIALS);
    printf("  -k, --throwout <throwout count> : number of iterations to throw away (default=%d)\n", DEFAULT_THROWOUT);
    printf("  -h, ---help : display this message\n");
    printf("  -v, --version : display the version number and exit\n");

    printf("\nExperiments (default=%s):\n", "scan");
    printf("  --scan\n");

    printf("\n");
}


static void
version ()
{
    printf("simple operator experiment (table scan):\n");
    printf("version %s\n\n", VERSION_STRING);
}


static void 
driver () 
{
}

typedef enum exp_id {
    EXP_SCAN,
} exp_id_t;


int
main (int argc, char ** argv)
{
    int c;
    unsigned trials   = DEFAULT_TRIALS;
    unsigned throwout = DEFAULT_THROWOUT;
	char * exp_str    = DEFAULT_EXP_STR;
	int exp_id        = DEFAULT_EXP;
	
    while (1) {

        int optidx = 0;

        static struct option lopts[] = {
            {"trials", required_argument, 0, 't'},
            {"throwout", required_argument, 0, 'k'},
            {"scan", no_argument, 0, 's'},
            {"help", no_argument, 0, 'h'},
            {"version", no_argument, 0, 'v'},
            {0, 0, 0, 0}
        };

        c = getopt_long(argc, argv, "t:k:shv", lopts, &optidx);

        if (c == -1) {
            break;
        }

        switch (c) {
            case 't':
                trials = atoi(optarg);
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
            default:
                printf("?? getopt returned character code 0%o ??\n", c);
        }

    }

    INFO("# table scan experiment:\n");
    INFO("# Clocksource = clock_gettime(CLOCK_REALTIME)\n");
    INFO("# Experiment = %s\n", exp_str);
    INFO("# Output is in ns\n");
    INFO("# %d trials\n", trials);
    INFO("# %d throwout\n", throwout);

	// TODO: experiment
    
    return 0;
}

