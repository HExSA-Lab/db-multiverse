/*
#ifdef __NAUTILUS__
	#include <nautilus/libccompat.h>
#else
	#include <assert.h>
	#include <stdbool.h>
#endif

#include "perf/perf.h"
#include "database/database.h"
#include "database/operators.h"
#include "database/my_malloc.h"

#define log_num_chunks 10
#define LOG_SIZEOF_VAL_T 4
#define 

void test_db_sort() {
	if(1 << LOG_SIZEOF_MVAL != sizeof()) {
		printf("Assert failed");
		exit(1);
	}

	rand_seed(0);
	for(size_t log_chunk_size = 15; log_chunk_size < 25; ++log_chunk_size) {
		size_t chunk_size = 1 << log_chunk_size;
		for(size_t log_num_cols = 0; log_num_cols < 5; ++log_num_cols) {
			size_t log_total_size = log_num_chunks + log_chunk_size + LOG_NUM_COLS + LOG_SIZEOF_VAL_T;
			my_malloc_init(TOTAL_SIZE * chunk_size * TOTAL_SIZE_EXTRA_FACTOR + TOTAL_SIZE_EXTRA);

			col_table_t* table = create_col_table(1 << log_num_chunks, 1 << log_chunk_size, 1 << log_num_cols, domain_size);

			timer_start(&timer);
			// note that this also counts the time to alloc a new table
			table = countingmergesort(table, SORT_COL, DOMAIN_SIZE);
			timer_stop(&timer); timer_print(&timer);
			bool sorted = check_sorted(table, SORT_COL, DOMAIN_SIZE, table_copy);
			if(!sorted) {
				printf("table not sorted;\n");
				exit(1);
			}

			my_malloc_free();
		}
	}
}
*/
