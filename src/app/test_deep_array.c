#include "perf/perf.h"

#ifdef __NAUTILUS__
	#include <nautilus/libccompat.h>
#else
	#include <stdint.h>
	#include <stdio.h>
	#include <stdlib.h>
	#include <string.h>
#endif

#ifdef SMALL
	#define REPS 1
	#define LOG_MEM_MAX 25
	#define LOG_DIM1_MIN 6
	#define LOG_DIM1_MAX 20
	#define LOG_DIM2_MIN 8
	#define LOG_DIM2_MAX 20
#else
	#define REPS 10
	#define LOG_MEM_MAX 24
	#define LOG_DIM1_MIN 6
	#define LOG_DIM1_MAX 20
	#define LOG_DIM2_MIN 6
	#define LOG_DIM2_MAX 20
#endif

#define MIN(a, b) (((a) < (b)) ? (a) : (b))

typedef uint8_t mval_t;

static inline mval_t** deep_array_create(size_t dim1, size_t dim2) {
	void* chunk = malloc(dim1 * sizeof(mval_t*) + dim1 * dim2 * sizeof(mval_t));
	if(!chunk) {
		printf("Failed to allocate %lu\n", dim1 * sizeof(mval_t*) + dim1 * dim2 * sizeof(mval_t));
		exit(1);
	}

	mval_t** ret = chunk;
	chunk += dim1 * sizeof(mval_t*);
	for(size_t i = 0; i < dim1; ++i) {
		ret[i] = chunk;
		chunk += dim2 * sizeof(mval_t);
	}
	return ret;
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
static inline void deep_array_destroy(mval_t** arr, size_t dim1) {
#pragma GCC diagnostic pop
	free(arr);
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
static inline void deep_array_get1(mval_t** arr, size_t dim1, size_t dim2) {
	for(size_t i = 0; i < dim1; ++i) {
		for(size_t j = 0; j < dim2; ++j) {
			volatile mval_t a = arr[i][j];
		}
	}
}

static inline void deep_array_get2(mval_t** arr, size_t dim1, size_t dim2) {
	for(size_t j = 0; j < dim2; ++j) {
		for(size_t i = 0; i < dim1; ++i) {
			volatile mval_t a = arr[i][j];
		}
	}
}
#pragma GCC diagnostic pop

static inline void deep_array_set1(mval_t** arr, size_t dim1, size_t dim2) {
	for(size_t i = 0; i < dim1; ++i) {
		for(size_t j = 0; j < dim2; ++j) {
			arr[i][j] = 0;
		}
	}
}

static inline void deep_array_set2(mval_t** arr, size_t dim1, size_t dim2) {
	for(size_t j = 0; j < dim2; ++j) {
		for(size_t i = 0; i < dim1; ++i) {
			arr[i][j] = 0;
		}
	}
}


static inline void deep_array_copy1(mval_t** arr1, mval_t** arr2, size_t dim1, size_t dim2) {
	for(size_t i = 0; i < dim1; ++i) {
		for(size_t j = 0; j < dim2; ++j) {
			arr2[i][j] = arr1[i][j];
		}
	}
}

static inline void deep_array_copy2(mval_t** arr1, mval_t** arr2, size_t dim1, size_t dim2) {
	for(size_t j = 0; j < dim2; ++j) {
		for(size_t i = 0; i < dim1; ++i) {
			arr2[i][j] = arr1[i][j];
		}
	}
}

void test_deep_array_(size_t dim1, size_t dim2, timer_data_t* timer) {
	timer_start(timer);
	mval_t** arr1 = deep_array_create(dim1, dim2);
	mval_t** arr2 = deep_array_create(dim1, dim2);
	timer_stop_print(timer);

	timer_start(timer);
	deep_array_get1(arr1, dim1, dim2);
	timer_stop_print(timer);

	timer_start(timer);
	deep_array_get2(arr1, dim1, dim2);
	timer_stop_print(timer);

	timer_start(timer);
	deep_array_set1(arr1, dim1, dim2);
	timer_stop_print(timer);

	timer_start(timer);
	deep_array_set2(arr1, dim1, dim2);
	timer_stop_print(timer);

	timer_start(timer);
	deep_array_copy1(arr1, arr2, dim1, dim2);
	timer_stop_print(timer);

	timer_start(timer);
	deep_array_copy2(arr1, arr2, dim1, dim2);
	timer_stop_print(timer);

	timer_start(timer);
	deep_array_destroy(arr1, dim1);
	deep_array_destroy(arr2, dim1);
	timer_stop_print(timer);
}

void test_deep_array() {
	timer_data_t timer;
	timer_initialize(&timer);
	printf("file: $parent_deep_array.csv {\n");

	printf("x dim1,x dim2,");
	timer_print_header("create");
	timer_print_header("get1");
	timer_print_header("get2");
	timer_print_header("set1");
	timer_print_header("set2");
	timer_print_header("copy1");
	timer_print_header("copy2");
	timer_print_header("free");
	printf("\n");

	for(uint8_t log_dim1 = LOG_DIM1_MIN; log_dim1 < LOG_DIM1_MAX; ++log_dim1) {
		for(uint8_t log_dim2 = LOG_DIM2_MIN; log_dim2 < LOG_DIM2_MAX; ++log_dim2) {
			uint8_t log_total_size = log_dim1 + log_dim2;
			if(log_total_size > LOG_MEM_MAX) {
				continue;
			}
			size_t dim1 = 1 << log_dim1;
			size_t dim2 = 1 << log_dim2;
			for(uint8_t reps = 0; reps < REPS; ++reps) {
				printf("%u,%u,", log_dim1, log_dim2);
				test_deep_array_(dim1, dim2, &timer);
				printf("\n");
			}
		}
	}

	printf("}\n");
	timer_finalize(&timer);
}
