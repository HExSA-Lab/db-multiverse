#ifdef __NAUTILUS__
	#include <nautilus/libccompat.h>
#else
	#include <stdint.h>
	#include <stdio.h>
	#include <stdlib.h>
	#include <string.h>
#endif

#include "app/perf.h"

#ifdef SMALL
	#define REPS 1
	#define PARAM_MIN 6
	#define PARAM_MAX 23
#else
	#define REPS 10
	#define PARAM_MIN 6
	#define PARAM_MAX 31
#endif

typedef uint8_t mval_t;

void test_array() {
	timer_data_t timer;
	timer_initialize(&timer);

	printf("file: $parent_array.csv {\n");
	printf("x array size,");
	timer_print_header("malloc");
	timer_print_header("copy (memcpy)");
	timer_print_header("copy (individually)");
	timer_print_header("set");
	timer_print_header("get");
	timer_print_header("reverse");
	timer_print_header("free");
	printf("\n");

	for(size_t param = PARAM_MIN; param < PARAM_MAX; ++param) {
		for(size_t reps = 0; reps < REPS; ++reps) {
			size_t size = 1 << param;
			printf("%ld,", param);

			timer_start(&timer);
			mval_t* array = malloc(size * sizeof(mval_t));
			mval_t* array_copy = malloc(size * sizeof(mval_t));
			timer_stop_print(&timer);

			timer_start(&timer);
			memcpy(array_copy, array, size * sizeof(mval_t));
			timer_stop_print(&timer);

			timer_start(&timer);
			for(mval_t *src = array, *dst = array_copy, *stop = array + size;
				src < stop; src++, dst++) {
				*dst = *src;
			}
			timer_stop_print(&timer);

			timer_start(&timer);
			#pragma GCC diagnostic push
			#pragma GCC diagnostic ignored "-Wunused-variable"
			for(mval_t *src = array, *stop = array + size;
				src < stop; src++) {
				volatile mval_t dst = *src;
			}
			#pragma GCC diagnostic pop
			timer_stop_print(&timer);

			timer_start(&timer);
			for(mval_t *dst = array, *stop = array + size;
				dst < stop; dst++) {
				*dst = 0;
			}
			timer_stop_print(&timer);

			timer_start(&timer);
			for(mval_t *src = array, *dst = array_copy + size - 1, *stop = array + size;
				src < stop; src++, dst--) {
				*dst = *src;
			}
			timer_stop_print(&timer);

			timer_start(&timer);
			free(array_copy);
			free(array);
			timer_stop_print(&timer);

			printf("\n");
		}
	}
	printf("}\n");
	timer_finalize(&timer);
}
