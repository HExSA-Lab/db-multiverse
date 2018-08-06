#include "app/perf.h"

#ifdef __NAUTILUS__
	#include "./perf_nautk.c_source"
#else
	#include "./perf_linux.c_source"
#endif

void timer_initialize(timer_data_t* obj) {
	timer_initialize_specific(obj);
}

void timer_print(timer_data_t* obj) {
	uint64_t start_cycles = ((uint64_t)obj->start_hi << 32) | obj->start_lo;
	uint64_t stop_cycles = ((uint64_t)obj->stop_hi << 32) | obj->stop_lo;
	uint64_t elapsed_cycles = stop_cycles - start_cycles;
	printf("%lu,", elapsed_cycles);
	for(unsigned int i = 0; i < PERF_EVENTS_SPECIFIC; ++i) {
		uint64_t diff = obj->perf_event_stop[i] - obj->perf_event_start[i];
		printf("%lu,", diff);
	}
}

void timer_print_header(char* name) {
	printf("%s (time),", name);
	for(unsigned int i = 0; i < PERF_EVENTS_SPECIFIC; ++i) {
		printf("%s (%s),", name, perf_event_hdr_specific[i]);
	}
}

void timer_finalize(timer_data_t* obj) {
	timer_finalize_specific(obj);
}
