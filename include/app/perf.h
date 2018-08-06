#ifndef __PERF_H__
#define __PERF_H__

#ifdef __NAUTILUS__
	#include <nautilus/libccompat.h>
	#include <nautilus/pmc.h>
	#define PERF_EVENTS_SPECIFIC 4
#else
	#include <stdint.h>
	#include <stdio.h>
	#include <assert.h>
	#define PERF_EVENTS_SPECIFIC 4
#endif

typedef struct {
	volatile uint32_t start_lo;
	volatile uint32_t start_hi;
	volatile uint32_t stop_lo;
	volatile uint32_t stop_hi;

	uint64_t perf_event_start[PERF_EVENTS_SPECIFIC];
	uint64_t perf_event_stop [PERF_EVENTS_SPECIFIC];

	#ifdef __NAUTILUS__
		perf_event_t* perf_event_nautk[PERF_EVENTS_SPECIFIC];
	#else
		int perf_event_fds_linux[PERF_EVENTS_SPECIFIC];
	#endif

} timer_data_t;

#ifdef __NAUTILUS__
	#include <app/perf_nautk.h>
#else
	#include <app/perf_linux.h>
#endif

void timer_initialize(timer_data_t* obj);
void timer_print(timer_data_t* obj);
void timer_print_header(char* name);
void timer_finalize(timer_data_t* obj);

static inline void timer_start(timer_data_t* obj) {
	for(unsigned int i = 0; i < PERF_EVENTS_SPECIFIC; ++i) {
		obj->perf_event_start[i] = timer_read_specific(obj, i);
	}
	// do the bare minimum
	// and be inlined,
	// so this does not affect performance.
	// For best results, timer_data_t should be stack-allocated.
	__asm volatile ("rdtsc\n"
	                : "=a" (obj->start_lo), "=d" (obj->start_hi)
	                );
}
static inline void timer_stop(timer_data_t* obj) {
	__asm volatile ("rdtsc"
	                : "=a" (obj->stop_lo), "=d" (obj->stop_hi)
	                );
	for(unsigned int i = 0; i < PERF_EVENTS_SPECIFIC; ++i) {
		obj->perf_event_stop[i] = timer_read_specific(obj, i);
	}
}

static inline void timer_stop_print(timer_data_t* obj) {
	timer_stop(obj);
	timer_print(obj);
}

#endif
