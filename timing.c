#ifdef __NAUTILUS__
	#include <nautilus/libccompat.h>
#else
	#include <stdlib.h>
	#include <stdio.h>
#endif
#include "app/timing.h"


#define max_timing_msgs 30000000
size_t n_timing_msgs;
char** timing_msgs_strs;
uint64_t* timing_msgs_time;
bool time_stuff = true;

void init_timing() {
	n_timing_msgs = 0;
	timing_msgs_strs = malloc(max_timing_msgs * sizeof(char*));
	timing_msgs_time = malloc(max_timing_msgs * sizeof(uint64_t));
}

void destroy_timing() {
	free(timing_msgs_strs);
	free(timing_msgs_time);
}

void enable_timing() {
	time_stuff = true;
}

void disable_timing() {
	time_stuff = false;
}

inline void record_time(char* mes, uint64_t s) {
	if(n_timing_msgs > max_timing_msgs) {
		printf("Allocate bigger buffer\n");
	}
		
	timing_msgs_strs[n_timing_msgs] = mes;
	timing_msgs_time[n_timing_msgs] = s;
	n_timing_msgs++;
}

void output_timing() {
	for(size_t i = 0; i < n_timing_msgs; ++i) {
		printf("%s, %lu\n", timing_msgs_strs[i], timing_msgs_time[i]);
	}
}
