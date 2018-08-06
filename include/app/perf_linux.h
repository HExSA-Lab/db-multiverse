#include <linux/perf_event.h>
#include <unistd.h>
#include <stdlib.h>

typedef struct {
	uint64_t value; /* The value of the event */
	//uint64_t time_enabled; /* if PERF_FORMAT_TOTAL_TIME_ENABLED */
	//uint64_t time_running; /* if PERF_FORMAT_TOTAL_TIME_RUNNING */
	//uint64_t id; /* if PERF_FORMAT_ID */
} read_format_t;


#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
static inline uint64_t timer_read_specific(timer_data_t* obj, unsigned int i) {
	read_format_t counter_result;
	int rc = read(obj->perf_event_fds_linux[i], &counter_result, sizeof(read_format_t));

	#ifndef DNDEBUG
		if(rc != sizeof(read_format_t)) {
			printf("perf counter %u failed (fd = %d, rc = %d)\n", i, obj->perf_event_fds_linux[i], rc);
			perror("read(perf_counter_fd)");
			exit(EXIT_FAILURE);
		}
	#endif

	return counter_result.value;
}
#pragma GCC diagnostic pop
