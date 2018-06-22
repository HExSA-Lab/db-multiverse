#include "timing.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
int      counter_init(cntr_type_t type, void * arg) { return 0; }
#pragma GCC diagnostic pop
void     counter_deinit() {}
void     counter_start() {}
void     counter_stop() {}
void     counter_reset() {}
void     counter_report() {}
uint64_t counter_get() { return 0; }
const char * counter_get_name() { return "none"; }
