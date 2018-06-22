#ifndef __TIMING_H__
#define __TIMING_H__
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
 */
#ifdef __USER
#include <stdint.h>
#endif

#ifdef __NAUTILUS__
#include <nautilus/libccompat.h>
#endif

typedef uint64_t time_int;

typedef enum {
    COUNTER_TYPE_GTOD,  // gettimeofday()
    COUNTER_TYPE_CGT,   // clock_gettime()
    COUNTER_TYPE_PERF,  // perf counter
} cntr_type_t;


typedef enum {
    COUNTER_INIT,
    COUNTER_STARTED,
    COUNTER_STOPPED,
} cntr_state_t;

typedef enum {
    PERF_CTX_SWITCH,
    PERF_PAGE_FAULTS,
    PERF_CPU_CYCLES,
    PERF_INSTR_RETIRED,
    PERF_BRANCH_MISSES,
    PERF_CPU_MIGRATIONS,
    PERF_CACHE_MISSES,
    PERF_DTLB_LOAD_MISS,
    PERF_ITLB_LOAD_MISS,
    PERF_NUM_COUNTERS
} perf_type_t;

// abstract counter interface
typedef struct cntr_ops {
    void*    (*init)(void * arg);
    void     (*start)(void * priv);
    void     (*stop) (void * priv);
    void     (*reset)(void * priv);
    uint64_t (*get)(void * priv);
    void     (*report)(void * priv);
    void     (*deinit)(void * priv);
} cntr_ops_t;

typedef struct cntr_info {

    const char * name;
    cntr_type_t  type;
    cntr_state_t state;

    cntr_ops_t * ops;

    void * data; // implementation specific state

} cntr_info_t;

struct stateless_data {
    uint64_t start;
    uint64_t end;
};

typedef struct perf_arg {
    const char * name;
    uint8_t type;
} perf_arg_t;

struct perf_data {
    struct libperf_data * lp_data;
    uint8_t lp_type; // which counter

};

/* counter API */
int      counter_init(cntr_type_t type, void * arg);
void     counter_deinit();
void     counter_start();
void     counter_stop();
void     counter_reset();
void     counter_report();
uint64_t counter_get();
const char * counter_get_name();

// NOTE: this assumes a counter has already been started!
// For now, I want this to be done the same exact way in Nautilus as on Linux
/* #define TIMEIT(mes, code) \ */
/*      do { \ */
/*          uint64_t _s = counter_get(); \ */
/*          code; \ */
/*          uint64_t _e = counter_get(); \ */
/*          uint64_t _res = _e - _s; \ */
/*          printf("\t\tcounter - [%s]: %s is <%lu>\n", counter_get_name(), (mes), _res); \ */
/*      } while(0) */

#define TIMEIT(mes, code) \
    do {                                                               \
        uint64_t _s; rdtscll(_s);                                      \
        code;                                                          \
        uint64_t _e; rdtscll(_e);                                      \
        uint64_t _res = _e - _s;                                       \
        printf("\t\tcounter - [rdtscll]: %s is <%lu>\n", (mes), _res); \
    } while(0)

#define rdtscll(val)                                    \
    do {                                                \
        uint32_t hi, lo;                                \
        __asm volatile("rdtsc" : "=a" (lo), "=d" (hi)); \
        val = ((uint64_t)hi << 32) | lo;                \
    } while (0);
// see http://oliveryang.net/2015/09/pitfalls-of-TSC-usage/#2-why-using-tsc

#endif
