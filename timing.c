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
 * This gives us a unified way of counting metrics for an operator experiment.
 *
 */
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <sys/time.h>

#include <libperf.h>

#include <common.h>
#include <timing.h>

static cntr_info_t * cntr_data = NULL;

static const char * name_map[] = {
    "gettimeofday",
    "clock_gettime",
    "perf counter",
};

static void * 
gtod_init (__attribute__((unused)) void * arg)
{
    struct stateless_data * data = NEWZ(struct stateless_data);
    MALLOC_CHECK(data, "gtod timing data");
    return data;
}


static void
gtod_start (void * priv)
{
    struct stateless_data * data = (struct stateless_data*)priv;
    struct timeval tv;

    gettimeofday(&tv, NULL);

    data->start = 1000000 * tv.tv_sec + tv.tv_usec;
}


static void
gtod_stop (void * priv)
{
    struct stateless_data * data = (struct stateless_data*)priv;
    struct timeval tv;

    gettimeofday(&tv, NULL);

    data->end = 1000000 * tv.tv_sec + tv.tv_usec;
}


static void
gtod_reset (void * priv)
{
    struct stateless_data * data = (struct stateless_data*)priv;
    data->start = 0;
    data->end   = 0;
}


static uint64_t
gtod_get (void * priv)
{
    struct stateless_data * data = (struct stateless_data*)priv;

    if (cntr_data->state == COUNTER_STOPPED) {
        return data->end - data->start;
    } else if (cntr_data->state == COUNTER_STARTED) { // nested call
        struct timeval tv;
        gettimeofday(&tv, NULL);
        uint64_t t = tv.tv_sec * 1000000 + tv.tv_usec;
        return t - data->start;
    } 

    return 0;
} 


static void
gtod_report (__attribute__((unused)) void * priv)
{
    //struct stateless_data * data = (struct stateless_data*)priv;
    ERROR("%s not implemented\n", __func__);
    exit(EXIT_FAILURE);
}



static void
gtod_deinit (void * priv)
{
    struct stateless_data * data = (struct stateless_data*)priv;
    free(data);
}


static struct cntr_ops gtod_cntr_ops = {
    .init   = gtod_init,
    .start  = gtod_start,
    .stop   = gtod_stop,
    .reset  = gtod_reset,
    .get    = gtod_get,
    .report = gtod_report,
    .deinit = gtod_deinit,
}; 


static void * 
cgt_init (__attribute__((unused)) void * arg)
{
    struct stateless_data * data = NEWZ(struct stateless_data);
    MALLOC_CHECK(data, "clock_gettime timing data");
    return data;
}


static void
cgt_start (void * priv)
{
    struct stateless_data * data = (struct stateless_data*)priv;
    struct timespec ts;

    clock_gettime(CLOCK_REALTIME, &ts);

    data->start = 1000000000 * ts.tv_sec + ts.tv_nsec;
}


static void
cgt_stop (void * priv)
{
    struct stateless_data * data = (struct stateless_data*)priv;
    struct timespec ts;

    clock_gettime(CLOCK_REALTIME, &ts);

    data->end = 1000000000 * ts.tv_sec + ts.tv_nsec;
}


static void
cgt_reset (void * priv)
{
    struct stateless_data * data = (struct stateless_data*)priv;
    data->start = 0;
    data->end   = 0;
}


static uint64_t
cgt_get (void * priv)
{
    struct stateless_data * data = (struct stateless_data*)priv;

    if (cntr_data->state == COUNTER_STOPPED) {
        return data->end - data->start;
    } else if (cntr_data->state == COUNTER_STARTED) { // nested call
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        uint64_t t = ts.tv_sec * 1000000000 + ts.tv_nsec;
        return t - data->start;
    } 

    return 0;
}

static void
cgt_report (__attribute__((unused)) void * priv)
{
    //struct stateless_data * data = (struct stateless_data*)priv;
    ERROR("%s not implemented\n", __func__);
    exit(EXIT_FAILURE);
}


static void
cgt_deinit (void * priv)
{
    struct stateless_data * data = (struct stateless_data*)priv;
    free(data);
}


static struct cntr_ops cgt_cntr_ops = {
    .init   = cgt_init,
    .start  = cgt_start,
    .stop   = cgt_stop,
    .reset  = cgt_reset,
    .get    = cgt_get,
    .report = cgt_report,
    .deinit = cgt_deinit,
};


static void * 
perf_init (void * arg)
{
    struct perf_arg * pa = (struct perf_arg*)arg;
    struct perf_data * pd = NEWZ(struct perf_data); 
    MALLOC_CHECK(pd, "perf data");

    switch (pa->type) {
        case PERF_CTX_SWITCH:
            pd->lp_type = LIBPERF_COUNT_SW_CONTEXT_SWITCHES;
            break;
        case PERF_PAGE_FAULTS:
            pd->lp_type = LIBPERF_COUNT_SW_PAGE_FAULTS;
            break;
        case PERF_CPU_CYCLES:
            pd->lp_type = LIBPERF_COUNT_HW_CPU_CYCLES;
            break;
        case PERF_INSTR_RETIRED:
            pd->lp_type = LIBPERF_COUNT_HW_INSTRUCTIONS;
            break;
        case PERF_BRANCH_MISSES:
            pd->lp_type = LIBPERF_COUNT_HW_BRANCH_MISSES;
            break;
        case PERF_CPU_MIGRATIONS:
            pd->lp_type = LIBPERF_COUNT_SW_CPU_MIGRATIONS;
            break;
        case PERF_CACHE_MISSES:
            pd->lp_type = LIBPERF_COUNT_HW_CACHE_MISSES;
            break;
        case PERF_DTLB_LOAD_MISS:
            pd->lp_type = LIBPERF_COUNT_HW_CACHE_DTLB_LOADS_MISSES;
            break;
        case PERF_ITLB_LOAD_MISS:
            pd->lp_type = LIBPERF_COUNT_HW_CACHE_ITLB_LOADS_MISSES;
            break;
        default:
            ERROR("Unknown perf type (%d)\n", pa->type);
            exit(EXIT_FAILURE);
    }

    pd->lp_data = libperf_initialize(-1, -1);

    if (!pd->lp_data) {
        ERROR("Could not initialize libperf\n");
        exit(EXIT_FAILURE);
    }

    return (void*)pd;
}


static void
perf_start (void * priv)
{
    struct perf_data * pd = (struct perf_data*)priv;
    libperf_enablecounter(pd->lp_data, pd->lp_type);
}


static void
perf_stop (void * priv)
{
    struct perf_data * pd = (struct perf_data*)priv;
    libperf_disablecounter(pd->lp_data, pd->lp_type);
}


static void
perf_reset (__attribute__((unused)) void * priv)
{
    // nothing to do
}


static uint64_t
perf_get (void * priv)
{
    struct perf_data * pd = (struct perf_data*)priv;
    return libperf_readcounter(pd->lp_data, pd->lp_type);
}

static void 
perf_report (__attribute__((unused)) void * priv)
{
    //struct perf_data * pd = (struct perf_data*)priv;
    ERROR("%s not implemented\n", __func__);
    exit(EXIT_FAILURE);
}


static void
perf_deinit (void * priv)
{
    struct perf_data * pd = (struct perf_data*)priv;
    libperf_finalize(pd->lp_data, 0);
    free(pd);
}


static struct cntr_ops perf_cntr_ops = {
    .init   = perf_init,
    .start  = perf_start,
    .stop   = perf_stop,
    .reset  = perf_reset,
    .get    = perf_get,
    .report = perf_report,
    .deinit = perf_deinit,
};


static cntr_ops_t * op_map[] = {
    &gtod_cntr_ops,
    &cgt_cntr_ops,
    &perf_cntr_ops,
};


int
counter_init (cntr_type_t type, void * arg)
{
    cntr_data = NEWZ(cntr_info_t);
    MALLOC_CHECK_INT(cntr_data, "counter data");

    cntr_data->type  = type;
    cntr_data->name  = name_map[type];
    cntr_data->ops   = op_map[type];
    cntr_data->state = COUNTER_INIT;

    if (cntr_data->ops->init) {
        cntr_data->data = cntr_data->ops->init(arg);
    }

    return 0;
}


void
counter_deinit ()
{
    if (cntr_data->ops->deinit) {
        cntr_data->ops->deinit(cntr_data->data);
    }

    free(cntr_data);
    
    cntr_data = NULL;
}


void 
counter_start ()
{
    if (cntr_data->state == COUNTER_STARTED) {
        return;
    }

    cntr_data->state = COUNTER_STARTED;

    if (cntr_data->ops->start) {
        cntr_data->ops->start(cntr_data->data);
    } else {
        ERROR("Could not start counter\n");
        exit(EXIT_FAILURE);
    }
}


void
counter_stop ()
{
    if (cntr_data->state == COUNTER_STOPPED) {
        return;
    }

    if (cntr_data->ops->stop) {
        cntr_data->ops->stop(cntr_data->data);
    } else {
        ERROR("Could not stop counter\n");
        exit(EXIT_FAILURE);
    }

    cntr_data->state = COUNTER_STOPPED;
}


uint64_t
counter_get ()
{
    if (cntr_data->ops->get) {
        return cntr_data->ops->get(cntr_data->data);
    } else {
        ERROR("Could not get counter\n");
        exit(EXIT_FAILURE);
    }
}


const char * 
counter_get_name ()
{
    return cntr_data->name;
}


void
counter_reset ()
{
    if (cntr_data->ops->reset) {
        cntr_data->ops->reset(cntr_data->data);
    } else {
        ERROR("Could not reset counter\n");
        exit(EXIT_FAILURE);
    }

    cntr_data->state = COUNTER_INIT;
}
