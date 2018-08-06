#ifndef RAND_H
#define RAND_H

#ifdef __NAUTILUS__
	#include <nautilus/libccompat.h>
#else
	#include <stdint.h>
#endif

typedef uint32_t randval_t;
typedef uint32_t seed_t;

void rand_seed(seed_t seed);
seed_t rand_state();
randval_t rand_next(unsigned int domain_size);

#endif
