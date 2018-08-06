#include "app/database/rand.h"

// rand() is very slow in Nautilus, so I roll my own.
// See https://en.wikipedia.org/wiki/Linear_congruential_generator#Parameters_in_common_use
seed_t x = 0;
seed_t a = 1664525;
seed_t c = 1013904223;
void rand_seed(uint32_t seed) {
	x = seed;
}

uint32_t rand_state() {
	return x;
}

randval_t rand_next(unsigned int domain_size) {
	x = (a * x + c);
	// overflow is modulus
	return (randval_t) (x % domain_size);
}
