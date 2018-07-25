#ifndef BITVEC_H
#define BITVEC_H

#include <stdbool.h>
#include "database/my_malloc.h"

typedef unsigned long bit_unit_t;
typedef bit_unit_t bit_mask_t;
typedef struct {
	size_t n_bits;
	bit_unit_t* data;
} bit_vec_t;
typedef bool bit_t;
typedef struct {
	bit_unit_t* bit_unit;
	size_t n_bits;
	bit_mask_t bit_mask;
} bit_vec_iter_t;

void bv_init(bit_vec_t*, size_t n_bits);
void bv_free(bit_vec_t* bv);
void bv_reset(bit_vec_t* bv);
bit_t bv_get(bit_vec_t* bv, size_t idx);
void bv_print(bit_vec_t* bv);
void bv_iter_init(bit_vec_iter_t* it, bit_vec_t* bv);
void bv_iter_set_rest(bit_vec_iter_t* bv, bit_t val);
void bv_iter_skip(bit_vec_iter_t* it, unsigned long n_bits);

static inline void bv_iter_next(bit_vec_iter_t* it) {
	--it->n_bits;
	it->bit_mask <<= 1;
	if(__builtin_expect(it->bit_mask == 0, 0)) {
		it->bit_mask = 1;
		++it->bit_unit;
	}
}

static inline bit_t bv_iter_get(bit_vec_iter_t* it) {
	return *it->bit_unit & it->bit_mask;
}

static inline void bv_iter_set(bit_vec_iter_t* it, bit_t val) {
	// note bv must be reset first
	*it->bit_unit |= it->bit_mask * val;
}

static inline bool bv_iter_has_next(bit_vec_iter_t* it) {
	return it->n_bits > 0;
}

#endif
