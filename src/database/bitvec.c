#ifdef __NAUTILUS__
	#include <nautilus/libccompat.h>
#else
 	#include <stdio.h>
	#include <string.h>
#endif

#include "database/bitvec.h"

#define BITS_PER_UNIT sizeof(bit_unit_t) * 8
#define BIT_MASK_ONE 0xFFFFFFFF

void bv_init(bit_vec_t* bv, size_t n_bits) {
	bv->data = my_malloc((n_bits / 8) + 1);
	bv->n_bits = n_bits;
}

void bv_free(bit_vec_t* bv) {
	my_free(bv->data);
}

void bv_reset(bit_vec_t* bv) {
	memset(bv->data, 0, (bv->n_bits / 8) + 1);
}

/*
// Use bv_iter_get to get/set
// This is what it *would* be though.
bit_t bv_get(bit_vec_t* bv, size_t bit_idx) {
	assert(bit_idx < bv->n_bits);
	return bv[bit_idx / BITS_PER_UNIT] & (1 << (bit_idx % BITS_PER_UNIT));
}
*/

void bv_iter_init(bit_vec_iter_t* it, bit_vec_t* bv) {
	it->bit_unit = bv->data;
	it->n_bits = bv->n_bits;
	it->bit_mask = 1;
}

void bv_iter_set_rest(bit_vec_iter_t* it, bit_t val) {
	*it->bit_unit |= (BIT_MASK_ONE * it->bit_mask) * val;
	if(val) {
		memset(it->bit_unit + 1, (int) (BIT_MASK_ONE * val), it->n_bits / 8);
	}
}

void bv_iter_skip(bit_vec_iter_t* it, unsigned long n_bits) {
	unsigned long n_bits_q8 = n_bits / 8;
	unsigned long n_bits_r8 = n_bits % 8;
	it->n_bits += n_bits_q8 * 8;
	it->bit_unit += n_bits_q8;
	for(unsigned long i = 0; i < n_bits_r8; ++i) {
		bv_iter_next(it);
	}
}

void bv_print(bit_vec_t* bv) {
	bit_vec_iter_t it;
	bv_iter_init(&it, bv);
	for(; bv_iter_has_next(&it); bv_iter_next(&it)) {
		printf("%d", bv_iter_get(&it));
	}
	printf("\n");
}
