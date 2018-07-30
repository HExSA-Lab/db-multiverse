#ifdef __NAUTILUS__
	#include <nautilus/libccompat.h>
#else
 	#include <stdio.h>
	#include <string.h>
	#include <assert.h>
	#include <stdint.h>
#endif

#include "database/bitvec.h"

#define BITS_PER_BYTE ((unsigned long) 8)
#define BITS_PER_UNIT ((unsigned long) (sizeof(bit_unit_t) * BITS_PER_BYTE))
#define BYTE_PRINTF "0x%02hhx"
#define INT_PRINTF "0x%08x"
#define UNIT_PRINTF "0x%016lx"
#define BIT_MASK_ONE 0xFFFFFFFFFFFFFFFF

void bv_init(bit_vec_t* bv, size_t n_bits) {
	bv->data = my_malloc((n_bits / BITS_PER_BYTE) + 1);
	bv->n_bits = n_bits;
}

void bv_free(bit_vec_t* bv) {
	my_free(bv->data);
}

void bv_reset(bit_vec_t* bv) {
	memset(bv->data, 0, (bv->n_bits / BITS_PER_BYTE) + 1);
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
	if(val) {
		//printf("setting this unit; *it->bit_unit = *%p = " UNIT_PRINTF " | " UNIT_PRINTF " = " UNIT_PRINTF "\n",
		//       it->bit_unit, it->bit_unit[0], (BIT_MASK_ONE * it->bit_mask) * val,
		//       it->bit_unit[0] | (BIT_MASK_ONE * it->bit_mask) * val);
		*it->bit_unit |= (BIT_MASK_ONE * it->bit_mask) * val;
		//printf("setting rest; memset(%p, " INT_PRINTF ", %lu (= %lu / BITS_PER_BYTE + 1))\n",
		//       it->bit_unit + 1, (int) (BIT_MASK_ONE * val), (it->n_bits / BITS_PER_BYTE) + 1, it->n_bits);
		memset(it->bit_unit + 1, (int) (BIT_MASK_ONE * val), (it->n_bits / BITS_PER_BYTE) + 1);
	}
}

void bv_iter_skip(bit_vec_iter_t* it, unsigned long n_bits) {
	unsigned long n_units = n_bits / BITS_PER_UNIT;
	unsigned long n_remainder_bits = n_bits % BITS_PER_UNIT;
	it->n_bits += n_units * BITS_PER_UNIT;
	it->bit_unit += n_units;
	for(unsigned long i = 0; i < n_remainder_bits; ++i) {
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

void bv_test() {
	assert(sizeof(bit_unit_t) * BITS_PER_BYTE == BITS_PER_UNIT);
	for(size_t total_n_bits = 1; total_n_bits < 129; ++total_n_bits) {
		for(uint8_t bit = 0; bit < 2; ++bit) {
			for(size_t skip_n_bits = 0; skip_n_bits < total_n_bits; ++skip_n_bits) {
				for(size_t set_n_bits = skip_n_bits; set_n_bits < total_n_bits; ++set_n_bits) {
					// Init vector/iterator
					bit_vec_t bv;
					bv_init(&bv, total_n_bits);
					bv_reset(&bv);

					bit_vec_iter_t it;
					bv_iter_init(&it, &bv);

					// Set vector
					bv_iter_skip(&it, skip_n_bits);
					for(size_t i = skip_n_bits; i < set_n_bits; ++i, bv_iter_next(&it)) {
						bv_iter_set(&it, bit);
					}
					bv_iter_set_rest(&it, !bit);

					// Check vector
					bv_iter_init(&it, &bv);
					size_t errors = 0;
					size_t i = 0;
					for(; i < skip_n_bits; ++i, bv_iter_next(&it)) {
						errors |= bv_iter_get(&it) != 0  ;
					}
					for(; i < set_n_bits; ++i, bv_iter_next(&it)) {
						errors |= bv_iter_get(&it) != bit;
					}
					for(; i < total_n_bits; ++i, bv_iter_next(&it)) {
						errors |= bv_iter_get(&it) == bit;
					}

					// Report errors
					if(errors) {
						printf("all bv[0:%lu] should be 0, all bv[%lu:%lu] should be %u, all bv[%lu:%lu] should be %u\n",
						       skip_n_bits, skip_n_bits, set_n_bits, bit, set_n_bits, total_n_bits, !bit);
						printf("bv = "); bv_print(&bv);
						printf("bv = {data: %p, n_bits: %lu}\n", bv.data, bv.n_bits);
						printf("*bv.data = ");
						for(size_t i = 0; i < (bv.n_bits / BITS_PER_UNIT) + 1; ++i) {
							printf(UNIT_PRINTF " ", *(bv.data + i));
						}
						printf(" = ");
						for(size_t i = 0; i < (bv.n_bits / BITS_PER_BYTE) + 1; ++i) {
							printf(BYTE_PRINTF " ", *(((char*)bv.data) + i));
						}
						printf("\n");
						printf("it = {.bit_mask = " UNIT_PRINTF ", .bit_unit = *%p = " UNIT_PRINTF "}\n",
						       it.bit_mask, it.bit_unit, *it.bit_unit);
						abort();
					}

					// Clean up vector
					bv_free(&bv);
				}
			}
		}
	}
}
