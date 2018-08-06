#ifdef __NAUTILUS__
	#include <nautilus/libccompat.h>
#else
 	#include <stdio.h>
	#include <string.h>
	#include <assert.h>
	#include <stdint.h>
#endif

#include "app/database/bitvec.h"

#define BITS_PER_BYTE ((unsigned long) 8)
#define BITS_PER_UNIT ((unsigned long) (sizeof(bit_unit_t) * BITS_PER_BYTE))
#define BYTE_PRINTF "0x%02hhx"
#define INT_PRINTF "0x%08x"
#define UNIT_PRINTF "0x%016lx"
#define BIT_MASK_ONE 0xFFFFFFFFFFFFFFFF

void bv_init(bit_vec_t* bv, size_t n_bits) {
	// allocate a not-lesser multiple of "BITS_PER_UNIT / BITS_PER_BYTE"
	bv->data = my_malloc((n_bits + BITS_PER_UNIT) / BITS_PER_UNIT * BITS_PER_BYTE);
	bv->n_bits = n_bits;
}

void bv_free(bit_vec_t* bv) {
	my_free(bv->data);
}

void bv_reset(bit_vec_t* bv) {
	memset(bv->data, 0, (bv->n_bits + BITS_PER_UNIT) / BITS_PER_UNIT * BITS_PER_BYTE);
	/*
	bit_unit_t* stop = bv->data + (bv->n_bits + BITS_PER_UNIT) / BITS_PER_UNIT;
	for(bit_unit_t* cur = bv->data; cur < stop; ++cur) {
		*cur = 0;
	}
	*/
}

/*
// Use bv_iter_get to get/set
// This is what it *would* be though.
bit_t bv_get(bit_vec_t* bv, size_t bit_idx) {
	assert(bit_idx < bv->n_bits);
	return bv[bit_idx / BITS_PER_UNIT] & (1 << (bit_idx % BITS_PER_UNIT));
}
*/

void bv_print(bit_vec_t* bv) {
	bit_vec_iter_t it;
	bv_iter_init(&it, bv);
	for(; bv_iter_has_next(&it); bv_iter_next(&it)) {
		printf("%d", bv_iter_get(&it));
	}
	printf("\n");
}

void bv_debug_print(bit_vec_t* bv) {
	printf("bit_vec_t from %p:%p (%zu bits)\n",
	       bv->data, ((char*) bv->data) + (bv->n_bits + BITS_PER_UNIT) / BITS_PER_BYTE, bv->n_bits);
}

void bv_iter_init(bit_vec_iter_t* it, bit_vec_t* bv) {
	it->bit_unit = bv->data;
	it->n_bits_left = bv->n_bits;
	it->bit_mask = 1;
	it->bit_vec = bv;
}

static inline unsigned mask_value(bit_unit_t unit) {
	// https://stackoverflow.com/questions/11376288/fast-computing-of-log2-for-64-bit-integers#11376759
	return BITS_PER_UNIT - __builtin_clzl(unit) - 1;
}

void bv_iter_debug_print(bit_vec_iter_t* it) {
	printf("bit_vec_iter_t from %p.%u (" UNIT_PRINTF ") for %zu bits\n",
	       it->bit_unit, mask_value(it->bit_mask), it->bit_mask, it->n_bits_left);
}

#define CEIL_DIV(a, b) (((a) + (b) - 1) / (b))

void bv_iter_set_rest(bit_vec_iter_t* it, bit_t val) {
	if(val) {
		size_t bits_left = it->n_bits_left + mask_value(it->bit_mask);
		*it->bit_unit |= (BIT_MASK_ONE * it->bit_mask) * val;
		if(bits_left > BITS_PER_UNIT) {
			size_t rest = CEIL_DIV(bits_left - BITS_PER_UNIT, (signed long) BITS_PER_BYTE);
			//printf("setting rest; memset(%p, " INT_PRINTF ", %zd)\n",
			//	   it->bit_unit + 1, (int) (BIT_MASK_ONE * val), rest);
			memset(it->bit_unit + 1, (int) (BIT_MASK_ONE * val), rest);
		}
	}
}

void bv_iter_skip(bit_vec_iter_t* it, unsigned long n_bits) {
	unsigned long n_units = n_bits / BITS_PER_UNIT;
	unsigned long n_remainder_bits = n_bits % BITS_PER_UNIT;
	it->n_bits_left += n_units * BITS_PER_UNIT;
	it->bit_unit += n_units;
	for(unsigned long i = 0; i < n_remainder_bits; ++i) {
		bv_iter_next(it);
	}
}

void bv_test() {
	my_malloc_init(1 << 28);
	assert(sizeof(bit_unit_t) * BITS_PER_BYTE == BITS_PER_UNIT);

	for(uint8_t i = 0; i < BITS_PER_UNIT; ++i) {
		assert(mask_value(1UL << i) == i);
	}

	assert(~(bit_mask_t) BIT_MASK_ONE == 0); // BIT_MASK_ONE has the right number of F's

	for(size_t total_n_bits = 1; total_n_bits < 129; ++total_n_bits) {
		for(uint8_t bit = 0; bit < 2; ++bit) {
			for(size_t skip_n_bits = 0; skip_n_bits < total_n_bits; ++skip_n_bits) {
				for(size_t set_n_bits = skip_n_bits; set_n_bits < total_n_bits; ++set_n_bits) {
					//fprintf(stderr, "%zu %zu %zu %u\n",
					//       skip_n_bits, set_n_bits, total_n_bits, bit);
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
						printf("all bv[0:%zu] should be 0, all bv[%zu:%zu] should be %u, all bv[%zu:%zu] should be %u\n",
						       skip_n_bits, skip_n_bits, set_n_bits, bit, set_n_bits, total_n_bits, !bit);
						printf("bv = "); bv_print(&bv);
						printf("bv = {data: %p, n_bits: %zu}\n", bv.data, bv.n_bits);
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
