#include "database/my_malloc.h"
#include "database/common.h"

#ifdef __NAUTILUS__
	#include <nautilus/libccompat.h>
	#define atexit(func)
#else
	#include <stdlib.h>
	#include <stdbool.h>
	#include <stdio.h>
	#include <assert.h>
	#include <string.h>
#endif

#ifdef REPLACE_MALLOC

	#warning Using custom malloc

	#ifndef REPLACE_MALLOC_DEFAULT_SIZE
		#define REPLACE_MALLOC_DEFAULT_SIZE (256 * 1024 * 1024)
	#endif

	void* allocation = NULL;
	void* unoccupied = NULL;
	size_t alloc_size = 0;

	void my_malloc_init(size_t alloc_size_) {
		atexit(my_malloc_deinit);
		alloc_size = alloc_size_;
		unoccupied = allocation = malloc(alloc_size);
	}

	void my_malloc_deinit() {
		if(allocation != NULL) {
			size_t usage = unoccupied - allocation;
			float usage_ratio = ((float) usage) / alloc_size;
			INFO("only used %ld / %lu = %.2f\n",
				 usage, alloc_size, usage_ratio);
			free(allocation);
			unoccupied = allocation = NULL;
		}
	}

	inline void* my_malloc(size_t size) {
		if(allocation == NULL) {
			my_malloc_init(REPLACE_MALLOC_DEFAULT_SIZE);
		}

		void* your_block = unoccupied;
		unoccupied += size;
		passert(0 <      unoccupied - allocation &&
				(size_t)(unoccupied - allocation) < alloc_size,
				"false: %ld < %lu\n", unoccupied - allocation, alloc_size);
		return your_block;
	}

	#pragma GCC diagnostic push
	#pragma GCC diagnostic ignored "-Wunused-parameter"
	inline void my_free(void* ptr) {}
	#pragma GCC diagnostic pop

#else

	#pragma GCC diagnostic push
	#pragma GCC diagnostic ignored "-Wunused-parameter"
	inline void my_malloc_init(size_t size) {}
	#pragma GCC diagnostic pop

	inline void my_malloc_deinit() {}

	inline void* my_malloc(size_t size) {
		return malloc(size);
	}

	inline void my_free(void* ptr) {
		free(ptr);
	}

#endif

#ifdef REPLACE_MEMCPY
	inline void* my_memcpy (void* destination, const void* source, size_t num) {
		for(size_t i = 0; i < num; ++i) {
			destination[i] = source[i];
		}
		return destination;
	}
#else
	inline void* my_memcpy (void* destination, const void* source, size_t num) {
		return memcpy(destination, source, num);
	}
#endif
