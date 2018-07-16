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
#endif

#ifdef REPLACE_MALLOC

	#ifndef REPLACE_MALLOC_DEFAULT_SIZE
		#define REPLACE_MALLOC_DEFAULS_SIZE (256 * 1024 * 1024)
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
			INFO("only used %ld / %lu = %f\n", unoccupied - allocation, alloc_size, ((float) (unoccupied - allocation)) / alloc_size);
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
		if(unoccupied > allocation + alloc_size) {
			printf("tried to allocate %ld > %lu\n", (unoccupied - allocation), alloc_size);
			exit(1);
		}
		return your_block;
	}

	#pragma GCC diagnostic push
	#pragma GCC diagnostic ignored "-Wunused-parameter"
	inline void my_free(void* ptr) {}
	#pragma GCC diagnostic pop

#else

	#error You forgot to replace malloc

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

