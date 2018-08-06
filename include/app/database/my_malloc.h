#ifndef MY_MALLOC_H

#ifdef __NAUTILUS__
	#include <nautilus/libccompat.h>
#else
	#include <stdlib.h>
#endif

void my_malloc_init(size_t size);
void my_malloc_deinit();
void* my_malloc(size_t size);
void my_free(void* ptr);
void my_malloc_print();

#endif
