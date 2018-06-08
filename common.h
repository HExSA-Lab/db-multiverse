#ifndef __COMMON_H__
#define __COMMON_H__
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
 *                     Boris Glavic <bglavic@iit.edu>
 *
 */

#define INFO(fmt, args...)  printf("DB-MV: " fmt, ##args)
#define DEBUG(fmt, args...) fprintf(stderr, "DB-MB DBG: " fmt, ##args)
#define ERROR(fmt, args...) fprintf(stderr, "DB-MB ERR: " fmt, ##args)

#define NEW(typ)  ((typ *) malloc(sizeof(typ)))
#define NEWZ(typ) ((typ *) calloc(1, sizeof(typ)))
#define NEWA(typ,size) ((typ *) malloc(sizeof(typ) * size))
#define NEWPA(typ,size) ((typ **) malloc(sizeof(typ*) * size))

#define MALLOC_CHECK(pointer, mes) \
	do { \
		if (!(pointer)) { \
			ERROR("Could not allocate " #pointer  " (%s) in %s:%u\n", mes, __FUNCTION__, __LINE__); \
        	return NULL; \
		} \
	} while(0)

#define MALLOC_CHECK_VOID(pointer, mes) \
	do { \
		if (!(pointer)) { \
			ERROR("Could not allocate " #pointer  " (%s) in %s:%u\n", mes, __FUNCTION__, __LINE__); \
        	return; \
		} \
	} while(0)

#define MALLOC_CHECK_INT(pointer, mes) \
	do { \
		if (!(pointer)) { \
			ERROR("Could not allocate " #pointer  " (%s) in %s:%u\n", mes, __FUNCTION__, __LINE__); \
        	return -1; \
		} \
	} while(0)

#define MALLOC_CHECK_NO_MES(pointer) MALLOC_CHECK(pointer,"")

#define VERSION_STRING "0.0.1"

#define DEFAULT_TRIALS   1
#define DEFAULT_THROWOUT 0
#define DEFAULT_EXP_STR "scan"
#define DEFAULT_EXP      EXP_SCAN

#endif