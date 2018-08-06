#ifndef __OPERATORS_H__
#define __OPERATORS_H__

#ifdef __NAUTILUS__
	#include <nautilus/libccompat.h>
#else
	#include <stdbool.h>
#endif

#include "app/database/database.h"

typedef enum operator {
	SELECTION_CONST = 0,
	SELECTION_ATT,
	PROJECTION,
	SORT,
	NUM_OPS
} operator_t;

#define MAX_IMPL 5

typedef struct op_implementation_info {
	operator_t op;
	char *names[MAX_IMPL];
	op_implementation_t implementations[MAX_IMPL];
	size_t num_impls;
} op_implementation_info_t;

// information about operator implementations
extern op_implementation_info_t impl_infos[];
extern op_implementation_t default_impls[];
extern const char * op_names[];

/*
src/app/database/operators.c:1280:1: error: conflicting types for ‘check_sorted’
 check_sorted(col_table_t *result, size_t sort_col, size_t domain_size, col_table_t *copy) {
 ^~~~~~~~~~~~
In file included from src/app/database/operators.c:15:
include/app/database/operators.h:34:6: note: previous declaration of ‘check_sorted’ was here
 bool check_sorted(col_table_t *result, size_t col, size_t domain_size, col_table_t *copy);
      ^~~~~~~~~~~~
*/
//bool check_sorted(col_table_t *result, size_t col, size_t domain_size, col_table_t *copy);
col_table_t* countingmergesort(col_table_t *in, size_t col, size_t domain_size);
col_table_t* countingmergesort2(col_table_t *in, size_t col, size_t domain_size);

#endif
