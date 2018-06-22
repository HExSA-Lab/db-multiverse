#ifndef __OPERATORS_H__
#define __OPERATORS_H__

#include <stdbool.h>
#include "database.h"

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

bool check_sorted(col_table_t *result, size_t col, size_t domain_size, col_table_t *copy);

#endif
