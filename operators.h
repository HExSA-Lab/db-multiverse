#ifndef __OPERATORS_H__
#define __OPERATORS_H__

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

// function declarations
col_table_t *projection(col_table_t *t, size_t *pos, size_t num_proj);
col_table_t *selection_const (col_table_t *t, size_t col, val_t val);
col_table_t *basic_rowise_selection_const (col_table_t *t, size_t col, val_t val);
col_table_t *scatter_gather_selection_const (col_table_t *t, size_t col, val_t val);
void mergesort_helper(col_table_t *in, size_t start, size_t stop, col_table_t *out, size_t col);
col_table_t * mergesort(col_table_t *in, size_t col);

// information about operator implementations
extern op_implementation_info_t impl_infos[];
extern op_implementation_t default_impls[];
extern const char * op_names[];

#endif
