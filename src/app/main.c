#ifdef __NAUTILUS__
	#include <nautilus/libccompat.h>
#else
	#include <stdio.h>
	#include <stdint.h>
	#include <stdlib.h>
#endif

#include "app/test_array.h"
#include "app/test_deep_array.h"
#include "app/test_db.h"

void test() {
	test_array();
	test_deep_array();
	test_db();
	/* test_just_sort(8, 8, 4, 100); */
}

#ifdef __NAUTILUS__
void app_main() {
	printf("version thug_lyfe\n");
	printf("begin_data\n");
	test();
	printf("end_data\n");
}
#else
int main() {
	test();
	return 0;
}
#endif
