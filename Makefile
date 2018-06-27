CC:=gcc
# INCLUDES:= -I. -Iinclude
WARNINGS:= -Wall -Wextra
CFLAGS_EXT:=-D__USER -std=c99
CFLAGS_DBG:=$(INCLUDES) $(WARNINGS) $(CFLAGS_EXT) -O0 -g
CFLAGS_OPT:=$(INCLUDES) $(WARNINGS) $(CFLAGS_EXT) -O3 -msse2 -ffast-math
#-funroll-loops
# LIBS:= -lperf
LDFLAGS:= -L$(PWD)/lib $(LIBS) -Wl,-rpath=$(PWD)/lib
SOURCES:=$(shell find . -name '*.c' | grep -v './advanced_timing.c')

all: main

run_data2: main
	./main

run_data: main
	./main -t 5 -k 1 | tee /tmp/output | grep 'actual sort' | grep -oP '\d*' | ./mean > /tmp/output2 && cat /tmp/output && cat /tmp/output2

sorting_runtimes.png: sorting_runtimes.py README.md
	./sorting_runtimes.py

run: main
	./main

run_small: main
	./main --chunksize 3 --numchunks 3 --throwout 0 --trials 1

run_small_debug: main_debug
	gdb -q main_debug -ex 'r --chunksize 3000 --numchunks 10 --throwout 0 --trials 1'

run_small_memcheck: main_debug
	valgrind --leak-check=full ./main_debug --chunksize 3000 --numchunks 10 --throwout 0 --trials 1

run_debug: main_debug
	gdb -q main_debug -ex r

%.o_debug: %.c
	$(CC) $(CFLAGS_DBG) -c -o $@ $<

%.o: %.c
	$(CC) $(CFLAGS_OPT) -c -o $@ $<

main_debug: $(SOURCES:.c=.o_debug) libs
	$(CC) $(LDFLAGS) -o $@ $(SOURCES:.c=.o_debug)

main: $(SOURCES:.c=.o) libs
	$(CC) $(LDFLAGS) -o $@ $(SOURCES:.c=.o)

libs: lib/libperf.so.0 lib/libperf.so

lib/libperf.so: lib/libperf.so.0.0.0
	ln -s $(PWD)/lib/libperf.so.0.0.0 lib/libperf.so

lib/libperf.so.0: lib/libperf.so.0.0.0
	ln -s $(PWD)/lib/libperf.so.0.0.0 lib/libperf.so.0

clean:
	@rm -f main main_debug *.o *.o_debug test/*.o test/*.o_debug sorting_runtimes.png

distclean: clean
	@rm -f  lib/libperf.so.0 lib/libperf.so

.PHONY: clean distclean run debug run_data sorting_runtime.png libs
