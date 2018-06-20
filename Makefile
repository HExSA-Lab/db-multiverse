CC:=gcc
INCLUDES:= -I. -Iinclude
WARNINGS:= -Wall -Wextra
CFLAGS_DBG:=$(INCLUDES) $(WARNINGS) -O0 -g
CFLAGS_OPT:=$(INCLUDES) $(WARNINGS) -O3 -msse2 -ffast-math #-funroll-loops
LIBS:= -lperf
LDFLAGS:= -L$(PWD)/lib $(LIBS) -Wl,-rpath=$(PWD)/lib
SOURCES:=$(shell ls *.c)

all: main

run_data: main
	./main -t 6 | tee /tmp/output | grep 'actual sort' | grep -oP '\d*' | tail -n +2 | ./mean && cat /tmp/output

sorting_runtimes.png: sorting_runtimes.py README.md
	./sorting_runtimes.py

run: main
	./main

debug: main_debug
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
	@rm -f main main_debug *.o *.o_debug sorting_runtimes.png

distclean: clean
	@rm -f  lib/libperf.so.0 lib/libperf.so

.PHONY: clean distclean run debug run_data sorting_runtime.png libs
