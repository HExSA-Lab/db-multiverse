# Targets:
# make linux version
#     make main
#     ./main
#
# run linux version:
#     make run
#
# runs a smaller debug-test case with assertions on
#     make run_small
#
# run in GDB
#     make run_debug
#
# check for memory leaks
#     make run_memcheck

CC:=gcc
WFLAGS:= -Wall -Wextra
IFLAGS:=-I./include/
CVERSION := -std=gnu99
include src/app/macros.mk
# from nautilus/Makefile
CFLAGS_NAUT := -O2 \
               -fno-omit-frame-pointer \
               -fno-stack-protector \
               -fno-strict-aliasing \
               -fno-strict-overflow \
               -fno-delete-null-pointer-checks \
               -mno-red-zone \
               -mno-sse2 \
               -mcmodel=large \
               -fno-common \
               -Wstrict-overflow=5 \
               -fgnu89-inline \
               -g \
               -m64
CFLAGS_OPT  := $(CVERSION) $(IFLAGS) $(WFLAGS) $(CFLAGS_NAUT)
# that line ^ should have no extra flags.
# -std=* -I* and -W* won't affect the outputed code, so they are fine.
# All flags which do (eg. -f* -O* -m*) should be a part of CFLAGS_NAUT
CFLAGS_DEBUG:= $(CVERSION) $(IFLAGS) $(WFLAGS) -fno-inline -static -Og -g -DVERBOSE -DSMALL
# -DREPLACE_MALLOC

SOURCES:=$(shell find src/ -name '*.c' -printf '%P\n')
OBJECTS:=$(addprefix build/,$(SOURCES:.c=.o))
OBJECTS_DBG:=$(addprefix build/,$(SOURCES:.c=.o_debug))

all: main

run: main
	./main
.PHONY: run

run_debug: main_debug
	gdb -q main_debug -ex r
.PHONY: run_debug

run_small: main
	./main --chunksize 3 --numchunks 3 --throwout 0 --trials 1
.PHONY: run_small

run_memcheck: main_debug
	valgrind --leak-check=full --show-leak-kinds=all ./main_debug --chunksize 3000 --numchunks 10 --throwout 0 --trials 1
.PHONY: run_memcheck

build/%.o_debug: src/%.c
	mkdir -p $(shell dirname $@)
	$(CC) $(CFLAGS_DBG) -c -o $@ $<

build/%.o: src/%.c
	mkdir -p $(shell dirname $@)
	$(CC) $(CFLAGS_OPT) -c -o $@ $<

main_debug: $(OBJECTS_DEBUG)
	$(CC) $(LDFLAGS) -o $@ $^

main: $(OBJECTS)
	$(CC) $(LDFLAGS) -o $@ $^

clean:
	rm -f main main_debug $(shell find . -name '*.o' -o -name '*.cmd')
	rm -rf build/

.PHONY: clean
