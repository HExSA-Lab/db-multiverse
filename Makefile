CC:=gcc
WFLAGS:= -Wall -Wextra
IFLAGS:=-I./include/

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
CFLAGS_DBG:=$(IFLAGS) $(WFLAGS) -Og -g
CFLAGS_OPT:=$(IFLAGS) $(WFLAGS) $(CFLAGS_NAUT) -DNDEBUG
SOURCES:=$(shell find src/ -name '*.c' -printf '%P\n')
OBJECTS:=$(addprefix build/,$(SOURCES:.c=.o))
OBJECTS_DBG:=$(addprefix build/,$(SOURCES:.c=.o_debug))

all: main

run: main
	./main

run_debug: main_debug
	gdb -q main_debug -ex r

run_small: main
	./main --chunksize 3 --numchunks 3 --throwout 0 --trials 1

run_small_debug: main_debug
	gdb -q main_debug -ex 'r --chunksize 3000 --numchunks 10 --throwout 0 --trials 1'

run_small_memcheck: main_debug
	valgrind --leak-check=full --show-leak-kinds=all ./main_debug --chunksize 3000 --numchunks 10 --throwout 0 --trials 1

build/%.o_debug: src/%.c
	$(CC) $(CFLAGS_DBG) -c -o $@ $<

build/%.o: src/%.c
	$(CC) $(CFLAGS_OPT) -c -o $@ $<

main_debug: $(OBJECTS_DEBUG)
	$(CC) $(LDFLAGS) -o $@ $^

main: $(OBJECTS)
	$(CC) $(LDFLAGS) -o $@ $^

libs: lib/libperf.so.0 lib/libperf.so

lib/libperf.so: lib/libperf.so.0.0.0
	ln -s $(PWD)/lib/libperf.so.0.0.0 lib/libperf.so

lib/libperf.so.0: lib/libperf.so.0.0.0
	ln -s $(PWD)/lib/libperf.so.0.0.0 lib/libperf.so.0

clean:
	@rm -rf main main_debug build
	mkdir -p build/database

distclean: clean
	@rm -f  lib/libperf.so.0 lib/libperf.so

.PHONY: clean distclean run debug run_data sorting_runtime.png libs
