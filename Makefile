CC:=gcc
INCLUDES:= -I. -Iinclude
WARNINGS:= -Wall -Wextra -Wshadow
CFLAGS_DBG:=-O0 -g
CFLAGS_OPT:=-O3 -msse2 -ffast-math -funroll-loops
LIBS:= -lperf
LDFLAGS:= -L$(PWD)/lib $(LIBS) -Wl,-rpath=$(PWD)/lib
SOURCES:=$(shell ls *.c)

# change CFLAGS_OPT to CFLAGS_DBG for debug
CFLAGS:=$(CFLAGS_OPT) $(INCLUDES) $(WARNINGS)

all: main

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

main: $(SOURCES:.c=.o) libs
	$(CC) $(LDFLAGS) -o $@ $(SOURCES:.c=.o)

libs: lib/libperf.so.0 lib/libperf.so

lib/libperf.so: lib/libperf.so.0.0.0
	ln -s $(PWD)/lib/libperf.so.0.0.0 lib/libperf.so

lib/libperf.so.0: lib/libperf.so.0.0.0
	ln -s $(PWD)/lib/libperf.so.0.0.0 lib/libperf.so.0

clean:
	@rm -f main *.o

distclean:
	@rm -f main *.o lib/libperf.so.0 lib/libperf.so

.PHONY: clean distclean
