CC:=gcc
CFLAGS:=-O0 -Wall -g -I. -Iinclude
LIBS:= -lperf
LDFLAGS:= -L$(PWD)/lib $(LIBS) -Wl,-rpath=$(PWD)/lib
#CFLAGS:=-O3 -Wall -g -msse2 -ffast-math
# -funroll-loops 

all: sample_op 

sample_op: sample_op.o timing.o libs
	$(CC) $(LDFLAGS) -o $@ sample_op.o timing.o

timing.o: timing.c timing.h common.h
	$(CC) $(CFLAGS) -c -o $@ $<

sample_op.o: sample_op.c timing.h common.h
	$(CC) $(CFLAGS) -c -o $@ $<

libs: lib/libperf.so.0 lib/libperf.so

lib/libperf.so: lib/libperf.so.0.0.0
	ln -s $(PWD)/lib/libperf.so.0.0.0 lib/libperf.so

lib/libperf.so.0: lib/libperf.so.0.0.0
	ln -s $(PWD)/lib/libperf.so.0.0.0 lib/libperf.so.0

clean:
	@rm -f sample_op *.o

distclean:
	@rm -f sample_op *.o lib/libperf.so.0 lib/libperf.so

.PHONY: clean distclean
