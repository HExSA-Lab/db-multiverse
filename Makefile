CC:=gcc
LDFLAGS:= 
CFLAGS:=-O3 -Wall -g -msse2 -ffast-math
# -funroll-loops 

all: sample_op

sample_op: sample_op.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $<

clean:
	@rm -f sample_op

.PHONY: clean
