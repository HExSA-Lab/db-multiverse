CC:=gcc
LDFLAGS:= 
CFLAGS:=-O2 -Wall

all: sample_op

sample_op: sample_op.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $<

clean:
	@rm -f sample_op

.PHONY: clean
