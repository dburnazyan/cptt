CFLAGS=-std=gnu99
LDFLAGS=-pthread -lrados -ljansson -lm
all: cptt

cptt: cptt.c
	cc -g -c cptt.c -o cptt.o $(CFLAGS)
	cc -g cptt.o -o cptt $(LDFLAGS)

clean:
	rm cptt.o cptt
