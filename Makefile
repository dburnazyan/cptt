CFLAGS=-I../../src/include -pthread -std=gnu99  -lm -lrt
all: cptt

cptt: cptt.c
	cc -g -c cptt.c -o cptt.o $(CFLAGS)
	cc -g cptt.o -lrados -pthread -o cptt -L/usr/local/lib/ -ljansson $(LDFLAGS)

clean:
	rm cptt.o cptt
