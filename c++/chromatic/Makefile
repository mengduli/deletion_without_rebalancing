ROOT = ../../..

include $(ROOT)/common/Makefile.common

.PHONY:	all clean

all:	main
BINS = $(BINDIR)/lockfree-chromatic

chromatic.o:
	$(CC) -std=gnu99 -lm $(CFLAGS) $(GSLFLAGS) $(JEMALLOCFLAGS) -O3 -c -o chromatic.o chromatic.c

test.o:
	$(CC) -std=gnu99 -lm $(CFLAGS) $(GSLFLAGS) $(JEMALLOCFLAGS) -O3 -c -o test.o test.c

main: chromatic.o test.o
	$(CC) -std=gnu99 -lm $(CFLAGS) $(GSLFLAGS) $(JEMALLOCFLAGS) -O3 chromatic.o test.o -o $(BINS) $(LDFLAGS)

clean:
	-rm -f $(BINS) *.o