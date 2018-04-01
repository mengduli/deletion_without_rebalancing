ROOT = ../../..

include $(ROOT)/common/Makefile.common

.PHONY:	all clean

all:	main
BINS = $(BINDIR)/lockfree-dwrbavl
dwrbavl.o:
	$(CC) -std=gnu99 -lm $(CFLAGS) $(GSLFLAGS) $(JEMALLOCFLAGS) -O3 -c -o dwrbavl.o dwrbavl.c

test.o:
	$(CC) -std=gnu99 -lm $(CFLAGS) $(GSLFLAGS) $(JEMALLOCFLAGS) -O3 -c -o test.o test.c
	
main: dwrbavl.o test.o
	$(CC) -std=gnu99 -lm $(CFLAGS) $(GSLFLAGS) $(JEMALLOCFLAGS) -O3 dwrbavl.o test.o -o $(BINS) $(LDFLAGS)
	
clean:
	-rm -f $(BINS) *.o