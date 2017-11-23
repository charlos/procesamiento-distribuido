CC := gcc
CFLAGS := -Wall
ISL := -I../../shared-library
LSL := -L../../shared-library/shared-library
LIBS := -lcommons -lshared-library

OBJS := data-node.o
HDRS := data-node.h

all: data-node

data-node: $(OBJS)
	$(CC) $(ISL) $(LSL) $(CFLAGS) -o data-node $(OBJS) $(LIBS)
	
data-node.o: data-node.c $(HDRS)
	$(CC) $(ISL) $(LSL) -c $(CFLAGS) data-node.c $(LIBS)

clean:
	rm data-node $(OBJS)