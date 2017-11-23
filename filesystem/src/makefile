CC := gcc
CFLAGS := -Wall
ISL := -I../../shared-library
LSL := -L../../shared-library/shared-library
LIBS := -lcommons -lpthread -lshared-library 

OBJS := file-system.o
HDRS := file-system.h

all: file-system

file-system: $(OBJS)
	$(CC) $(ISL) $(LSL) $(CFLAGS) -o file-system $(OBJS) $(LIBS)
	
file-system.o: file-system.c $(HDRS)
	$(CC) $(ISL) $(LSL) -c $(CFLAGS) file-system.c $(LIBS)

clean:
	rm file-system $(OBJS)