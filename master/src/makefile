CC := gcc
CFLAGS := -Wall
ISL := -I../../shared-library
LSL := -L../../shared-library/shared-library
LIBS := -lcommons -lpthread -lshared-library

OBJS := master.o
HDRS := master.h

all: master

master: $(OBJS)
	$(CC) $(ISL) $(LSL) $(CFLAGS) -o master $(OBJS) $(LIBS)
	
master.o: master.c $(HDRS)
	$(CC) $(ISL) $(LSL) -c $(CFLAGS) master.c $(LIBS)
	
clean:
	rm master $(OBJS)