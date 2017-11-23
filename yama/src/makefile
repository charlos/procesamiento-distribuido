CC := gcc
CFLAGS := -Wall
ISL := -I../../shared-library
LSL := -L../../shared-library/shared-library
LIBS := -lcommons -lpthread -lshared-library

OBJS := yama.o
HDRS := yama.h

all: yama

yama: $(OBJS)
	$(CC) $(ISL) $(LSL) $(CFLAGS) -o yama $(OBJS) $(LIBS)
	
yama.o: yama.c $(HDRS)
	$(CC) $(ISL) $(LSL) -c $(CFLAGS) yama.c $(LIBS)
	
clean:
	rm yama $(OBJS)