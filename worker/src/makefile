CC := gcc
CFLAGS := -Wall
ISL := -I../../shared-library
LSL := -L../../shared-library/shared-library
LIBS := -lcommons -lpthread -lshared-library

OBJS := worker.o worker_generales.o
HDRS := worker.h

all: worker

worker: $(OBJS)
	$(CC) $(ISL) $(LSL) $(CFLAGS) -o worker $(OBJS) $(LIBS)
	
worker.o: worker.c $(HDRS)
	$(CC) $(ISL) $(LSL) -c $(CFLAGS) worker.c $(LIBS)
	
worker_generales.o: worker.h
	$(CC) $(ISL) $(LSL) -c $(CFLAGS) worker_generales.c $(LIBS) -o worker_generales.o

clean:
	rm worker $(OBJS)