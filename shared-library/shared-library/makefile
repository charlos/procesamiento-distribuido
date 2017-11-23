all: libshared-library.so

libshared-library.so: data-node-prot.o file-system-prot.o generales.o master-prot.o socket.o worker-prot.o yama-prot.o
	gcc -shared -o libshared-library.so data-node-prot.o file-system-prot.o generales.o master-prot.o socket.o worker-prot.o yama-prot.o
data-node-prot.o: data-node-prot.h
	gcc -c -fPIC data-node-prot.c
file-system-prot.o: file-system-prot.h
	gcc -c -fPIC file-system-prot.c
generales.o: generales.h
	gcc -c -fPIC generales.c
master-prot.o: master-prot.h
	gcc -c -fPIC master-prot.c
socket.o: socket.h
	gcc -c -fPIC socket.c
worker-prot.o: worker-prot.h
	gcc -c -fPIC worker-prot.c
yama-prot.o: yama-prot.h
	gcc -c -fPIC yama-prot.c

clean:
	rm libshared-library.so data-node-prot.o file-system-prot.o generales.o master-prot.o socket.o worker-prot.o yama-prot.o