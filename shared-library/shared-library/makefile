all: libshared-library.so

libshared-library.so: file_system_prot.o generales.o memory_prot.o serializar.o socket.o
	gcc -shared -o libshared-library.so file_system_prot.o generales.o memory_prot.o serializar.o socket.o
file_system.o: file_system.h
	gcc -c -fPIC file_syhstem.c
generales.o: generales.h
	gcc -c -fPIC generales.c
memory_prot.o: memory_prot.h
	gcc -c -fPIC memory_prot.c
serializar.o: serializar.h
	gcc -c -fPIC serializar.c
socket.o: socket.h
	gcc -c -fPIC socket.c

clean:
	rm libshared-library.so file_system_prot.o generales.o memory_prot.o serializar.o socket.o
