# Shared library 

# Memoria: Funciones principales

## handshake
```
int handshake(int * server_socket, t_log * logger)
```

Pedido de tamaño de frame en memoria: 
- server_socket : socket file descriptor 
- logger: opcional

Respuesta: 
- int = tamaño frame de memoria. Posibles errores: (DISCONNECTED_SERVER=-202)



## init process
```
int memory_init_process(int server_socket, int pid, int pages, t_log * logger)
```

Pedido de inicio de proceso: 
- server_socket : socket file descriptor 
- pid: id del proceso
- pages: cantidad de páginas a solicitar para el proceso
- logger: opcional

Respuesta: 
- int = código respuesta inicio del proceso (SUCCESS=1, DISCONNECTED_SERVER=-202, ENOSPC=-203)



## write
```
int memory_write(int server_socket, int pid, int page, int offset, int size, int buffer_size, void * buffer, t_log * logger)
```

Pedido de escritura: 
- server_socket : socket file descriptor 
- pid: id del proceso
- page: página a realizar la escritura
- offset: posición de inicio de escritura
- size: cantidad de bytes a escribir
- buffer_size: tamaño del buffer
- buffer: buffer con los bytes a escribir
- logger: opcional

Respuesta: 
- int = código respuesta escritura (SUCCESS=1, DISCONNECTED_SERVER=-202)



## read
```
t_read_response * memory_read(int server_socket, int pid, int page, int offset, int size, t_log * logger) 
```

Pedido de lectura: 
- server_socket : socket file descriptor 
- pid: id del proceso
- page: página a realizar la lectura
- offset: posición de inicio de lectura
- size: cantidad de bytes a leer
- logger: opcional

Respuesta:
- t_read_response->exec_code = código respuesta lectura (SUCCESS=1, DISCONNECTED_SERVER=-202)
- t_read_response->buffer_size = tamaño del buffer
- t_read_response->buffer = buffer con los bytes leídos

**No olvidar liberar memoria** 
```
t_read_response * read_response = memory_read(server_socket, pid, page, offset, size, logger);
free(read_response->buffer);
free(read_response);
```  



## assign pages
```
int memory_assign_pages(int server_socket, int pid, int pages, t_log * logger) 
```

Pedido de asignación de páginas a un proceso: 
- server_socket : socket file descriptor 
- pid: id del proceso
- pages: cantidad de páginas a asignar
- logger: opcional

Respuesta:
- int = código respuesta asignación (SUCCESS=1, DISCONNECTED_SERVER=-202, ENOSPC=-203)



## finalize process
```
int memory_finalize_process(int server_socket, int pid, t_log * logger) 
```

Pedido de finalización del proceso: 
- server_socket : socket file descriptor 
- pid: id del proceso
- logger: opcional

Respuesta:
- int = código respuesta finalización del proceso (SUCCESS=1, DISCONNECTED_SERVER=-202)


## Proceso ejemplo utilizando todas las funciones
```
#include <stdio.h>
#include <stdlib.h>
#include <shared-library/socket.h>
#include <shared-library/memory_prot.h>

int server_socket;

int main(void) {
	char * server_ip = "127.0.0.1";
	char * server_port = "5003";
	server_socket = connect_to_socket(server_ip, server_port);

	//handshake
	int mem_size = handshake(server_socket, NULL);
	printf("CPU ::: handshake memory ::: frame size %d bytes\n", mem_size);

	// init process
	int pid = 1;
	int pages = 5;
	int resp = memory_init_process(server_socket, pid, pages, NULL);
	printf("CPU ::: memory init process ::: pid %d pages %d ::: response code %d\n", pid, pages, resp);

	// writing
	int page = 2;
	int offset = 0;
	char * msg = NULL;
	size_t len = 0;
	ssize_t read;
	printf("CPU ::: enter message to write in page 2 ([ctrl + d] to quit)\n");
	while ((read = getline(&msg, &len, stdin)) != -1) {
		if (read > 0) msg[read-1] = '\0';
		printf("CPU ::: sending to memory ::: msg : %s\n", msg);
		resp = memory_write(server_socket, pid, page, offset, strlen(msg) + 1, strlen(msg) + 1, msg, NULL);
		printf("CPU ::: memory write ::: response code %d\n", resp);
		break;
	}
	free(msg);

	// reading
	printf("CPU ::: reading page %d in memory\n", page);
	t_read_response * read_resp = memory_read(server_socket, pid, page, offset, mem_size, NULL);
	((char *)(read_resp->buffer))[(read_resp->buffer_size) - 1] = "\0";
	printf("CPU ::: page 2 content ::: %s\n", read_resp->buffer);
	free(read_resp->buffer);
	free(read_resp);

	// assigning pages
	pages = 3;
	printf("CPU ::: assigning %d pages to process %d\n", pages, pid);
	resp = memory_assign_pages(server_socket, pid, pages, NULL);
	printf("CPU ::: assigning pages ::: response code %d\n", resp);

	// ending process
	printf("CPU ::: ending process %d\n", pid);
	resp = memory_finalize_process(server_socket, pid, NULL);
	printf("CPU ::: ending process ::: response code %d\n", resp);

	return EXIT_SUCCESS;
}
```


# File-System: Funciones principales

## handshake
```
int fs_handshake(int * server_socket, t_log * logger)
```
(Sin definir)



## validate file
```
int fs_validate_file(int server_socket, char * path, t_log * logger)
```

Pedido de validación de un archivo: 
- server_socket : socket file descriptor 
- path: path del archivo
- logger: opcional

Respuesta: 
- int = código respuesta (ISREG=2, ISNOTREG=-205, DISCONNECTED_SERVER=-202)



## create file
```
int fs_create_file(int server_socket, char * path, t_log * logger)
```

Creación del archivo dentro del path solicitado.: 
- server_socket : socket file descriptor 
- path: path del archivo a crear
- logger: opcional

Respuesta: 
- int = código respuesta (ISDIR=3, ENOSPC=-203, SUCCESS=1, DISCONNECTED_SERVER=-202)



## delete file
```
int fs_delete_file(int server_socket, char * path, t_log * logger)
```

Eliminación de un archivo: 
- server_socket : socket file descriptor 
- path: path del archivo a eliminar
- logger: opcional

Respuesta: 
- int = código respuesta (SUCCESS=1, DISCONNECTED_SERVER=-202)



## read
```
t_fs_read_resp * fs_read(int server_socket, char * path, int offset, int size, t_log * logger)
```

Leer de un archivo: 
- server_socket : socket file descriptor 
- path: path del archivo a leer
- offset
- size
- logger: opcional

Respuesta:
- t_fs_read_resp->exec_code = código respuesta (ISNOTREG=-205, SUCCESS=1, DISCONNECTED_SERVER=-202)
- t_fs_read_resp->buffer_size = tamaño del buffer
- t_fs_read_resp->buffer = buffer con los bytes leídos

**No olvidar liberar memoria** 
```
t_fs_read_resp * read_resp = fs_read(server_socket, path, offset, size, logger);
free(read_resp->buffer);
free(read_resp);
```  



## write
```
int fs_write(int server_socket, char * path, int offset, int size, int buffer_size, void * buffer, t_log * logger)
```

Leer de un archivo: 
- server_socket : socket file descriptor 
- path: path del archivo a escribir
- offset
- size
- buffer_size : tamaño del buffer
- buffer : buffer con los bytes a escribir
- logger: opcional

Respuesta:
int = código respuesta (ISNOTREG=-205, SUCCESS=1, ENOSPC=-203, DISCONNECTED_SERVER=-202)

