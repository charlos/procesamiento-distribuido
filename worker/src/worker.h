/*
 * worker.h
 *
 *  Created on: 11/9/2017
 *      Author: utnso
 */

#ifndef WORKER_H_
#define WORKER_H_

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <commons/log.h>
#include <commons/string.h>
#include <shared-library/generales.h>
#include <shared-library/socket.h>
#include <shared-library/worker-prot.h>
#include <shared-library/file-system-prot.h>
#include <shared-library/master-prot.h>

#define	SOCKET_BACKLOG 			100
#define BLOCK_SIZE 			1048576

#define PATH   "/home/utnso/yama/"

typedef struct{
	char* filesystem_ip;
	char* filesystem_port;
	char* nodo_name;
	int worker_port;
	char* databin_path;
}t_worker_conf;


typedef struct {
	void * file;
	size_t filesize;
} struct_file;

typedef struct {
	int fd;
	int longitud_linea;
	char *linea;
} t_estructura_loca_apareo;

typedef struct {
	int fd;
	char *resultado_reduccion_local;
} t_argumento_reduccion_global;

void * map_file(char * file_path, int flags);
void load_properties(char*);
void create_script_file(char *script_filename, int script_size, void* script );
void create_block_file(char *filename, int size, void* block );
size_t merge_two_files(FILE* file1, FILE* file2, char** result);
int processRequest(uint8_t task_code, void* pedido);
void free_request(int task_code, void* buffer);
void free_request_local_reduction(t_request_local_reduction* request);
void free_request_transformation(t_request_transformation* request);
struct_file * read_file(char * path);
bool quedan_datos_por_leer(t_list *lista);
void leer_linea(t_estructura_loca_apareo *est_apareo);
t_estructura_loca_apareo *convertir_a_estructura_loca(t_red_global *red_global);
void merge_global(t_list *lista_reduc_global);
bool quedan_datos_por_leer(t_list *lista);

#endif /* WORKER_H_ */
