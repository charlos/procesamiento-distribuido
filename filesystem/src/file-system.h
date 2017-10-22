
#include <stdint.h>
#include <commons/collections/list.h>
#include <commons/collections/node.h>

#ifndef FILE_SYSTEM_H_
#define FILE_SYSTEM_H_

#define DIR_NAME_LENGTH 	255

typedef void (* Function)(int);
typedef char **CPPFunction ();

typedef struct {
	uint32_t port;
	char * mount_point;
	char * logfile;
} t_fs_conf;

typedef struct {
	int index;
	char name[DIR_NAME_LENGTH];
	int parent_dir;
} t_fs_directory;

typedef struct {
	char * node_name;
	char * ip_port;
	int size;
	int fd;
	t_bitarray * bitmap;
} t_fs_node;

typedef struct {
	int block;
	int bytes;
} t_fs_required_block;

typedef struct {
	char* name;
	Function funcion;
	char ** argumentos;
} t_command;

typedef struct {
	int blocks;
	char * node;
} t_fs_to_release;

#endif /* FILE_SYSTEM_H_ */
