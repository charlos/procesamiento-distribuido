
#include <stdint.h>
#include <commons/collections/list.h>
#include <commons/collections/node.h>

#ifndef FILE_SYSTEM_H_
#define FILE_SYSTEM_H_

#define DIR_NAME_LENGTH 	255

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
	int size;
	int fd;
	t_bitarray * bitmap;
} t_fs_node;

typedef struct {
	int block;
	int bytes;
} t_fs_required_block;

#endif /* FILE_SYSTEM_H_ */
