
#include <stdint.h>
#include <commons/collections/list.h>
#include <commons/collections/node.h>

#ifndef FILE_SYSTEM_H_
#define FILE_SYSTEM_H_

typedef struct {
	uint32_t port;
	char * mount_point;
	char * logfile;
} t_fs_conf;

typedef struct {
	int index;
	char name[255];
	int parent_dir;
} t_fs_directory;

typedef struct {
	char * node_name;
	t_bitarray * bitmap;
} t_fs_bitmap_node;

#endif /* FILE_SYSTEM_H_ */
