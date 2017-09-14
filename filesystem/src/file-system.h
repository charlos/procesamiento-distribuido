
#include <stdint.h>
#include <commons/collections/list.h>
#include <commons/collections/node.h>

#ifndef FILE_SYSTEM_H_
#define FILE_SYSTEM_H_

typedef struct {
	uint32_t port;
	char *   logfile;
} t_fs_conf;

#endif /* FILE_SYSTEM_H_ */
