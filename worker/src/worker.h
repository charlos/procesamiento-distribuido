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
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <commons/log.h>
#include <commons/string.h>
#include <shared-library/generales.h>
#include <shared-library/socket.h>
#include <shared-library/worker-prot.h>

#define	SOCKET_BACKLOG 			100
#define BLOCK_SIZE 			1048576

typedef struct{
	char* filesystem_ip;
	u_int32_t filesystem_port;
	char* nodo_name;
	u_int32_t worker_port;
	char* databin_path;
}t_worker_conf;


void load_properties(void);
void create_script_file(char *script_filename, int script_size, void* script );

#endif /* WORKER_H_ */
