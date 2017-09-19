/*
 * worker.h
 *
 *  Created on: 11/9/2017
 *      Author: utnso
 */

#ifndef WORKER_H_
#define WORKER_H_

#include <stdio.h>
#include <stdlib.h>

#define	SOCKET_BACKLOG 			100
#define BUFFER_SIZE 			2

typedef struct{
	char* filesystem_ip;
	u_int32_t filesystem_port;
	char* nodo_name;
	u_int32_t worker_port;
	char* databin_path;
}t_worker_conf;


void load_properties(void);

#endif /* WORKER_H_ */
