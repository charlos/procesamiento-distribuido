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
#define BUFFER_SIZE 			1024

typedef struct{
	char* filesystem_ip;
	char* filesystem_port;
	char* nodo_name;
	char* worker_port;
	char* databin_path;
}t_worker_conf;


#endif /* WORKER_H_ */
