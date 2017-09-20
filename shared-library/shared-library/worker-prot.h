/*
 * worker-prot.h
 *
 *  Created on: 19/9/2017
 *      Author: Gustavo Tofaletti
 */

#ifndef WORKER_PROT_H_
#define WORKER_PROT_H_

#define TRANSFORM_OC			1
#define REDUCE_LOCALLY_OC		2

#define	SUCCESS							1
#define	ERROR							-200
#define	DISCONNECTED_CLIENT				-201
#define	DISCONNECTED_SERVER				-202


typedef struct{
	 int block;
	 int used_size;
	 char* result_file;
	 int script_size;
	 void* script;
	 int16_t exec_code;
} t_request_transformation;

typedef struct{
	 char* temp_files;
	 char* result_file;
	 int script_size;
	 void* script;
	 int16_t exec_code;
} t_request_local_reduction;

int transform_req_send(int worker_socket, int block, int used_size, char* result_file, int script_size, void* script, t_log * logger);
t_request_transformation * transform_req_recv(int * client_socket, t_log * logger);
int local_reduction_req_send(int worker_socket, char* temp_files, char* result_file, int script_size, void* script, t_log * logger);
t_request_local_reduction * local_reduction_req_recv(int * client_socket, t_log * logger);


#endif /* WORKER_PROT_H_ */
