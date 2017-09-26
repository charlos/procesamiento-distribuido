/*
 * master-prot.c
 *
 *  Created on: 24/9/2017
 *      Author: utnso
 */

#include "master-prot.h"

int transform_res_send(int * master_socket, int * result) {
	int status = socket_send(master_socket, result, sizeof(int), 0);
	return status;
}
int transform_res_recv(int * worker_socket, int * result) {
	int status = socket_recv(worker_socket, result, sizeof(int));
	return status;
}
int yama_request_send(int * yama_socket, char * dir_archivo_origen) {
	int *length_dir, *buffer_size; void * buffer; char cierre = '\0';
	*length_dir = string_length(dir_archivo_origen) +1;
	*buffer_size = length_dir +1;
	buffer = malloc(*buffer_size);
	memcpy(buffer, length_dir, 1);
	memcpy(buffer +1, dir_archivo_origen, *buffer_size);
	memcpy(buffer + *length_dir, &cierre, 1);
	int status = socket_send(yama_socket, buffer, *buffer_size, 0);
	return status;
}
int yama_request_recv(int * master_socket, char* dir_archivo_origen) {
	void * dir_length = malloc(sizeof(int)); int status;
	status = socket_recv(master_socket, dir_length, 1);

	if(status < 0) {

	}
	dir_archivo_origen = malloc(dir_length);
	status = socket_recv(master_socket, dir_archivo_origen, dir_length);
	if(status < 0) {

	}
	return status;
}
ip_port_combo * split_ipport(char *ipport) {
	char ** ip_port = string_split(ipport, ":");
	ip_port_combo * combo = malloc(sizeof(ip_port_combo));
	combo->ip = ip_port[0];
	combo->port = ip_port[1];

	return combo;
}
