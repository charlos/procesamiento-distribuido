/*
 * master-prot.c
 *
 *  Created on: 24/9/2017
 *      Author: utnso
 */

#include "master-prot.h"

int transform_res_send(int * master_socket, resultado_transformacion * result) {
	int status = socket_send(master_socket, result, sizeof(resultado_transformacion), 0);
	return status;
}
int transform_res_recv(int * worker_socket, resultado_transformacion * result) {
	int status = socket_recv(worker_socket, result, sizeof(resultado_transformacion));
	return status;
}
int yama_request_send(int * yama_socket, char * dir_archivo_origen) {
	int *length_dir = malloc(1);
	int *buffer_size = malloc(1);
	void * buffer;
	//char cierre = '\0';
	*length_dir = string_length(dir_archivo_origen) +1;
	*buffer_size = length_dir +1;
	buffer = malloc(*buffer_size);
	memcpy(buffer, length_dir, 1);
	memcpy(buffer +1, dir_archivo_origen, *buffer_size);
//	memcpy(buffer +1 + *length_dir, &cierre, 1);
	int status = socket_send(yama_socket, buffer, *buffer_size, 0);
	free(buffer);
	free(buffer_size);
	free(length_dir);
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
int yama_response_send(int * master_socket, respuesta_yama_transform * paquete_transformacion) {
	int length_ip_port = string_length(paquete_transformacion->ip_port) + 1;
	int length_dest_archivo = string_length(paquete_transformacion->archivo_temporal) + 1;
	int buffer_size = sizeof(pthread_t) + sizeof(int) + sizeof(int) + length_ip_port + sizeof(int) + sizeof(int) + sizeof(int) + length_dest_archivo;
	void * buffer = sizeof(buffer_size);

	memcpy(buffer, paquete_transformacion->thread_id, sizeof(pthread_t));
	memcpy(buffer + sizeof(pthread_t), paquete_transformacion->nodo, 1);
	memcpy(buffer + sizeof(pthread_t) + 1, &length_ip_port, 1);
	memcpy(buffer + sizeof(pthread_t) + 1 + 1, paquete_transformacion->ip_port, length_ip_port);
	memcpy(buffer + sizeof(pthread_t) + 1 + 1 + length_ip_port, paquete_transformacion->bloque, 1);
	memcpy(buffer + sizeof(pthread_t) + 1 + length_ip_port + 1, paquete_transformacion->bytes_ocupados, 1);
	memcpy(buffer + sizeof(pthread_t) + 1 + length_ip_port + 1 + 1, &length_dest_archivo, 1);
	memcpy(buffer + sizeof(pthread_t) + 1 + length_ip_port + 1 + 1 + 1, paquete_transformacion->archivo_temporal, length_dest_archivo);

	int status = socket_send(master_socket, buffer, buffer_size, 0);

	free(buffer);
	return status;
}
int yama_response_recv(int * yama_socket, respuesta_yama_transform * paquete_transformacion) {
	int length_ip_port, length_dest_archivo, status;
	socket_recv(yama_socket, &(paquete_transformacion->thread_id), sizeof(pthread_t));
	socket_recv(yama_socket, &(paquete_transformacion->nodo), 1);
	socket_recv(yama_socket, &length_ip_port, 1);
	socket_recv(yama_socket, paquete_transformacion->ip_port, length_ip_port);
	socket_recv(yama_socket, &(paquete_transformacion->bloque), 1);
	socket_recv(yama_socket, &(paquete_transformacion->bytes_ocupados), 1);
	socket_recv(yama_socket, &length_dest_archivo, 1);
	status = socket_recv(yama_socket, paquete_transformacion->archivo_temporal, length_dest_archivo);

	return status;
}
int yama_transform_res_send(int * yama_socket, int * result) {
	int status = socket_send(yama_socket, result, sizeof(int), 0);
	return status;
}
int yama_transform_res_recv(int * master_socket, int * result) {
	int status = socket_recv(master_socket, result, sizeof(int));
		return status;
}
int reduccion_local_res_recv(int * yama_socket, respuesta_yama_reduccion_local * struct_reduccion) {

}
ip_port_combo * split_ipport(char *ipport) {
	char ** ip_port = string_split(ipport, ":");
	ip_port_combo * combo = malloc(sizeof(ip_port_combo));
	combo->ip = ip_port[0];
	combo->port = ip_port[1];

	return combo;
}
int enviar_solicitud_almacenamiento_a_worker(int worker_socket, char * path) {
	int length_path = string_length(path) + 1;
	void * buffer = malloc(length_path + 1);
	memcpy(buffer, &length_path, 1);
	memcpy(buffer + 1, path, length_path);

	int status = socket_write(&worker_socket, buffer, length_path +1);
	free(buffer);
	return status;
}
char * recibir_solicitud_alamacenamiento_desde_worker(int master_socket) {
	int * length_path = malloc(sizeof(int));
	int status = socket_recv(&master_socket, length_path, 1);
	//chequear status?
	char * path = malloc(*length_path);
	status = socket_recv(&master_socket, path, *length_path);
	free(length_path);
	return path;
}
