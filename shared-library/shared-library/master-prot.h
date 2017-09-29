/*
 * master-prot.h
 *
 *  Created on: 24/9/2017
 *      Author: utnso
 */

#ifndef MASTER_PROT_H_
#define MASTER_PROT_H_

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "socket.h"

typedef struct {
	pthread_t thread_id;
	int nodo;
	char * ip_port;
	int bloque;
	int bytes_ocupados;
	char * archivo_temporal;
} respuesta_yama_transform;

typedef struct {
	int nodo;
	char * ip_port;
	char ** archivos_temporales_transformacion;
	char * archivo_temporal_reduccion;
} respuesta_yama_reduccion;

typedef struct {
	char * ip;
	char * port;
} ip_port_combo;

//AUX
ip_port_combo * split_ipport(char * ipport);

int yama_request_send(int * yama_socket, char * archivo_origen);
int yama_request_recv(int * master_socket, char * archivo_origen_ptr);
int yama_response_send(int * master_socket, respuesta_yama_transform * paquete_transformacion);
int yama_response_recv(int * yama_socket, respuesta_yama_transform * paquete_transformacion);
int transform_res_send(int * master_socket, int * result);
int transform_res_recv(int * worker_socket, int * result);
int yama_transform_res_send(int * yama_socket, int * result);
int yama_transform_res_recv(int * master_socket, int * result);
int reduccion_local_res_send(int * master_socket, respuesta_yama_reduccion * struct_reduccion);
int reduccion_local_res_recv(int * yama_socket, respuesta_yama_reduccion * struct_reduccion);
#endif /* MASTER_PROT_H_ */
