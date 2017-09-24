/*
 * master.h
 *
 *  Created on: 11/9/2017
 *      Author: utnso
 */

#ifndef MASTER_H_
#define MASTER_H_

#include <stdio.h>
#include <stdlib.h>
#include <commons/temporal.h>
#include <commons/log.h>
#include <commons/config.h>
#include <commons/collections/list.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/mman.h>

t_log * logger;
typedef struct {
	char * ip_yama;
	char * port_yama;
} master_cfg;
master_cfg * master_config;

typedef struct {
	char * ruta_trans;
	char * ruta_reduc;
	char * ruta_orige;
	char * ruta_resul;
} pedido_master;

typedef struct {
	pthread_t thread_id;
	int nodo;
	char * ip_port;
	int bloque;
	int bytes_ocupados;
	char * archivo_temporal;
} respuesta_yama;

typedef struct {
	char * file;
	size_t filesize;
} struct_file;
struct_file * transformador_file;

typedef struct {
	pthread_t thread_id;
	int num;
	char * transformador;
	int bloque;
} thread_args_transformacion;

master_cfg * crear_config();
pedido_master * crear_pedido_yama(char ** argv);
int atender_respuesta(void * resp);
char * read_file(char * path);

#endif /* MASTER_H_ */
