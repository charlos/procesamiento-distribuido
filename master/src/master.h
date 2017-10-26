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
#include <shared-library/master-prot.h>
#include <shared-library/worker-prot.h>
#include <shared-library/yama-prot.h>
#include <sys/stat.h>
#include <sys/mman.h>

int yama_socket;

typedef struct {
	char * ruta_trans;
	char * ruta_reduc;
	char * ruta_orige;
	char * ruta_resul;
} pedido_master;

typedef struct {
	void * file;
	size_t filesize;
} struct_file;
struct_file * transformador_file;

t_log * logger;
typedef struct {
	char * ip_yama;
	char * port_yama;
} master_cfg;
master_cfg * master_config;

typedef struct {
	pthread_t thread_id;
	int num;
	char * transformador;
	int bloque;
} thread_args_transformacion;

master_cfg * crear_config();

struct_file * read_file(char * path);
void atender_respuesta_transform(respuesta_yama_transform * resp);
void atender_respuesta_reduccion(t_red_local * respuesta);
pedido_master * crear_pedido_yama(char ** argv);
void liberar_respuesta_transformacion(respuesta_yama_transform *respuesta);
void liberar_respuesta_reduccion_local(t_red_local *respuesta);
void liberar_combo_ip(ip_port_combo *combo);
void crear_hilo_transformador(t_transformacion *transformacion, int job_id);
void crear_hilo_reduccion_local(t_red_local *reduccion);
respuesta_yama_transform *crear_transformacion_master(t_transformacion *transformacion_yama);
void atender_solicitud(t_yama_planificacion_resp *solicitud);


#endif /* MASTER_H_ */
