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
#include <shared-library/connect.h>
#include <commons/temporal.h>
#include <commons/log.h>
#include <commons/config.h>

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

master_cfg * crear_config();
pedido_master * crear_pedido_yama(char ** argv);
#endif /* MASTER_H_ */
