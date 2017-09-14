/*
 * generales.c
 *
 *  Created on: 13/9/2017
 *      Author: utnso
 */
#include "generales.h"

// Crea el logger con el nombre del proceso
void crear_logger(char* pathProceso, t_log** logger, bool active_console, t_log_level level){
	char* nombreProceso;
	char* nombre_log = string_new();
	string_append(&nombre_log, pathProceso);
	string_append(&nombre_log, ".log");
	nombreProceso = obtener_nombre_proceso(pathProceso);
	string_to_upper(nombreProceso);
	*logger = log_create(nombre_log, nombreProceso, active_console, level);
}

char* obtener_nombre_proceso(char* pathProceso){
	char* nombreProceso;
	char** lista;
	nombreProceso = string_reverse(pathProceso);
	lista = string_split(nombreProceso, "/");
	nombreProceso = lista[0];
	nombreProceso = string_reverse(nombreProceso);
	return nombreProceso;
}
