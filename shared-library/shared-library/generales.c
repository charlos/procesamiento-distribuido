/*
 * generales.c
 *
 *  Created on: 12/4/2017
 *      Author: utnso
 */
#include "generales.h"
#include "socket.h"
#include "serializar.h"

char* obtener_nombre_proceso(char* pathProceso){
	char* nombreProceso;
	char** lista;
	nombreProceso = string_reverse(pathProceso);
	lista = string_split(nombreProceso, "/");
	nombreProceso = lista[0];
	nombreProceso = string_reverse(nombreProceso);
	return nombreProceso;
}

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


int serializar_y_enviar_PCB(t_PCB* pcb, int socket_destino, int OC){
	t_stream *paquete = pcb_serializer(pcb);
	int ret;
	ret = connection_send(socket_destino, OC, paquete);

	free(paquete->data);
	free(paquete);

	return ret;
}

void element_stack_destroy(t_element_stack* contexto){
	list_destroy_and_destroy_elements(contexto->args, (void*) free);
	list_destroy_and_destroy_elements(contexto->vars, (void*) free);
//	if(contexto->retVar != NULL)free(contexto->retVar);
	free(contexto);
}

void pcb_destroy(t_PCB* pcb){
	/*
	typedef struct{
	uint16_t pid;
	uint16_t PC;
	uint16_t cantidad_paginas;
	int_least16_t exit_code;
	uint16_t SP;
	uint16_t cantidad_instrucciones;
	t_stack* indice_stack; //lista con elementos t_element_stack
	t_indice_codigo* indice_codigo; //lista con elementos t_indice_codigo
	t_dictionary* indice_etiquetas;
	}t_PCB;
	*/

	list_destroy_and_destroy_elements(pcb->indice_stack, (void*) element_stack_destroy);
//	list_destroy_and_destroy_elements(pcb->indice_codigo, (void*) free);
	free(pcb->indice_codigo);
	dictionary_clean_and_destroy_elements(pcb->indice_etiquetas, (void*) free);
	free(pcb);
}

