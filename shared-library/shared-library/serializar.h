/*
 * serializar.h
 *
 *  Created on: 21/5/2017
 *      Author: utnso
 */

#ifndef SERIALIZAR_H_
#define SERIALIZAR_H_

#include <stdio.h>
#include <commons/string.h>
#include <commons/collections/list.h>
#include <commons/collections/queue.h>
#include <commons/collections/dictionary.h>
#include "generales.h"

typedef t_list t_stack;

//typedef struct{
//	int offset;
//	int size;
//}t_indice_codigo;

//typedef struct{
//	int pagina;
//	int offset;
//	int size;
//}posicion_memoria;

typedef struct{
	int length;
	int pid;
	int PC;
	int cantidad_paginas;
	int exit_code;

	/*int lengthListIndiceStack;
	t_stack* indice_stack; //lista con elementos t_element_stack*/
	//int lengthListIndiceCodigo;//cantidad de elementos en la lista
	//int lengthElementsIndiceCodigo;//longitud de los elementos de la lista
	//char* ElementsIndiceCodigo; //elementos t_indice_codigo
	// tabla de archivos

} __attribute__ ((__packed__)) t_packagePCB;



t_stream *pcb_serializer(t_PCB* pcb);
t_PCB* deserializer_pcb(char* buffer);
t_stream* indiceCodigo_serializer(t_PCB* pcb);
t_stream* indiceStack_serializer(t_stack* indiceStack);
t_stream* indiceEtiquetas_serializer(t_dictionary* indice_etiquetas);
t_stream* elementStack_serializer(t_element_stack* elementStack);
t_stream* argsVars_serializer(t_list* argsVars);

int indice_codigo_size(t_indice_codigo* indiceCodigo);

#endif /* SERIALIZAR_H_ */
