/*
 ============================================================================
 Name        : serializar.c
 Author      : Carlos Flores
 Version     :
 Copyright   : GitHub @Charlos
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include "serializar.h"

/*int main(void) {
	t_stream* stream;
	puts("PCB Serializer\n");
	//inicializar PCB
	t_PCB* pcb = malloc(sizeof(t_PCB));
	t_indice_codigo* elem_1 = malloc(sizeof(t_indice_codigo));
	elem_1->offset = 911;
	elem_1->size = 912;
	t_indice_codigo* elem_2 = malloc(sizeof(t_indice_codigo));
	elem_2->offset = 921;
	elem_2->size = 922;
	t_indice_codigo* elem_3 = malloc(sizeof(t_indice_codigo));
	elem_3->offset = 931;
	elem_3->size = 932;

	pcb->pid = 111;
	pcb->PC = 22;
	pcb->cantidad_paginas = 33;
	pcb->exit_code = -4;
	pcb->stack_pointer = 7;
	pcb->cantidad_instrucciones = 1;
	pcb->indice_codigo = malloc(sizeof(t_indice_codigo));
	pcb->indice_codigo = elem_1;
	pcb->indice_stack = list_create();
		t_element_stack* elem_stack_1 = malloc(sizeof(t_element_stack));
		elem_stack_1->retPos = 666;
		elem_stack_1->retVar = malloc(sizeof(posicion_memoria));
		elem_stack_1->retVar->offset = 6661;
		elem_stack_1->retVar->pagina = 6662;
		elem_stack_1->retVar->size = 6663;
		elem_stack_1->args = list_create();
		t_args_vars* elem_arg_1 = malloc(sizeof(t_args_vars));
			elem_arg_1->id = 'a';
			elem_arg_1->offset = 7772;
			elem_arg_1->pagina = 7773;
			elem_arg_1->size = 7774;
		t_args_vars* elem_arg_2 = malloc(sizeof(t_args_vars));
			elem_arg_2->id = 'b';
			elem_arg_2->offset = 7782;
			elem_arg_2->pagina = 7783;
			elem_arg_2->size = 7778;
		list_add(elem_stack_1->args, elem_arg_1);
		list_add(elem_stack_1->args, elem_arg_2);
		elem_stack_1->vars = list_create();
		t_args_vars* elem_var_1 = malloc(sizeof(t_args_vars));
			elem_var_1->id = 'y';
			elem_var_1->offset = 8782;
			elem_var_1->pagina = 8783;
			elem_var_1->size = 8784;
		t_args_vars* elem_var_2 = malloc(sizeof(t_args_vars));
			elem_var_2->id = 'z';
			elem_var_2->offset = 8782;
			elem_var_2->pagina = 8783;
			elem_var_2->size = 8784;
		list_add(elem_stack_1->vars, elem_var_1);
		list_add(elem_stack_1->vars, elem_var_2);
	list_add(pcb->indice_stack, elem_stack_1);
	pcb->indice_etiquetas = dictionary_create();
		int value1 = 27;
		char* key1 = malloc(7); key1 = "nombre\0";
		int value2 = 26;
		char* key2 = malloc(9); key2 = "apellido\0";
	dictionary_put(pcb->indice_etiquetas, "nombre", &value1);

	//int resultado = *dictionary_get(dicResultado, "apellido"); //El resultado debe ser 26! (como los campeonatos que ganó BOCA xD

	stream = pcb_serializer(pcb);

	//escribimos en el archivo
 	FILE *fp;
 	fp = fopen("fichero.txt", "wb+");

 	//fwrite(data, sizeof(char), 12, fp);
 	//fclose (fp);

 	//fwrite(stream, (1+stream->length), 1, fp);
 	//fwrite(stream, sizeof(char), (10), fp);

 	fwrite(&stream->length, sizeof(int), 1, fp);
 	//stream->data = malloc(90);

 	fwrite(stream->data, sizeof(char), stream->length, fp);
 	fclose (fp);
 	puts("END\n");
 	//getchar();

 	//leemos el archivo
 	t_stream* stream_2 = malloc(sizeof(t_stream));

 	puts("LEER ARCHIVO");
 	FILE *file;
 	file = fopen("fichero.txt", "rb");
 	fread(&stream_2->length, (sizeof(int)), 1, file);
 	stream_2->data = malloc(stream_2->length);
 	fread(stream_2->data, sizeof(char), stream_2->length, file);
 	fclose (file);
 	//stream_2->data = data;

 	t_PCB* pcb_recv = deserializer_pcb(stream_2->data);

 	int* resultado = dictionary_get(pcb_recv->indice_etiquetas, "apellido"); //El resultado debe ser 26! (como los campeonatos que ganó BOCA xD
 	t_list* vars = ((t_element_stack*)list_get(pcb_recv->indice_stack, 0))->vars;
 	char id = ((t_args_vars*)list_get(vars, 0))->id;

 	puts("END\n");

 	return EXIT_SUCCESS;
}*/

t_PCB* deserializer_pcb(char* buffer){
	int offset = 0;
	int valor = 0;
	t_dictionary* dic_indiceEtiquetas = dictionary_create();
	t_PCB* pcb_result = malloc(sizeof(t_PCB));
	memcpy(&pcb_result->pid, buffer+offset, valor=sizeof(uint16_t)); offset+=valor;
 	memcpy(&pcb_result->PC, buffer+offset, valor=sizeof(uint16_t)); offset+=valor;
 	memcpy(&pcb_result->cantidad_paginas, buffer+offset, valor=sizeof(uint16_t)); offset+=valor;
 	memcpy(&pcb_result->SP, buffer+offset, valor=sizeof(uint16_t)); offset+=valor;
 	memcpy(&pcb_result->exit_code, buffer+offset, valor=sizeof(int_least16_t)); offset+=valor;

 	uint16_t lengthIndiceCodigo;
 	memcpy(&lengthIndiceCodigo, buffer+offset, valor=sizeof(uint16_t)); offset+=valor;
 	pcb_result->cantidad_instrucciones = lengthIndiceCodigo;
 	int tamElementos = lengthIndiceCodigo*sizeof(t_indice_codigo);
 	pcb_result->indice_codigo = malloc(tamElementos);
 	int i;
 	for (i = 0; i < lengthIndiceCodigo; i++) {
 		memcpy(pcb_result->indice_codigo + i, buffer+offset, valor=sizeof(t_indice_codigo)); offset+=valor;
 		/*memcpy(&((pcb->indice_codigo+i)->offset), buffer+offset, valor=sizeof(int)); offset+=valor;
 		memcpy(&((pcb->indice_codigo+i)->size), buffer+offset, valor=sizeof(int)); offset+=valor;*/
	}

 	int lengthStack; //cantidad de t_element_stack de las lista stack
 	memcpy(&lengthStack, buffer+offset, valor=sizeof(int)); offset+=valor;
 	pcb_result->indice_stack = list_create();
 	for (i = 0; i < lengthStack; i++) {
 		t_element_stack* elementStack = malloc(sizeof(t_element_stack));
 		posicion_memoria* posicionMemoria = malloc(sizeof(posicion_memoria));
		memcpy(&elementStack->retPos, buffer+offset, valor=sizeof(int)); offset+=valor;
		memcpy(posicionMemoria, buffer+offset, valor=sizeof(posicion_memoria)); offset+=valor;
		elementStack->retVar = posicionMemoria;
		//lista args de un elemento del stack
		elementStack->args = list_create();
		int lengthArgs; //cantidad de t_args_vars de la lista
		memcpy(&lengthArgs, buffer+offset, valor=sizeof(int)); offset+=valor;
		int j;
	 	for (j = 0; j < lengthArgs; j++) {
	 		t_args_vars* elementArgs = malloc(sizeof(t_args_vars));
			memcpy(elementArgs, buffer+offset, valor=sizeof(t_args_vars)); offset+=valor;
			list_add(elementStack->args, elementArgs);
		}
		//lista vars de un elemento del stack
		elementStack->vars = list_create();
		int lengthVars; //cantidad de t_args_vars de la lista
		memcpy(&lengthVars, buffer+offset, valor=sizeof(int)); offset+=valor;
	 	int k;
		for (k = 0; k < lengthVars; k++) {
	 		t_args_vars* elementVars = malloc(sizeof(t_args_vars));
			memcpy(&elementVars->id, buffer+offset, valor=sizeof(char)); offset+=valor;
			memcpy(&elementVars->pagina, buffer+offset, valor=sizeof(int)); offset+=valor;
			memcpy(&elementVars->offset, buffer+offset, valor=sizeof(int)); offset+=valor;
			memcpy(&elementVars->size, buffer+offset, valor=sizeof(int)); offset+=valor;

			list_add(elementStack->vars, elementVars);
		}

	 	list_add(pcb_result->indice_stack, elementStack);
	}

 	uint16_t lengthEtiquetas; //cantidad de etiquetas del diccionario
 	memcpy(&lengthEtiquetas, buffer+offset, valor=sizeof(uint16_t)); offset+=valor;
	/*pcb_result->indice_etiquetas = malloc(sizeof(t_dictionary));
	pcb_result->indice_etiquetas = dictionary_create();*/

	for (i = 0; i < lengthEtiquetas; i++) {
		int* data = malloc(sizeof(int));
		int valor = 0;
		uint16_t key_length;
		memcpy(&key_length, buffer+offset, valor=sizeof(uint16_t)); offset+=valor;
		char* key = malloc(key_length);
		char *unaPalabra = string_new();
		string_append(&unaPalabra, buffer+offset);
		char charEnd = '\0';
		string_append(&unaPalabra, &charEnd);
		offset+=key_length;
		//memcpy(key, buffer+offset, key_length); offset+=key_length;
		memcpy(data, buffer+offset, valor=sizeof(int)); offset+=valor;

		dictionary_put(dic_indiceEtiquetas, unaPalabra, data);
	}
	pcb_result->indice_etiquetas = dic_indiceEtiquetas;
 	return pcb_result;
}

t_stream* pcb_serializer(t_PCB* pcb){
	t_stream* stream_indiceCodigo = indiceCodigo_serializer(pcb);
	t_stream* stream_indiceStack = indiceStack_serializer(pcb->indice_stack);
	t_stream* stream_indiceEtiquetas = indiceEtiquetas_serializer(pcb->indice_etiquetas);
	int tamElementos = sizeof(int)+sizeof(t_PCB)+stream_indiceCodigo->length+stream_indiceStack->length+stream_indiceEtiquetas->length;
	char* data = malloc(tamElementos);
	int valor = 0;
	int offset = 0;
	memcpy(data+offset, &pcb->pid, valor = sizeof(uint16_t)); offset += valor;
	memcpy(data+offset, &pcb->PC, valor = sizeof(uint16_t)); offset += valor;
	memcpy(data+offset, &pcb->cantidad_paginas, valor = sizeof(uint16_t)); offset += valor;
	memcpy(data+offset, &pcb->SP, valor = sizeof(uint16_t)); offset += valor;
	memcpy(data+offset, &pcb->exit_code, valor = sizeof(uint16_t)); offset += valor;
	//memcpy(data+offset, &pcb->cantidad_instrucciones, valor = sizeof(uint16_t)); offset += valor;
	//memcpy(data+offset, &stream_indiceCodigo->length, valor = sizeof(int)); offset += valor;
	memcpy(data+offset, stream_indiceCodigo->data, valor = stream_indiceCodigo->length); offset += valor;
	//memcpy(data+offset, &stream_indiceStack->length, valor = sizeof(int)); offset += valor;
	memcpy(data+offset, stream_indiceStack->data, valor = stream_indiceStack->length); offset += valor;
	memcpy(data+offset, stream_indiceEtiquetas->data, valor = stream_indiceEtiquetas->length); offset += valor;

	t_stream* stream = malloc(sizeof(t_stream));
	stream->length = tamElementos;
	stream->data = data;

	return stream;
}

t_stream* indiceEtiquetas_serializer(t_dictionary* indice_etiquetas){
	uint16_t cantElementos = dictionary_size(indice_etiquetas);
	char* data_dictionary = NULL;
	int offset = 0;
	void _serializerDicEtiquetas(char* key, int* value) {
		char *unaPalabra = string_new();
		string_append(&unaPalabra, key);
		char charEnd = '\0';
		string_append(&unaPalabra, &charEnd);
		/*char charEnd = '\0';
		string_append(&key, &charEnd);*/
		uint16_t key_length = string_length(unaPalabra)+1;
		char* dataAux = malloc(offset);
		int valor;
	 	memcpy(dataAux, data_dictionary, offset);
	 	free(data_dictionary);

	 	data_dictionary = malloc(offset+sizeof(uint16_t)+key_length+sizeof(int));
	 	memcpy(data_dictionary, dataAux, offset);
	 	memcpy(data_dictionary+offset, &key_length, valor=sizeof(uint16_t)); offset+=valor;
	 	memcpy(data_dictionary+offset, key, key_length); offset+=key_length;
	 	memcpy(data_dictionary+offset, value, valor=sizeof(int)); offset+=valor;
	}
	dictionary_iterator(indice_etiquetas, (void*) _serializerDicEtiquetas);

	t_stream* stream = malloc(sizeof(t_stream));
	stream->length = sizeof(uint16_t)+offset;
	stream->data = malloc(stream->length);
	memcpy(stream->data, &cantElementos, sizeof(uint16_t));
	memcpy(stream->data+sizeof(uint16_t), data_dictionary, offset);
	free(data_dictionary);

	return stream;
}

t_stream* indiceCodigo_serializer(t_PCB* pcb){
	t_indice_codigo* indiceCodigo = pcb->indice_codigo;
	uint16_t cantElementos = pcb->cantidad_instrucciones; //indice_codigo_size(indiceCodigo);
	int tamElementos = sizeof(uint16_t)+(sizeof(t_indice_codigo)*cantElementos);
	char* data = malloc(tamElementos);
	//t_indice_codigo* iCodigo = list_get(indiceCodigo, 0);
	int offset = 0;
	int valor = 0;
	memcpy(data+offset, &cantElementos, valor=sizeof(uint16_t)); offset+=valor;
	int i;
	for (i = 0; i < cantElementos; i++) {

		memcpy(data+offset, (t_indice_codigo*)indiceCodigo + i, valor=sizeof(t_indice_codigo));offset+=valor;
/*		memcpy(data+offset, &indiceCodigo->offset, valor=sizeof(int)); offset+=valor;
		memcpy(data+offset, &indiceCodigo->size, valor=sizeof(int)); offset+=valor;*/
	}

	t_stream* stream = malloc(sizeof(t_stream));
	stream->length = tamElementos;
	stream->data = data;

	return stream;
}

t_stream* indiceStack_serializer(t_stack* indiceStack){
	int cantElementos = list_size(indiceStack);

	//t_indice_codigo* iCodigo = list_get(indiceCodigo, 0);
	int i;
	int offset = 0;
	char* data = NULL;
	char* dataAux;
	for (i = 0; i < cantElementos; ++i) {
		t_element_stack* elementStack = list_get(indiceStack, i);
		t_stream* stream_elementStack = elementStack_serializer(elementStack);
		//int offset = (sizeof(uint16_t)+stream_elementStack->length)*i;
		dataAux = malloc(offset+stream_elementStack->length);
		if(offset > 0){
			memcpy(dataAux, data, offset);
			free(data);
		}

		int valor = 0;
		//memcpy(dataAux+offset, &stream_elementStack->length, valor = sizeof(int)); offset+=valor;
		memcpy(dataAux+offset, stream_elementStack->data, valor = stream_elementStack->length); offset+=valor;

		data = malloc(offset);
		memcpy(data, dataAux, offset);
		free(dataAux);
	}

	t_stream* stream = malloc(sizeof(t_stream));
	stream->length = sizeof(int)+offset;
	stream->data = malloc(stream->length);
	memcpy(stream->data, &cantElementos, sizeof(int));
	memcpy(stream->data+sizeof(int), data, offset);
	//memcpy(stream->data+offset, &zero, sizeof(int));//se agrega "length = 0" como fin de la lista
	//free(data);

	return stream;
}

t_stream* elementStack_serializer(t_element_stack* elementStack){
	t_stream* stream_args = argsVars_serializer(elementStack->args);
	t_stream* stream_vars = argsVars_serializer(elementStack->vars);
	int tamElementos = sizeof(int)+sizeof(posicion_memoria)+stream_args->length+stream_vars->length;
	int offset = 0;
	int valor = 0;
	char* data = malloc(tamElementos);
	memcpy(data, &elementStack->retPos, valor=sizeof(int)); offset+=valor;
	memcpy(data+offset, &elementStack->retVar->pagina, valor=sizeof(int)); offset+=valor;
	memcpy(data+offset, &elementStack->retVar->offset, valor=sizeof(int)); offset+=valor;
	memcpy(data+offset, &elementStack->retVar->size, valor=sizeof(int)); offset+=valor;
	//memcpy(data+offset, &stream_args->length, valor=sizeof(int)); offset+=valor;
	memcpy(data+offset, stream_args->data, valor=stream_args->length); offset+=valor;
	//memcpy(data+offset, &stream_vars->length, valor=sizeof(int)); offset+=valor;
	memcpy(data+offset, stream_vars->data, valor=stream_vars->length); offset+=valor;

	t_stream* stream = malloc(sizeof(t_stream));
	stream->length = offset;
	stream->data = data;

	return stream;
}

t_stream* argsVars_serializer(t_list* argsVars){
	int cantElementos = list_size(argsVars);
	int tamElementos = (sizeof(t_args_vars)*cantElementos);
	char* data = malloc(tamElementos);
	int offset = 0;
	int valor = 0;
	t_args_vars* element;
	//t_indice_codigo* iCodigo = list_get(indiceCodigo, 0);
	int i;
	for (i = 0; i < cantElementos; ++i) {
		element = list_get(argsVars, i);
		memcpy(data+offset, &element->id, valor=sizeof(char)); offset+=valor;
		memcpy(data+offset, &element->pagina, valor=sizeof(int)); offset+=valor;
		memcpy(data+offset, &element->offset, valor=sizeof(int)); offset+=valor;
		memcpy(data+offset, &element->size, valor=sizeof(int)); offset+=valor;
	}

	t_stream* stream = malloc(sizeof(t_stream));
	stream->length = sizeof(int)+offset;
	stream->data = malloc(stream->length);
	memcpy(stream->data, &cantElementos, sizeof(int));
	memcpy(stream->data+sizeof(int), data, offset);

	return stream;
}



