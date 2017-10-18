/*
 * helper.c
 *
 *  Created on: 3/10/2017
 *      Author: utnso
 */
#include "helper.h"
#include <stdlib.h>

t_struct* create_struct(){
	t_struct* estructura = malloc(sizeof(t_struct));
	return estructura;
}

t_info_archivo* info_archivo_create(){
	return malloc(t_info_archivo);
}

t_list* planificar_transformacion(t_list* list_infoArchivo){
	t_list* lista_transformaciones = list_create();
	algoritmo_planificacion(lista_transformaciones, list_infoArchivo);
	return lista_transformaciones;
}

void algoritmo_planificacion(t_list* lista_transformaciones, t_list* list_infoArchivo){
	void _list_elements(t_info_archivo* infoArchivo) {
		t_worker_info* worker = find_worker_by_id(infoArchivo->original->nodo);
		if(worker == NULL){
			//TODO: accion para cuando no se encuentra el worker buscado
		} else {
			count_tde++;
			t_registro_transformacion* registro_transformacion = registro_transformacion_create();
			registro_transformacion->nodo = worker->id;
			registro_transformacion->ip = worker->ip;
			registro_transformacion->puerto = worker->puerto;
			registro_transformacion->bloque = infoArchivo->original->bloque;
			registro_transformacion->bytes_ocupados = infoArchivo->bytes_ocupados;

			char* nombre = string_new();
			string_append(nombre, "/tmp/");
			string_append(nombre, "J");
			string_append(nombre, string_itoa(count_job_get()));
			string_append(nombre, "N");
			string_append(nombre, string_itoa(registro_transformacion->nodo));
			string_append(nombre, "B");
			string_append(nombre, string_itoa(registro_transformacion->bloque));
			string_append(nombre, "E");
			string_append(nombre, "TF");

			registro_transformacion->archivo_temporal = nombre;
			list_add(lista_transformaciones, registro_transformacion);
		}
	}

	count_job_increment();
	list_iterate(list_infoArchivo, (void*) _list_elements);
}

t_worker_info *find_worker_by_id(int worker_id) {
	int _is_worker_id(t_worker_info *worker) {
		return worker->id == worker_id;
	}

	return list_find(lista_worker, (void*) _is_worker_id);
}

//TODO: se deberian agregar semaforos binarios en estas funciones (?)
void count_job_initialize(){

	count_job = 0;
}
int count_job_get(){
	return count_job;
}
void count_job_increment(){
	count_job++;
}

void TDE_create(){
	tabla_de_estados = list_create();
}
void TDE_add_transformaciones(t_list* lista_transformaciones){
    t_list* lista_job = list_create();
	void _list_elements(t_registro_transformacion* reg_transformacion) {

		t_registro_TDE* reg_tde = registro_TDE_create();
		reg_tde->job = count_job_get();
		reg_tde->master = 1; /*TODO: se debe definir de donde sacar el master*/
		reg_tde->nodo = reg_transformacion->nodo;
		reg_tde->bloque = reg_transformacion->bloque;
		reg_tde->etapa = string_from_format("%s", "transformacion");
		reg_tde->archivo_temporal = reg_transformacion->archivo_temporal;
		reg_tde->estado = string_from_format("%s", "en proceso");

		list_add(lista_job, reg_tde);
    }

    list_iterate(lista_transformaciones, (void*) _list_elements);

	list_add_all(tabla_de_estados, lista_job);
}
//-------------------------------------------------------------------

int enviar_lista_transformacion(t_list* lista_transformaciones){
	//se agregan todas las transformaciones a la tabla de estado
	TDE_add_transformaciones(lista_transformaciones);

	//se envia la respuesta a master
	//...
}



