/*
 * helper.h
 *
 *  Created on: 3/10/2017
 *      Author: utnso
 */

#ifndef HELPER_H_
#define HELPER_H_

	t_list* lista_worker;
	t_list* tabla_de_estados;

	int count_tde;
	int count_job;

	typedef struct{
		int port;
		fd_set* master;
	//	fd_set lectura;
	}t_struct;

	typedef struct{
		int nodo;
		int bloque;
	}t_NB;

	typedef struct{
		char* nombre;
		int	bloque;
		t_NB* original;
		t_NB* copia;
		long bytes_ocupados;
	}t_info_archivo;

	typedef struct{
		int job;
		int master;
		int nodo;
		int bloque;
		char* etapa;
		char* archivo_temporal;
		char* estado;
	}t_registro_TDE;

	typedef struct{
		int nodo;
		char* ip;
		char* puerto;
		int bloque;
		int bytes_ocupados;
		char* archivo_temporal;
	}t_registro_transformacion;

	typedef struct{
		int id;
		char* ip;
		char* puerto;
	}t_worker_info;

	t_struct* create_struct();
	t_registro_TDE* registro_TDE_create();
	t_info_archivo* info_archivo_create();
	void info_archivo_destroy(*t_info_archivo);
	t_list* planificar_transformacion(t_list* infoArchivo);
	void algoritmo_planificacion(t_list* lista_transformaciones, t_list* list_infoArchivo);
	int enviar_lista_transformacion(t_list* lista_transformaciones);

#endif /* HELPER_H_ */
