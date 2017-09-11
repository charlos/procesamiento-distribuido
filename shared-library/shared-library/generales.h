/*
 * generales.h
 *
 *  Created on: 18/4/2017
 *      Author: utnso
 */

#ifndef GENERALES_H_
#define GENERALES_H_

#include <commons/log.h>
#include <commons/string.h>
#include <commons/collections/list.h>
#include <commons/collections/queue.h>
#include <commons/collections/dictionary.h>
#include <parser/metadata_program.h>
#include <stdint.h>


//Exit Code de proceso
#define EC_FINALIZACION_OK 		0
#define EC_SIN_RECURSOS_PARA_EJECUTAR -1
#define EC_NO_EXISTE_ARCHIVO 		-2
#define EC_SIN_PERMISO_LECTURA		-3
#define EC_SIN_PERMISO_ESCRITURA	-4
#define EC_EXCEPCION_MEMORIA		-5
#define EC_DESCONEXION_CONSOLA		-6
#define EC_FINALIZADO_POR_CONSOLA	-7
#define EC_ALOCAR_MUY_GRANDE		-8
#define EC_SIN_PAGINAS_PROCESO		-9
#define EC_STACKOVERFLOW			-10
#define EC_DESCONOCIDO				-20
#define EC_ERROR_CONEXION			-19
#define EC_ARCHIVO_NO_ABIERTO		-21
#define EC_FS_LLENO					-22
#define EC_ARCHIVO_ABIERTO_OTROS    -23
#define EC_SIN_ESPACIO_MEMORIA      -24

typedef t_list t_stack;

typedef struct{
	char* nombre;
	int valor;
}t_shared_var;

typedef struct{
	int offset;
	int size;
}t_indice_codigo;

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

//typedef struct{
//	int pid;
//	int PC;
//	int cantidad_paginas;
//	uint16_t SP;
//	uint16_t cantidad_instrucciones;
//	t_stack* indice_stack;
//	t_indice_codigo* indice_codigo;
//	t_dictionary* indice_etiquetas;
//	int exit_code;
//}t_PCB;

typedef struct{
	int pagina;
	int offset;
	int size;
}posicion_memoria;

typedef struct{
	char id;
	int pagina;
	int offset;
	int size;
}__attribute__ ((__packed__))t_args_vars;

typedef struct{
	int retPos;
	posicion_memoria* retVar;
	t_list* args;
	t_list* vars;
} t_element_stack;

typedef struct{
	int length;
	char* data;
}__attribute__ ((__packed__))t_stream;

typedef struct {
	t_descriptor_archivo descriptor_archivo;
	void * informacion;
	t_valor_variable tamanio;
	int pid;
} t_archivo;

typedef struct {
	t_descriptor_archivo descriptor_archivo;
	t_puntero informacion;
	t_valor_variable tamanio;
	int pid;
} t_pedido_archivo_leer;

typedef struct{
	int proceso_fd;
	int global_fd;
	t_banderas flags;
	int offset_cursor;
} t_process_file;

typedef struct{
	int pid;
	t_list* tabla_archivos;
	uint8_t contador_fd;
} t_table_file;


typedef struct{
	char* file;
	int open;
	int global_fd;
} t_global_file;

typedef struct{
	int pid;
	int espacio_pedido;
}t_pedido_reservar_memoria;

typedef struct{
	int pid;
	int nro_pagina;
	int posicion;
}t_pedido_liberar_memoria;

char* obtener_nombre_proceso(char*);

	/**
	 * @NAME: crear_logger
	 * @DESC: crea una instancia de logger
	 *
	 * @PARAMS char* path: path del proceso ejecutado. Tomara su nombre para el archivo .log. Siempre se va a usar argv[0]
	 * 			t_log** logger: puntero a direccion de logger
	 * 			bool console: determina si los logs se mostraran por pantalla
	 * 			t_log_level: tipo de seguimiento que se quiere en el log
	 */
void crear_logger(char*, t_log**, bool, t_log_level);
int serializar_y_enviar_PCB(t_PCB* , int , int );
void pcb_destroy(t_PCB* pcb);

#endif /* GENERALES_H_ */
