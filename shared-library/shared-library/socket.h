/*
 * socket.h
 *
 *  Created on: 9/4/2017
 *      Author: utnso
 */

#ifndef SHARED_LIBRARY_SOCKET_H_
#define SHARED_LIBRARY_SOCKET_H_

#include <commons/log.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netdb.h>
#include <sys/time.h>
#include <unistd.h>
#include "memory_prot.h"
#include "syscall.h"


#define OC_SOLICITUD_PROGRAMA_NUEVO 1
#define OC_MEMORIA_INSUFICIENTE 2
#define OC_SOLICITUD_MEMORIA 3
#define OC_LIBERAR_MEMORIA 4
#define OC_ASIGNA_PCB 5
#define OC_HANDSHAKE_MEMORY 8
#define OC_NUEVA_CONSOLA_PID 9
#define OC_CODIGO 11
#define OC_INSTRUCCION_CONSOLA 12
#define OC_KILL_CONSOLA 13
#define OC_PCB 14
#define OC_QUANTUM_SLEEP 15
#define OC_RESP_QUANTUM_SLEEP 16

//FUNCIONES HACIA KERNEL
#define OC_FUNCION_ABRIR 30
#define OC_FUNCION_CERRAR 31
#define OC_FUNCION_BORRAR 32
#define OC_FUNCION_LEER 33
#define OC_FUNCION_ESCRIBIR 34
#define OC_FUNCION_WAIT 35
#define OC_FUNCION_SIGNAL 36
#define OC_FUNCION_RESERVAR 37
#define OC_FUNCION_LIBERAR 38
#define OC_FUNCION_MOVER_CURSOR 39
#define OC_FUNCION_ESCRIBIR_VARIABLE 50
#define OC_FUNCION_LEER_VARIABLE 51

//FUNCIONES DESDE KERNEL
#define OC_RESP_ABRIR 40
#define OC_RESP_CERRAR 41
#define OC_RESP_BORRAR 42
#define OC_RESP_LEER 43
#define OC_RESP_ESCRIBIR 44
#define OC_RESP_WAIT 45
#define OC_RESP_SIGNAL 46
#define OC_RESP_RESERVAR 47
#define OC_RESP_LIBERAR 48
#define OC_RESP_MOVER_CURSOR 49
#define OC_RESP_ESCRIBIR_VARIABLE 52
#define OC_RESP_LEER_VARIABLE 53
#define OC_RESP_TERMINO_INSTRUCCION 54
#define OC_ESCRIBIR_EN_CONSOLA 55
#define OC_MUERE_PROGRAMA 99
#define OC_RESP_LEER_ERROR 63

//VALORES DE RETORNO DEL CPU
#define OC_TERMINA_PROGRAMA 6
#define OC_TERMINO_INSTRUCCION 7
#define OC_DESCONEX_CPU 60
#define OC_ERROR_EJECUCION_CPU 61
#define HANDSHAKE_CPU 100
	/**
	 * @NAME   open_socket
	 * @DESC   Permite crear un socket "servidor" o "de escucha" El mismo abre un puerto en la red y después espera a que un cliente se conecte a ese puerto
	 *
	 * @PARAMS int backlog : máximo número de conexiones de clientes
	 * 		   int port    : puerto que va a "escuchar" conexiones de clientes
	 *
	 * @RETURN int : file descriptor del nuevo socket
	 */
	int open_socket(int, int);

	/**
	 * @NAME   close_socket
	 * @DESC   Cierra el socket
	 *
	 * @PARAMS int listenning_socket : file descriptor del socket a cerrar
	 *
	 * @RETURN int : 0 funcionamiento normal, -1 en caso de error
	 */
	int close_socket(int);

	/**
	 * @NAME:  accept_connection
	 * @DESC:  Acepta la conexión de un cliente al socket
	 *
	 * @PARAMS int listenning_socket : file descriptor del socket que quiere aceptar la conexión
	 *
	 * @RETURN int : file descriptor del nuevo cliente
	 */
	int accept_connection(int);

	/**
	 * @NAME:  connect_to_socket
	 * @DESC:  Permite conectarse a un socket servidor
	 *
	 * @PARAMS char * server_ip   : ip socket servidor
	 * 		   char * server_port : puerto socket servidor
	 *
	 * @RETURN int : file descriptor del socket servidor
	 */
	int connect_to_socket(char *, char *);

	/**
	 * @NAME:  socket_send
	 * @DESC:  Permite enviar un mensaje a un socket servidor
	 *
	 * @PARAMS int * server_socket : file descriptor del socket servidor
	 *		   void * buffer       : buffer con el mensaje
	 *		   int buffer_size	   : tamaño del buffer
	 *		   int flags           : flags
	 *
	 * @RETURN int : 0 funcionamiento normal, -1 en caso de error
	 */
	int socket_send(int *, void *, int, int);

	/**
	 * @NAME:  socket_recv
	 * @DESC:  Permite recibir un mensaje de un socket cliente
	 *
	 * @PARAMS int * client_socket : file descriptor del socket cliente
	 *		   void * buffer       : buffer destinatario del mensaje
	 *		   int buffer_size	   : tamaño del buffer
	 *
	 * @RETURN int : 0 funcionamiento normal, -1 en caso de error
	 */
	int socket_recv(int *, void *, int);

	/**
	 * @NAME:  socket_write
	 * @DESC:  Permite enviar un mensaje de un socket cliente
	 *
	 * @PARAMS int * client_socket : file descriptor del socket cliente
	 *		   void * response     : buffer con el mensaje
	 *		   int response_size   : tamaño del buffer
	 *
	 * @RETURN int : 0 funcionamiento normal, -1 en caso de error
	 */
	int socket_write(int *, void *, int);

	/**
	 * @NAME:  close_client
	 * @DESC:  Cierra la conexion de un cliente
	 *
	 * @PARAMS int * client_socket : file descriptor del socket cliente a cerrar
	 *
	 * @RETURN int : 0 funcionamiento normal, -1 en caso de error
	 */
	int close_client(int);

	/**
	 * @NAME: connection_send
	 * @DESC:
	 *
	 * @PARAMS 	int file_descriptor: file descriptor del receptor del mensaje
	 * 			uint8_t operation_code: codigo de operacion del mensaje. Define como serializar el payload
	 * 			void* message: payload a mandar
	 * @RETURN int : cantidad de bytes mandados; -1 en caso de error
	 *
	 */
	int connection_send(int, uint8_t, void*);

	/**
	 * @NAME: connection_recv
	 * @DESC:
	 *
	 * @PARAMS 	int file_descriptor: file descriptor del receptor del mensaje
	 * 			uint8_t operation_code: codigo de operacion que se recibira primero. Define como desserializar el payload
	 * 			void** message: buffer con lo recibido
	 */
	int connection_recv(int, uint8_t*, void**);

#endif /* SHARED_LIBRARY_SOCKET_H_ */
