/*
 * connect.c
 *
 *  Created on: 9/4/2017
 *      Author: utnso
 */

#include <commons/config.h>
#include <errno.h>
#include <netdb.h>
#include <sys/socket.h>
#include "socket.h"
#include "generales.h"

int open_socket(int backlog, int port) {

	struct sockaddr_in server;
	int yes = 1;
	int listenning_socket = socket(AF_INET, SOCK_STREAM, 0);

	server.sin_family = AF_INET;
	server.sin_addr.s_addr = INADDR_ANY;
	server.sin_port = htons(port);

	setsockopt(listenning_socket, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int));

	if (bind(listenning_socket, (struct sockaddr *) &server, sizeof(server)) == -1) {
		fprintf("socket - error en el bind() (error de tipo: %d)\n", errno);
		close(listenning_socket);
	}

	if (listen(listenning_socket, backlog) == -1) {
		fprintf("socket - error en el listen() (error de tipo: %d)\n", errno);
		close(listenning_socket);
	};

	return listenning_socket;
}

int close_socket(int listenning_socket) {
	return close(listenning_socket);
}

int accept_connection(int listenning_socket) {
	struct sockaddr_in client;
	int c = sizeof(struct sockaddr_in);
	int client_sock = accept(listenning_socket, (struct sockaddr *) &client, (socklen_t *) &c);
	return client_sock;
}

int connect_to_socket(char * server_ip, char * server_port) {
	struct addrinfo hints;
	struct addrinfo * server_info;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;

	if (getaddrinfo(server_ip, server_port, &hints, &server_info) != 0) {
		fprintf(stderr, "socket - error en getaddrinfo()\n");
		exit(1);
	}
	int server_socket = socket(server_info->ai_family, server_info->ai_socktype, server_info->ai_protocol);
	if (server_socket == -1) {
		fprintf(stderr, "socket - error en socket()\n");
		exit(1);
	}
	if (connect(server_socket, server_info->ai_addr, server_info->ai_addrlen) == -1) {
		close(server_socket);
		fprintf(stderr, "socket - error en connect()\n");
		exit(1);
	}
	freeaddrinfo(server_info);
	return server_socket;
}

int socket_send(int * server_socket, void * buffer, int buffer_size, int flags) {
	return send(* server_socket, buffer, buffer_size, flags);
}

int socket_recv(int * client_socket, void * buffer, int buffer_size) {
	return recv(* client_socket, buffer, buffer_size, MSG_WAITALL);
}

int socket_write(int * client_socket, void * response, int response_size) {
	return write(* client_socket, response, response_size);
}

int close_client(int client_socket) {
	return close(client_socket);
}

int connection_send(int file_descriptor, uint8_t operation_code, void* message){
/**	╔══════════════════════════════════════════════╦══════════════════════════════════════════╦══════════════════════════════╗
	║ operation_code_value (operation_code_length) ║ message_size_value (message_size_length) ║ message (message_size_value) ║
	╚══════════════════════════════════════════════╩══════════════════════════════════════════╩══════════════════════════════╝ **/

	uint8_t operation_code_value = operation_code;
	uint32_t message_size_value;

	int i= 0;

	switch ((int)operation_code) {
		case OC_PCB:
		case OC_DESCONEX_CPU:
		case OC_ERROR_EJECUCION_CPU:
		case OC_TERMINA_PROGRAMA:
		case OC_TERMINO_INSTRUCCION:
			message_size_value = ((t_stream*) message)->length;
			message = ((t_stream*) message)->data;
			break;
		case OC_CODIGO:
		case OC_SOLICITUD_PROGRAMA_NUEVO:
			//message_size_value = *(uint8_t*) message;
			//(uint8_t *)message++;
			message_size_value = strlen((char*)message);
			break;
		case OC_NUEVA_CONSOLA_PID:
			message_size_value = sizeof(uint8_t);
			break;
		case OC_RESP_ESCRIBIR:
			message_size_value = sizeof(int8_t);
			break;
		case OC_QUANTUM_SLEEP:
		case OC_RESP_QUANTUM_SLEEP:
		case OC_MEMORIA_INSUFICIENTE:
		case OC_SOLICITUD_MEMORIA:
		case OC_LIBERAR_MEMORIA:
		case OC_RESP_WAIT:
		case OC_RESP_SIGNAL:
		case OC_HANDSHAKE_MEMORY:
			message_size_value = sizeof(uint8_t);
			break;
		case OC_FUNCION_ABRIR:
			message_size_value = sizeof(int) + sizeof(uint16_t) + *(int*)message + sizeof(t_banderas);
			break;
		case OC_FUNCION_BORRAR:
		case OC_FUNCION_CERRAR:
			message_size_value = sizeof(t_archivo);
			break;
		case OC_RESP_ABRIR:
		case OC_MUERE_PROGRAMA:
		case OC_RESP_TERMINO_INSTRUCCION:
		case OC_RESP_LEER_ERROR:
		case OC_RESP_BORRAR:
		case OC_RESP_CERRAR:
		case HANDSHAKE_CPU:
		case OC_RESP_LIBERAR:
			message_size_value = sizeof(int);
			break;
		case OC_FUNCION_RESERVAR:
			message_size_value = sizeof(t_pedido_reservar_memoria);
			break;
		case OC_RESP_RESERVAR:
			message_size_value = sizeof(t_puntero) * 2;
			break;
		case OC_RESP_LEER_VARIABLE:
			message_size_value = sizeof(t_valor_variable);
			break;
		case OC_FUNCION_LIBERAR:
			message_size_value = sizeof(t_pedido_liberar_memoria);
			break;
		case OC_FUNCION_ESCRIBIR:
			//message_size_value = sizeof(t_archivo);
			message_size_value = *(size_t *)message;
			//message++;
			break;
		case OC_ESCRIBIR_EN_CONSOLA:
			message_size_value = strlen((char*)message);
			break;
		case OC_FUNCION_LEER:
			message_size_value = sizeof(t_pedido_archivo_leer);
			break;
		case OC_RESP_LEER:
			//message_size_value = ((t_read_response *)message)->buffer_size;
			//message = ((t_read_response *)message)->buffer;
			message_size_value = sizeof(int);
			break;
		case OC_FUNCION_ESCRIBIR_VARIABLE:
			message_size_value = *(int*) message;

			message_size_value = sizeof(int)+message_size_value*sizeof(t_nombre_variable)+sizeof(int);
			break;
		case OC_FUNCION_LEER_VARIABLE:
		case OC_FUNCION_SIGNAL:
		case OC_FUNCION_WAIT:
			message_size_value = strlen((char *)message);
			break;
		case OC_FUNCION_MOVER_CURSOR:
			message_size_value = sizeof(t_descriptor_archivo) + sizeof(t_valor_variable) + sizeof(int);
			break;
		case OC_KILL_CONSOLA: {
			message_size_value = sizeof(int);
		}
//		DEFINIR COMPORTAMIENTO
		default:
			printf("ERROR: Socket %d, Invalid operation code...\n", file_descriptor);
			break;
	}

	uint8_t operation_code_length = sizeof(uint8_t);
	uint8_t message_size_length = sizeof(uint32_t);
	void * buffer = malloc(operation_code_length + message_size_length + message_size_value);
	memcpy(buffer, &operation_code_value, operation_code_length);
	memcpy(buffer + operation_code_length, &message_size_value, message_size_length);
	memcpy(buffer + operation_code_length + message_size_length, message, message_size_value);
	int ret = send(file_descriptor, buffer, operation_code_length + message_size_length + message_size_value, 0);
	free(buffer);

	return ret;
}

int connection_recv(int file_descriptor, uint8_t* operation_code_value, void** message){
/**	╔══════════════════════════════════════════════╦══════════════════════════════════════════╦══════════════════════════════╗
	║ operation_code_value (operation_code_length) ║ message_size_value (message_size_length) ║ message (message_size_value) ║
	╚══════════════════════════════════════════════╩══════════════════════════════════════════╩══════════════════════════════╝ **/

	uint8_t prot_ope_code_size = sizeof(uint8_t);
	uint8_t prot_message_size = sizeof(uint32_t);
	t_puntero respuesta;
	t_pedido_reservar_memoria* reservar;
	t_pedido_liberar_memoria* liberar;
	uint32_t message_size;
	int status = 1;
	int ret = 0;
	int stream_length;
	char* buffer;

	status = recv(file_descriptor, operation_code_value, prot_ope_code_size, MSG_WAITALL);
	if (status <= 0) {
		printf("ERROR: Socket %d, disconnected...\n", file_descriptor);
	} else {
		ret = ret + status;
		status = recv(file_descriptor, &message_size, prot_message_size, MSG_WAITALL);
		if (status <= 0) {
			printf("ERROR: Socket %d, no message size...\n", file_descriptor);
		} else {
			ret = ret + status;
			switch ((int)*operation_code_value) {
			case OC_PCB:
			case OC_DESCONEX_CPU:
			case OC_TERMINA_PROGRAMA:
			case OC_ERROR_EJECUCION_CPU:
			case OC_TERMINO_INSTRUCCION:
				buffer = malloc(message_size);
				recv(file_descriptor, buffer,message_size, MSG_WAITALL);
				*message = buffer;
				break;
			case OC_SOLICITUD_PROGRAMA_NUEVO:
				*message = malloc(message_size +1);
				buffer = (char*)*message;
				status = recv(file_descriptor, buffer, message_size, MSG_WAITALL);
				buffer[message_size] = '\0';
				break;
			case OC_RESP_BORRAR:
			case OC_RESP_CERRAR:
			case OC_RESP_TERMINO_INSTRUCCION:
			case OC_RESP_ABRIR:
			case OC_MUERE_PROGRAMA:
			case OC_RESP_LEER_ERROR:
			case HANDSHAKE_CPU:
			case OC_RESP_LIBERAR:
				buffer = malloc(message_size);
				recv(file_descriptor, buffer, message_size, MSG_WAITALL);
				*message = (int*)buffer;
				break;
			case OC_FUNCION_ABRIR:
				buffer = malloc(message_size);
				recv(file_descriptor, buffer, message_size, MSG_WAITALL);
				*message = buffer;
				break;
			case OC_RESP_ESCRIBIR:
				buffer = malloc(message_size);
				recv(file_descriptor, buffer, message_size, MSG_WAITALL);
				*message = (int8_t *)buffer;
				break;
			case OC_NUEVA_CONSOLA_PID:
				buffer = malloc(message_size);
				recv(file_descriptor, buffer, message_size, MSG_WAITALL);
				*message = (uint8_t *)buffer;
				break;
			case OC_FUNCION_LEER_VARIABLE:
			case OC_MEMORIA_INSUFICIENTE:
			case OC_SOLICITUD_MEMORIA:
			case OC_LIBERAR_MEMORIA:
				buffer = malloc(message_size + 1);
				if(message_size > 0){
					status = recv(file_descriptor, buffer, message_size, MSG_WAITALL);
				}
				if(status > 0){
					buffer[message_size] = '\0';
					*message = buffer;
				}
				//free(buffer);
				break;
			case OC_FUNCION_LIBERAR:
				buffer = malloc(sizeof(t_pedido_liberar_memoria));
				status = recv(file_descriptor, buffer, message_size, MSG_WAITALL);
				*message = (t_pedido_liberar_memoria*) buffer;
				break;
			case OC_HANDSHAKE_MEMORY:
				buffer = malloc(message_size);
				recv(file_descriptor, buffer, message_size, MSG_WAITALL);
				*message = (uint8_t *)buffer;
				break;
			case OC_FUNCION_RESERVAR:
				buffer = malloc(sizeof(t_pedido_reservar_memoria));
				recv(file_descriptor, buffer, message_size, MSG_WAITALL);
				*message = (t_pedido_reservar_memoria*) buffer;
				break;
			case OC_RESP_RESERVAR:
				buffer = malloc(sizeof(t_puntero) * 2);
				recv(file_descriptor, buffer, message_size,MSG_WAITALL);
				*message = buffer;
				break;
			case OC_FUNCION_ESCRIBIR:
				buffer = malloc(sizeof(t_size));
				recv(file_descriptor, buffer, sizeof(size_t), MSG_WAITALL);
				free(buffer);

				buffer = malloc(sizeof(t_descriptor_archivo));
				recv(file_descriptor, buffer, sizeof(t_descriptor_archivo), MSG_WAITALL);
				t_archivo * arch = malloc(sizeof(t_archivo));
				arch->descriptor_archivo = *(t_descriptor_archivo*)buffer;

				free(buffer);
				buffer = malloc(sizeof(int));
				recv(file_descriptor, buffer, sizeof(int), MSG_WAITALL);
				arch->pid = *(int*)buffer;

				free(buffer);
				buffer = malloc(sizeof(size_t));
				recv(file_descriptor, buffer, sizeof(size_t), MSG_WAITALL);
				arch->tamanio = *(t_valor_variable*)buffer;

				free(buffer);
				buffer = malloc(arch->tamanio);
				arch->informacion = malloc(arch->tamanio);
				recv(file_descriptor, buffer, arch->tamanio, MSG_WAITALL);

				memcpy(arch->informacion, buffer, arch->tamanio);
				free(buffer);

				*message = arch;

				break;
			case OC_ESCRIBIR_EN_CONSOLA:
			case OC_FUNCION_SIGNAL:
			case OC_FUNCION_WAIT:
				buffer = malloc(message_size+1);
				status = recv(file_descriptor, buffer, message_size, MSG_WAITALL);
				buffer[message_size] = '\0';
				*message = buffer;
				break;
			case OC_RESP_LEER_VARIABLE:
				buffer = malloc(message_size);
				recv(file_descriptor, buffer, message_size, MSG_WAITALL);
				*message = (t_valor_variable *)buffer;
				break;
			case OC_FUNCION_LEER:
				buffer = malloc(message_size);
				recv(file_descriptor, buffer, message_size, MSG_WAITALL);
				*message = buffer;
				break;
			case OC_RESP_LEER:
				/*buffer = malloc(message_size);
				recv(file_descriptor, *message, buffer, 0);
				*message = buffer;*/
				buffer = malloc(message_size);
				recv(file_descriptor, buffer, message_size,MSG_WAITALL);
				*message = buffer;
				break;
			case OC_FUNCION_ESCRIBIR_VARIABLE:
				buffer = malloc(message_size);
				recv(file_descriptor, buffer, message_size, MSG_WAITALL);
				*message = buffer;
				break;
			case OC_FUNCION_MOVER_CURSOR:
				buffer = malloc(message_size);
				recv(file_descriptor, buffer, message_size, MSG_WAITALL);
				*message = buffer;
				break;
			case OC_RESP_SIGNAL:
			case OC_RESP_WAIT:
				buffer = malloc(message_size);
				recv(file_descriptor, buffer, message_size, MSG_WAITALL);
				*message = buffer;
				break;
			case OC_FUNCION_BORRAR:
			case OC_FUNCION_CERRAR:
				buffer = malloc(message_size);
				recv(file_descriptor, buffer, message_size, MSG_WAITALL);
				*message = buffer;
				break;
			case OC_KILL_CONSOLA: {
				buffer = malloc(message_size);
				recv(file_descriptor, buffer, message_size, MSG_WAITALL);
				*message = buffer;
				break;
			}
			default:
				printf("ERROR: Socket %d, Invalid operation code(%d)...\n", file_descriptor, (int)*operation_code_value);
				break;
			}

			if (status <= 0) {
				printf("ERROR: Socket %d, no message...\n", file_descriptor);
				ret = status;
			}else{
				ret = ret + status;
			}

		}
	}

	return ret;
}
