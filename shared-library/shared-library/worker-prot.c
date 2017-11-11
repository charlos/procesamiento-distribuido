/*
 * worker-prot.c
 *
 *  Created on: 19/9/2017
 *      Author: Gustavo Tofaletti
 */
#include <commons/string.h>
#include <commons/log.h>
#include <commons/collections/list.h>
#include <stdint.h>
#include <stdlib.h>
#include "socket.h"
#include "worker-prot.h"


void request_send_resp(int * master_socket, int status) {
	uint8_t resp_prot_code = 2;
	int response_size = sizeof(char) * (resp_prot_code);
	void * response = malloc(response_size);
	memcpy(response, &status, resp_prot_code);
	socket_write(master_socket, response, response_size);
	free(response);
}

int transform_req_send(int worker_socket, int block, int used_size, char* result_file, int script_size, void* script, t_log * logger) {

	uint8_t prot_ope_code = 1;
	uint8_t prot_block = 4;
	uint8_t prot_used_size = 4;
	uint8_t prot_result_file = 4;
	uint8_t prot_script_size = 4;

	uint8_t  req_ope_code = TRANSFORM_OC;
	uint32_t req_block = block;
	uint32_t req_used_size = used_size;
	uint32_t req_result_file = string_length(result_file)+1;
	uint32_t req_script_size = script_size;

	int msg_size = sizeof(char) * (prot_ope_code + prot_block + prot_used_size + prot_result_file + req_result_file + 1 + prot_script_size + req_script_size);
	void * request = malloc(msg_size);

	memcpy(request, &req_ope_code, prot_ope_code);
	memcpy(request + prot_ope_code, &req_block, prot_block);
	memcpy(request + prot_ope_code + prot_block, &req_used_size, prot_used_size);
	memcpy(request + prot_ope_code + prot_block + prot_used_size, &req_result_file, prot_result_file);
	memcpy(request + prot_ope_code + prot_block + prot_used_size + prot_result_file, result_file, req_result_file);
	memcpy(request + prot_ope_code + prot_block + prot_used_size + prot_result_file + req_result_file, &req_script_size, prot_script_size);
	memcpy(request + prot_ope_code + prot_block + prot_used_size + prot_result_file + req_result_file + prot_script_size, script, req_script_size);
	socket_send(&worker_socket, request, msg_size, 0);
	free(request);

	uint8_t resp_prot_code = 2;
	uint16_t code;
	int received_bytes = socket_recv(&worker_socket, &code, resp_prot_code);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ SERVER %d >> disconnected", worker_socket);
		return DISCONNECTED_SERVER;
	}
	return code;

}


t_request_transformation * transform_req_recv(int * client_socket, t_log * logger) {
	t_request_transformation * request = malloc(sizeof(t_request_transformation));

	uint8_t prot_block = 4;
	uint8_t prot_used_size = 4;
	uint8_t prot_result_file = 4;
	uint8_t prot_script_size = 4;

	uint32_t result_file_size;

	int received_bytes = socket_recv(client_socket, &(request->block), prot_block);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}

	received_bytes = socket_recv(client_socket, &(request->used_size), prot_used_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}

	received_bytes = socket_recv(client_socket, &result_file_size, prot_result_file);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}

	request->result_file = malloc(sizeof(char)*result_file_size);

	received_bytes = socket_recv(client_socket, request->result_file, result_file_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}

	received_bytes = socket_recv(client_socket, &(request->script_size), prot_script_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	request->script = malloc(sizeof(char) * request->script_size);
	received_bytes = socket_recv(client_socket, request->script, request->script_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	request->exec_code = SUCCESS;
	return request;
}

int local_reduction_req_send(int worker_socket, char* temp_files, char* result_file, int script_size, void* script, t_log * logger) {
	// char* temp_files ---> cadena que contiene lista de los archivos separados por ";" para poder hacer un split luego

	uint8_t prot_ope_code = 1;
	uint8_t prot_temp_files = 4;
	uint8_t prot_result_file = 4;
	uint8_t prot_script_size = 4;

	uint8_t  req_ope_code = REDUCE_LOCALLY_OC;
	uint32_t req_temp_files = string_length(temp_files)+1;
	uint32_t req_result_file = string_length(result_file)+1;
	uint32_t req_script_size = script_size;

	int msg_size = sizeof(char) * (prot_ope_code + prot_temp_files + req_temp_files + prot_result_file + req_result_file + prot_script_size + req_script_size);
	void * request = malloc(msg_size);

	memcpy(request, &req_ope_code, prot_ope_code);
	memcpy(request + prot_ope_code, &req_temp_files, prot_temp_files);
	memcpy(request + prot_ope_code + prot_temp_files, temp_files, req_temp_files);
	memcpy(request + prot_ope_code + prot_temp_files + req_temp_files, &req_result_file, prot_result_file);
	memcpy(request + prot_ope_code + prot_temp_files + req_temp_files + prot_result_file, result_file, req_result_file);
	memcpy(request + prot_ope_code + prot_temp_files + req_temp_files + prot_result_file + req_result_file, &req_script_size, prot_script_size);
	memcpy(request + prot_ope_code + prot_temp_files + req_temp_files + prot_result_file + req_result_file + prot_script_size, script, req_script_size);
	socket_send(&worker_socket, request, msg_size, 0);
	free(request);

	uint8_t resp_prot_code = 2;
	int16_t code;
	int received_bytes = socket_recv(&worker_socket, &code, resp_prot_code);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ SERVER %d >> disconnected", worker_socket);
		return DISCONNECTED_SERVER;
	}
	return code;
}



t_request_local_reduction * local_reduction_req_recv(int * client_socket, t_log * logger) {
	t_request_local_reduction * request = malloc(sizeof(t_request_local_reduction));

	uint8_t prot_temp_files = 4;
	uint8_t prot_result_file = 4;
	uint8_t prot_script_size = 4;

	uint32_t result_file_size;
	uint32_t temp_files_size;

	int received_bytes = socket_recv(client_socket, &temp_files_size, prot_temp_files);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}

	request->temp_files = malloc(sizeof(char)*temp_files_size);

	received_bytes = socket_recv(client_socket, &(request->temp_files), temp_files_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}

	received_bytes = socket_recv(client_socket, &result_file_size, prot_result_file);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}

	request->result_file = malloc(sizeof(char)*result_file_size);

	received_bytes = socket_recv(client_socket, &(request->result_file), result_file_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}

	received_bytes = socket_recv(client_socket, &(request->script_size), prot_script_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	request->script = malloc(sizeof(char) * request->script_size);
	received_bytes = socket_recv(client_socket, request->script, request->script_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	request->exec_code = SUCCESS;
	return request;
}

int global_reduction_req_send(int worker_socket, int script_size, void *script, t_list* lista_nodos, t_log * logger){
	int prot_ope_code = 1;
	int prot_script_size = 4;
	int resp_prot_cant_elem = 4;

	uint8_t  req_ope_code = REDUCE_GLOBAL_OC;
	uint32_t req_script_size = script_size;

	// ---------------------------------------//
	int resp_size = 0;
	t_red_global * red_global;
	int i = 0;
	while (i < (lista_nodos->elements_count)) {
		red_global = (t_red_global *) list_get(lista_nodos, i);
		resp_size += sizeof(uint8_t) + (4 * sizeof(int32_t)) + (strlen(red_global->nodo) + 1)
						+ (strlen(red_global->ip_puerto) + 1) + (strlen(red_global->archivo_rl_temp) + 1) + (strlen(red_global->archivo_rg) + 1);
		i++;
	}
	int msg_size = sizeof(char) * (prot_ope_code + prot_script_size + req_script_size + resp_prot_cant_elem + resp_size);
	void * request = malloc(msg_size);

	int offset = 0;
	memcpy(request, &req_ope_code, prot_ope_code);
	offset += prot_ope_code;
	memcpy(request + offset, &req_script_size, prot_script_size);
	offset += prot_script_size;
	memcpy(request + offset, script, req_script_size);
	offset += req_script_size;
	agregar_reducciones_globales(request, offset, lista_nodos);

	int sent = socket_send(&worker_socket, request, msg_size, 0);
	if(sent < msg_size){
		if(logger)log_error(logger, "NO SE ENVIO TODO EL PAQUETE");
		return DISCONNECTED_SERVER;
	}
	free(request);

	uint8_t resp_prot_code = 2;
	int code;
	int received_bytes = socket_recv(&worker_socket, &code, resp_prot_code);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ SERVER %d >> disconnected", worker_socket);
		return DISCONNECTED_SERVER;
	}
	return code;
}

t_request_global_reduction *global_reduction_req_recv(int * client_socket, t_log * logger){
	t_request_global_reduction *request = malloc(sizeof(t_request_global_reduction));

	int received_bytes;
	int prot_script_size = 4;
	uint8_t resp_prot_cant_elem = 4;

	uint32_t *req_script_size = malloc(sizeof(uint32_t));

	received_bytes = socket_recv(client_socket, req_script_size, prot_script_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}

	request->script = malloc(sizeof(char)* (*req_script_size));

	received_bytes = socket_recv(client_socket, request->script, *req_script_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	t_list *lista_nodos = list_create();

	recv_reducciones_globales(*client_socket, lista_nodos, logger);

	request->lista_nodos_reduccion_global = lista_nodos;
	request->exec_code = EXITO;
	return request;
}

void task_response_send(int master_socket,int OC, int resp_code, t_log * logger){
//  TODO agregar identificador de pedido para los casos en que un master haga dos pedidos al mismo Worker
	uint8_t prot_OC = 2;
	uint8_t prot_resp_code = 2;

	int16_t  req_OC = OC;
	int16_t  req_resp_code = resp_code;

	int msg_size = sizeof(int) * 2;
	void * request = malloc(msg_size);

	memcpy(request, &req_OC, prot_OC);
	memcpy(request+prot_OC, &req_resp_code, prot_resp_code);

	socket_send(&master_socket, request, msg_size, 0);
	free(request);

//	uint8_t resp_prot_code = 2;
//	int16_t code;
//	int received_bytes = socket_recv(&master_socket, &code, resp_prot_code);
//	if (received_bytes <= 0) {
//		if (logger) log_error(logger, "------ SERVER %d >> disconnected", master_socket);
//		return DISCONNECTED_SERVER;
//	}
	//return code;

}

t_response_task* task_response_recv(int worker_socket, t_log * logger){

	uint8_t prot_OC = 2;
	uint8_t prot_resp_code = 2;

	t_response_task*response = malloc(sizeof(t_response_task));

	int received_bytes = socket_recv(&worker_socket, &(response->oc_code), prot_OC);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", worker_socket);
		response->exec_code = DISCONNECTED_CLIENT;
		return response;
	}

	received_bytes = socket_recv(&worker_socket, &(response->result_code), prot_resp_code);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", worker_socket);
		response->exec_code = DISCONNECTED_CLIENT;
		return response;
	}

	response->exec_code = SUCCESS;
	return response;
}

void mandar_archivo_temporal(int fd, char *nombre_archivo){
	FILE *f = fopen(nombre_archivo, "r");
	fseek(f, 0L, SEEK_END);
	unsigned long len = ftell(f);
	fseek(f, 0L, SEEK_SET);
	char *linea = NULL, *buffer;
	size_t size = 0;
	buffer = malloc(len);
	int offset = 0;
	while((getline(&linea, &size, f) != -1)){
		int len_linea = strlen(linea);
		buffer = realloc(buffer, len + sizeof(int));
		len += sizeof(int);
		memcpy(buffer + offset, &len_linea, sizeof(int));
		offset += sizeof(int);
		memcpy(buffer + offset, linea, len_linea);
		offset += len_linea;
	}
	char fin = '\0';
	int aa = 0;
//	memcpy(buffer + offset, &fin, sizeof(char));
//	offset += sizeof(int);
	memcpy(buffer + offset, &aa, sizeof(int));
	int enviado = socket_send(&fd, buffer, len + sizeof(int), 0);
}
