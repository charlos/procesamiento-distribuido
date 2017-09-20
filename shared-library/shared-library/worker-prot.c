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

int transform_req_send(int worker_socket, int block, int used_size, char* result_file, int script_size, void* script, t_log * logger) {

	uint8_t prot_ope_code = 1;
	uint8_t prot_block = 4;
	uint8_t prot_used_size = 4;
	uint8_t prot_result_file = 4;
	uint8_t prot_script_size = 4;

	uint8_t  req_ope_code = TRANSFORM_OC;
	uint32_t req_block = block;
	uint32_t req_used_size = used_size;
	uint32_t req_result_file = string_length(result_file);
	uint32_t req_script_size = script_size;

	int msg_size = sizeof(char) * (prot_ope_code + prot_block + prot_used_size + prot_result_file + req_result_file + prot_script_size + req_script_size);
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
	int16_t code;
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

int local_reduction_req_send(int worker_socket, char* temp_files, char* result_file, int script_size, void* script, t_log * logger) {
	// char* temp_files ---> cadena que contiene lista de los archivos separados por ";" para poder hacer un split luego

	uint8_t prot_ope_code = 1;
	uint8_t prot_temp_files = 4;
	uint8_t prot_result_file = 4;
	uint8_t prot_script_size = 4;

	uint8_t  req_ope_code = REDUCE_LOCALLY_OC;
	uint32_t req_temp_files = string_length(temp_files);
	uint32_t req_result_file = string_length(result_file);
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
