/*
 * memory_protocol.c
 *
 *  Created on: 22/4/2017
 *      Author: Dante Romero
 */
#include <commons/log.h>
#include <stdint.h>
#include <stdlib.h>
#include "file_system_prot.h"
#include "socket.h"



/**	╔═════════════════════════════╗
	║ FS - RECEIVE OPERATION CODE ║
	╚═════════════════════════════╝ **/

int fs_recv_ope_cod(int * client_socket, t_log * logger) {
	uint8_t prot_ope_code = 1;
	uint8_t ope_code;
	int received_bytes = socket_recv(client_socket, &ope_code, prot_ope_code);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		return DISCONNECTED_CLIENT;
	}
	return ope_code;
}



/**	╔════════════════╗
	║ FS - HANDSHAKE ║
	╚════════════════╝ **/

int fs_handshake(int * server_socket, t_log * logger) {

	/**	╔═════════════════════════╗
		║ operation_code (1 byte) ║
		╚═════════════════════════╝ **/

	uint8_t prot_ope_code = 1;
	uint8_t req_ope_code = FS_HANDSHAKE_OC;

	int msg_size = sizeof(char) * (prot_ope_code);
	void * request = malloc(msg_size);
	memcpy(request, &req_ope_code, prot_ope_code);
	socket_send(&server_socket, request, msg_size, 0);
	free(request);

	uint8_t resp_prot_code = 2;
	int16_t code;
	int received_bytes = socket_recv(&server_socket, &code, resp_prot_code);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ SERVER %d >> disconnected", server_socket);
		return DISCONNECTED_SERVER;
	}
	return code;
}

void fs_handshake_resp(int * client_socket, int resp_code) {
	send_resp(client_socket, resp_code);
}



/**	╔════════════════════╗
	║ FS - VALIDATE FILE ║
	╚════════════════════╝ **/
int fs_validate_file(int server_socket, char * path, t_log * logger) {
	return send_request(server_socket, path, FS_VALIDATE_FILE_OC, logger);
};

t_v_file_req * v_file_recv_req(int * client_socket, t_log * logger) {
	t_v_file_req * request = malloc(sizeof(t_v_file_req));
	uint8_t prot_path_size = 4;
	uint32_t path_size;
	int received_bytes = socket_recv(client_socket, &path_size, prot_path_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	request->path = malloc(sizeof(char) * path_size);
	received_bytes = socket_recv(client_socket, request->path, path_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	request->exec_code = SUCCESS;
	return request;
}

void v_file_send_resp(int * client_socket, int resp_code) {
	send_resp(client_socket, resp_code);
}



/**	╔══════════════════╗
	║ FS - CREATE FILE ║
	╚══════════════════╝ **/
int fs_create_file(int server_socket, char * path, t_log * logger) {
	return send_request(server_socket, path, FS_CREATE_FILE_OC, logger);
};

t_c_file_req * c_file_recv_req(int * client_socket, t_log * logger) {
	t_c_file_req * request = malloc(sizeof(t_c_file_req));
	uint8_t prot_path_size = 4;
	uint32_t path_size;
	int received_bytes = socket_recv(client_socket, &path_size, prot_path_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	request->path = malloc(sizeof(char) * path_size);
	received_bytes = socket_recv(client_socket, request->path, path_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	request->exec_code = SUCCESS;
	return request;
}

void c_file_send_resp(int * client_socket, int resp_code) {
	send_resp(client_socket, resp_code);
}



/**	╔══════════════════╗
	║ FS - DELETE FILE ║
	╚══════════════════╝ **/
int fs_delete_file(int server_socket, char * path, t_log * logger) {
	return send_request(server_socket, path, FS_DELETE_FILE_OC, logger);
};

t_d_file_req * d_file_recv_req(int * client_socket, t_log * logger) {
	t_d_file_req * request = malloc(sizeof(t_d_file_req));
	uint8_t prot_path_size = 4;
	uint32_t path_size;
	int received_bytes = socket_recv(client_socket, &path_size, prot_path_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	request->path = malloc(sizeof(char) * path_size);
	received_bytes = socket_recv(client_socket, request->path, path_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	request->exec_code = SUCCESS;
	return request;
}

void d_file_send_resp(int * client_socket, int resp_code) {
	send_resp(client_socket, resp_code);
}



/**	╔═══════════╗
	║ FS - READ ║
	╚═══════════╝ **/

t_fs_read_resp * fs_read(int server_socket, char * path, int offset, int size, t_log * logger) {

	/**	╔═════════════════════════╦══════════════════╦════════════════╦═════════════════════╦══════╗
		║ operation_code (1 byte) ║ offset (4 bytes) ║ size (4 bytes) ║ path_size (4 bytes) ║ path ║
		╚═════════════════════════╩══════════════════╩════════════════╩═════════════════════╩══════╝ **/

	uint8_t prot_ope_code = 1;
	uint8_t prot_offset = 4;
	uint8_t prot_size = 4;
	uint8_t prot_path_size = 4;

	uint8_t  req_ope_code = FS_READ_FILE_OC;
	uint32_t req_offset = offset;
	uint32_t req_size = size;
	uint32_t req_path_size = strlen(path) + 1;

	int msg_size = sizeof(char) * (prot_ope_code + prot_offset + prot_size + prot_path_size + req_path_size);
	void * request = malloc(msg_size);
	memcpy(request, &req_ope_code, prot_ope_code);
	memcpy(request + prot_ope_code, &req_offset, prot_offset);
	memcpy(request + prot_ope_code + prot_offset, &req_size, prot_size);
	memcpy(request + prot_ope_code + prot_offset + prot_size, &req_path_size, prot_path_size);
	memcpy(request + prot_ope_code + prot_offset + prot_size + prot_path_size, path, req_path_size);
	socket_send(&server_socket, request, msg_size, 0);
	free(request);

	t_fs_read_resp * response = malloc(sizeof(t_fs_read_resp));
	uint8_t resp_prot_code = 2;
	int received_bytes = socket_recv(&server_socket, &(response->exec_code), resp_prot_code);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ SERVER %d >> disconnected", server_socket);
		response->exec_code = DISCONNECTED_SERVER;
		return response;
	}
	uint8_t resp_prot_buff_size = 4;
	received_bytes = socket_recv(&server_socket, &(response->buffer_size), resp_prot_buff_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ SERVER %d >> disconnected", server_socket);
		response->exec_code = DISCONNECTED_SERVER;
		return response;
	}
	if ((response->buffer_size) > 0) {
		response->buffer = malloc((response->buffer_size));
		received_bytes = socket_recv(&server_socket, (response->buffer), (response->buffer_size));
		if (received_bytes <= 0) {
			if (logger) log_error(logger, "------ SERVER %d >> disconnected", server_socket);
			response->exec_code = DISCONNECTED_SERVER;
			return response;
		}
	}
	return response;
};

t_fs_read_req * fs_read_recv_req(int * client_socket, t_log * logger) {
	t_fs_read_req * request = malloc(sizeof(t_fs_read_req));
	uint8_t prot_req_offset = 4;
	int received_bytes = socket_recv(client_socket, &(request->offset), prot_req_offset);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	uint8_t prot_req_size = 4;
	received_bytes = socket_recv(client_socket, &(request->size), prot_req_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	uint8_t prot_path_size = 4;
	uint32_t path_size;
	received_bytes = socket_recv(client_socket, &path_size, prot_path_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	request->path = malloc(sizeof(char) * path_size);
	received_bytes = socket_recv(client_socket, request->path, path_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	request->exec_code = SUCCESS;
	return request;
}

void fs_read_send_resp(int * client_socket, int resp_code, int buffer_size, void * buffer) {
	uint8_t resp_prot_code = 2;
	uint8_t resp_prot_buff_size = 4;
	int response_size = sizeof(char) * (resp_prot_code + resp_prot_buff_size + ((buffer_size > 0) ? buffer_size : 0));
	void * response = malloc(response_size);
	memcpy(response, &resp_code, resp_prot_code);
	memcpy(response + resp_prot_code, &buffer_size, resp_prot_buff_size);
	if (buffer_size > 0) {
		memcpy(response + resp_prot_code + resp_prot_buff_size, buffer, buffer_size);
	}
	socket_write(client_socket, response, response_size);
	free(response);
}



/**	╔════════════╗
	║ FS - WRITE ║
	╚════════════╝ **/

int fs_write(int server_socket, char * path, int offset, int size, int buffer_size, void * buffer, t_log * logger) {

	/**	╔═════════════════════════╦══════════════════╦════════════════╦═════════════════════╦══════╦════════════════════════╦════════╗
		║ operation_code (1 byte) ║ offset (4 bytes) ║ size (4 bytes) ║ path_size (4 bytes) ║ path ║ buffer_size (4 bytes)  ║ buffer ║
		╚═════════════════════════╩══════════════════╩════════════════╩═════════════════════╩══════╩════════════════════════╩════════╝ **/

	uint8_t prot_ope_code = 1;
	uint8_t prot_offset = 4;
	uint8_t prot_size = 4;
	uint8_t prot_path_size = 4;
	uint8_t prot_buffer_size = 4;

	uint8_t  req_ope_code = FS_WRITE_FILE_OC;
	uint32_t req_offset = offset;
	uint32_t req_size = size;
	uint32_t req_path_size = strlen(path) + 1;
	uint32_t req_buffer_size = buffer_size;

	int msg_size = sizeof(char) * (prot_ope_code + prot_offset + prot_size + prot_path_size + req_path_size + prot_buffer_size + buffer_size);
	void * request = malloc(msg_size);
	memcpy(request, &req_ope_code, prot_ope_code);
	memcpy(request + prot_ope_code, &req_offset, prot_offset);
	memcpy(request + prot_ope_code + prot_offset, &req_size, prot_size);
	memcpy(request + prot_ope_code + prot_offset + prot_size, &req_path_size, prot_path_size);
	memcpy(request + prot_ope_code + prot_offset + prot_size + prot_path_size, path, req_path_size);
	memcpy(request + prot_ope_code + prot_offset + prot_size + prot_path_size + req_path_size, &req_buffer_size, prot_buffer_size);
	memcpy(request + prot_ope_code + prot_offset + prot_size + prot_path_size + req_path_size + prot_buffer_size, buffer, req_buffer_size);
	socket_send(&server_socket, request, msg_size, 0);
	free(request);

	uint8_t resp_prot_code = 2;
	int16_t code;
	int received_bytes = socket_recv(&server_socket, &code, resp_prot_code);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ SERVER %d >> disconnected", server_socket);
		return DISCONNECTED_SERVER;
	}
	return code;
};

t_fs_write_req * fs_write_recv_req(int * client_socket, t_log * logger) {
	t_fs_write_req * request = malloc(sizeof(t_fs_write_req));
	uint8_t prot_req_offset = 4;
	int received_bytes = socket_recv(client_socket, &(request->offset), prot_req_offset);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	uint8_t prot_req_size = 4;
	received_bytes = socket_recv(client_socket, &(request->size), prot_req_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	uint8_t prot_path_size = 4;
	uint32_t path_size;
	received_bytes = socket_recv(client_socket, &path_size, prot_path_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	request->path = malloc(sizeof(char) * path_size);
	received_bytes = socket_recv(client_socket, request->path, path_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	uint8_t prot_buffer_size = 4;
	uint32_t buffer_size;
	received_bytes = socket_recv(client_socket, &buffer_size, prot_buffer_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	request->buffer = malloc(sizeof(char) * buffer_size);
	received_bytes = socket_recv(client_socket, request->buffer, buffer_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	request->exec_code = SUCCESS;
	return request;
}

void fs_write_send_resp(int * client_socket, int resp_code) {
	send_resp(client_socket, resp_code);
}

/**************************************************************************************************/
/**************************************************************************************************/
/**************************************************************************************************/
/**************************************************************************************************/

int send_request(int server_socket, char * path, int ope_cod, t_log * logger) {

	/**	╔═════════════════════════╦═════════════════════╦═══════╗
		║ operation_code (1 byte) ║ path_size (4 bytes) ║ path  ║
		╚═════════════════════════╩═════════════════════╩═══════╝ **/

	uint8_t prot_ope_code = 1;
	uint8_t prot_path_size = 4;

	uint8_t  req_ope_code = ope_cod;
	uint32_t req_path_size = strlen(path) + 1;

	int msg_size = sizeof(char) * (prot_ope_code + prot_path_size + req_path_size);
	void * request = malloc(msg_size);
	memcpy(request, &req_ope_code, prot_ope_code);
	memcpy(request + prot_ope_code, &req_path_size, prot_path_size);
	memcpy(request + prot_ope_code + prot_path_size, path, req_path_size);
	socket_send(&server_socket, request, msg_size, 0);
	free(request);

	uint8_t resp_prot_code = 2;
	int16_t code;
	int received_bytes = socket_recv(&server_socket, &code, resp_prot_code);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ SERVER %d >> disconnected", server_socket);
		return DISCONNECTED_SERVER;
	}
	return code;
}

void send_resp(int * client_socket, int resp_code) {
	uint8_t resp_prot_code = 2;
	int response_size = sizeof(char) * (resp_prot_code);
	void * response = malloc(response_size);
	memcpy(response, &resp_code, resp_prot_code);
	socket_write(client_socket, response, response_size);
	free(response);
}
