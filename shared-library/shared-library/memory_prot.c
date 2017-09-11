/*
 * memory_protocol.c
 *
 *  Created on: 22/4/2017
 *      Author: Dante Romero
 */
#include <commons/log.h>
#include <stdint.h>
#include <stdlib.h>
#include "memory_prot.h"
#include "socket.h"



/**	╔═════════════════════════════════╗
	║ MEMORY - RECEIVE OPERATION CODE ║
	╚═════════════════════════════════╝ **/

int recv_operation_code(int * client_socket, t_log * logger) {
	uint8_t prot_ope_code = 1;
	uint8_t ope_code;
	int received_bytes = socket_recv(client_socket, &ope_code, prot_ope_code);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		return DISCONNECTED_CLIENT;
	}
	return ope_code;
}



/**	╔════════════════════╗
	║ MEMORY - HANDSHAKE ║
	╚════════════════════╝ **/

int handshake(int server_socket, char type, int stack_size, t_log * logger) {

	/**	╔═════════════════════════╦═══════════════╦═══════════════════════════════╗
		║ operation_code (1 byte) ║ type (1 byte) ║ stack size (4 bytes optional) ║
		╚═════════════════════════╩═══════════════╩═══════════════════════════════╝ **/

	uint8_t prot_ope_code = 1;
	uint8_t prot_type = 1;
	uint8_t prot_stack_size = 0;

	uint8_t req_ope_code = HANDSHAKE_OC;
	uint8_t req_ope_type = type;
	uint8_t req_stack_size = 0;

	if (type == 'k') {
		if (stack_size && stack_size > 0) {
			prot_stack_size = 1;
			req_stack_size = stack_size;
		} else {
			return UNDEFINED_STACK_SIZE;
		}
	}

	int msg_size = sizeof(char) * (prot_ope_code + prot_type + prot_stack_size);
	void * request = malloc(msg_size);
	memcpy(request, &req_ope_code, prot_ope_code);
	memcpy(request + prot_ope_code, &req_ope_type, prot_type);
	if (prot_stack_size > 0) {
		memcpy(request + prot_ope_code + prot_type, &req_stack_size, prot_stack_size);
	}
	socket_send(&server_socket, request, msg_size, 0);

	free(request);

	uint8_t resp_prot_code = 4;
	int32_t memory_size;
	int received_bytes = socket_recv(&server_socket, &memory_size, resp_prot_code);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ SERVER %d >> disconnected", server_socket);
		return DISCONNECTED_SERVER;
	}
	return memory_size;
}

t_handshake_request * handshake_recv_req(int * client_socket, t_log * logger) {
	t_handshake_request * request = malloc(sizeof(t_handshake_request));
	uint8_t prot_type = 1;
	int received_bytes = socket_recv(client_socket, &(request->type), prot_type);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	if ((request->type) == 'k') {
		uint8_t stack_size = 1;
		received_bytes = socket_recv(client_socket, &(request->stack_size), stack_size);
		if (received_bytes <= 0) {
			if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
			request->exec_code = DISCONNECTED_CLIENT;
			return request;
		}
	} else {
		request->stack_size = 0;
	}
	request->exec_code = SUCCESS;
	return request;
}

void handshake_resp(int * client_socket, int memory_size) {
	uint8_t resp_prot_code = 4;
	int response_size = sizeof(char) * (resp_prot_code);
	void * response = malloc(response_size);
	memcpy(response, &memory_size, resp_prot_code);
	socket_write(client_socket, response, response_size);
	free(response);
}



/**	╔═══════════════════════╗
	║ MEMORY - INIT PROCESS ║
	╚═══════════════════════╝ **/

int memory_init_process(int server_socket, int pid, int pages, t_log * logger) {

	/**	╔═════════════════════════╦═══════════════╦═════════════════╗
		║ operation_code (1 byte) ║ pid (4 bytes) ║ pages (4 bytes) ║
		╚═════════════════════════╩═══════════════╩═════════════════╝ **/

	uint8_t prot_ope_code = 1;
	uint8_t prot_pid = 4;
	uint8_t prot_pages = 4;

	uint8_t  req_ope_code = INIT_PROCESS_OC;
	uint32_t req_pid = pid;
	uint32_t req_pages = pages;

	int msg_size = sizeof(char) * (prot_ope_code + prot_pid + prot_pages);
	void * request = malloc(msg_size);
	memcpy(request, &req_ope_code, prot_ope_code);
	memcpy(request + prot_ope_code, &req_pid, prot_pid);
	memcpy(request + prot_ope_code + prot_pid, &req_pages, prot_pages);
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

t_init_process_request * init_process_recv_req(int * client_socket, t_log * logger) {
	t_init_process_request * request = malloc(sizeof(t_init_process_request));
	uint8_t prot_req_pid = 4;
	int received_bytes = socket_recv(client_socket, &(request->pid), prot_req_pid);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	uint8_t prot_req_pages = 4;
	received_bytes = socket_recv(client_socket, &(request->pages), prot_req_pages);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	request->exec_code = SUCCESS;
	return request;
}

void init_process_send_resp(int * client_socket, int resp_code) {
	uint8_t resp_prot_code = 2;
	int response_size = sizeof(char) * (resp_prot_code);
	void * response = malloc(response_size);
	memcpy(response, &resp_code, resp_prot_code);
	socket_write(client_socket, response, response_size);
	free(response);
}



/**	╔════════════════╗
	║ MEMORY - WRITE ║
	╚════════════════╝ **/

int memory_write(int server_socket, int pid, int page, int offset, int size, int buffer_size, void * buffer, t_log * logger) {

	/**	╔═════════════════════════╦═══════════════╦════════════════╦══════════════════╦════════════════╦════════════════════════╦════════╗
		║ operation_code (1 byte) ║ pid (4 bytes) ║ page (4 bytes) ║ offset (4 bytes) ║ size (4 bytes) ║ buffer_size (4 bytes)  ║ buffer ║
		╚═════════════════════════╩═══════════════╩════════════════╩══════════════════╩════════════════╩════════════════════════╩════════╝ **/

	uint8_t prot_ope_code = 1;
	uint8_t prot_pid = 4;
	uint8_t prot_page = 4;
	uint8_t prot_offset = 4;
	uint8_t prot_size = 4;
	uint8_t prot_buffer_size = 4;

	uint8_t  req_ope_code = WRITE_OC;
	uint32_t req_pid = pid;
	uint32_t req_page = page;
	uint32_t req_offset = offset;
	uint32_t req_size = size;
	uint32_t req_buffer_size = buffer_size;

	int msg_size = sizeof(char) * (prot_ope_code + prot_pid + prot_page + prot_offset + prot_size + prot_buffer_size + buffer_size);
	void * request = malloc(msg_size);
	memcpy(request, &req_ope_code, prot_ope_code);
	memcpy(request + prot_ope_code, &req_pid, prot_pid);
	memcpy(request + prot_ope_code + prot_pid, &req_page, prot_page);
	memcpy(request + prot_ope_code + prot_pid + prot_page, &req_offset, prot_offset);
	memcpy(request + prot_ope_code + prot_pid + prot_page + prot_offset, &req_size, prot_size);
	memcpy(request + prot_ope_code + prot_pid + prot_page + prot_offset + prot_size, &req_buffer_size, prot_buffer_size);
	memcpy(request + prot_ope_code + prot_pid + prot_page + prot_offset + prot_size + prot_buffer_size, buffer, buffer_size);
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

t_write_request * write_recv_req(int * client_socket, t_log * logger) {
	t_write_request * request = malloc(sizeof(t_write_request));
	uint8_t prot_req_pid = 4;
	int received_bytes = socket_recv(client_socket, &(request->pid), prot_req_pid);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	uint8_t prot_req_page = 4;
	received_bytes = socket_recv(client_socket, &(request->page), prot_req_page);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	uint8_t prot_req_offset = 4;
	received_bytes = socket_recv(client_socket, &(request->offset), prot_req_offset);
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

void write_send_resp(int * client_socket, int resp_code) {
	uint8_t resp_prot_code = 2;
	int response_size = sizeof(char) * (resp_prot_code);
	void * response = malloc(response_size);
	memcpy(response, &resp_code, resp_prot_code);
	socket_write(client_socket, response, response_size);
	free(response);
}



/**	╔═══════════════╗
	║ MEMORY - READ ║
	╚═══════════════╝ **/

t_read_response * memory_read(int server_socket, int pid, int page, int offset, int size, t_log * logger) {

	/**	╔═════════════════════════╦═══════════════╦════════════════╦══════════════════╦════════════════╗
		║ operation_code (1 byte) ║ pid (4 bytes) ║ page (4 bytes) ║ offset (4 bytes) ║ size (4 bytes) ║
		╚═════════════════════════╩═══════════════╩════════════════╩══════════════════╩════════════════╝ **/

	uint8_t prot_ope_code = 1;
	uint8_t prot_pid = 4;
	uint8_t prot_page = 4;
	uint8_t prot_offset = 4;
	uint8_t prot_size = 4;

	uint8_t  req_ope_code = READ_OC;
	uint32_t req_pid = pid;
	uint32_t req_page = page;
	uint32_t req_offset = offset;
	uint32_t req_size = size;

	int msg_size = sizeof(char) * (prot_ope_code + prot_pid + prot_page + prot_offset + prot_size);
	void * request = malloc(msg_size);
	memcpy(request, &req_ope_code, prot_ope_code);
	memcpy(request + prot_ope_code, &req_pid, prot_pid);
	memcpy(request + prot_ope_code + prot_pid, &req_page, prot_page);
	memcpy(request + prot_ope_code + prot_pid + prot_page, &req_offset, prot_offset);
	memcpy(request + prot_ope_code + prot_pid + prot_page + prot_offset, &req_size, prot_size);
	socket_send(&server_socket, request, msg_size, 0);
	free(request);

	t_read_response * response = malloc(sizeof(t_read_response));
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

t_read_request * read_recv_req(int * client_socket, t_log * logger) {
	t_read_request * request = malloc(sizeof(t_read_request));
	uint8_t prot_req_pid = 4;
	int received_bytes = socket_recv(client_socket, &(request->pid), prot_req_pid);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	uint8_t prot_req_page = 4;
	received_bytes = socket_recv(client_socket, &(request->page), prot_req_page);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	uint8_t prot_req_offset = 4;
	received_bytes = socket_recv(client_socket, &(request->offset), prot_req_offset);
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
	request->exec_code = SUCCESS;
	return request;
}

void read_send_resp(int * client_socket, int resp_code, int buffer_size, void * buffer) {
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



/**	╔═══════════════════════╗
	║ MEMORY - ASSIGN PAGES ║
	╚═══════════════════════╝ **/

int memory_assign_pages(int server_socket, int pid, int pages, t_log * logger) {

	/**	╔═════════════════════════╦═══════════════╦═════════════════╗
		║ operation_code (1 byte) ║ pid (4 bytes) ║ pages (4 bytes) ║
		╚═════════════════════════╩═══════════════╩═════════════════╝ **/

	uint8_t prot_ope_code = 1;
	uint8_t prot_pid = 4;
	uint8_t prot_pages = 4;

	uint8_t  req_ope_code = ASSIGN_PAGE_OC;
	uint32_t req_pid = pid;
	uint32_t req_pages = pages;

	int msg_size = sizeof(char) * (prot_ope_code + prot_pid + prot_pages);
	void * request = malloc(msg_size);
	memcpy(request, &req_ope_code, prot_ope_code);
	memcpy(request + prot_ope_code, &req_pid, prot_pid);
	memcpy(request + prot_ope_code + prot_pid, &req_pages, prot_pages);
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

t_assign_pages_request * assign_pages_recv_req(int * client_socket, t_log * logger) {
	t_assign_pages_request * request = malloc(sizeof(t_assign_pages_request));
	uint8_t prot_req_pid = 4;
	int received_bytes = socket_recv(client_socket, &(request->pid), prot_req_pid);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	uint8_t prot_req_pages = 4;
	received_bytes = socket_recv(client_socket, &(request->pages), prot_req_pages);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	request->exec_code = SUCCESS;
	return request;
}

void assign_pages_send_resp(int * client_socket, int resp_code) {
	uint8_t resp_prot_code = 2;
	int response_size = sizeof(char) * (resp_prot_code);
	void * response = malloc(response_size);
	memcpy(response, &resp_code, resp_prot_code);
	socket_write(client_socket, response, response_size);
	free(response);
}



/**	╔══════════════════════╗
	║ MEMORY - DELETE PAGE ║
	╚══════════════════════╝ **/

int memory_delete_page(int server_socket, int pid, int page, t_log * logger) {

	/**	╔═════════════════════════╦═══════════════╦════════════════╗
		║ operation_code (1 byte) ║ pid (4 bytes) ║ page (4 bytes) ║
		╚═════════════════════════╩═══════════════╩════════════════╝ **/

	uint8_t prot_ope_code = 1;
	uint8_t prot_pid = 4;
	uint8_t prot_page = 4;

	uint8_t  req_ope_code = DELETE_PAGE_OC;
	uint32_t req_pid = pid;
	uint32_t req_page = page;

	int msg_size = sizeof(char) * (prot_ope_code + prot_pid + prot_page);
	void * request = malloc(msg_size);
	memcpy(request, &req_ope_code, prot_ope_code);
	memcpy(request + prot_ope_code, &req_pid, prot_pid);
	memcpy(request + prot_ope_code + prot_pid, &req_page, prot_page);
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

t_delete_page_request * delete_page_recv_req(int * client_socket, t_log * logger) {
	t_delete_page_request * request = malloc(sizeof(t_delete_page_request));
	uint8_t prot_req_pid = 4;
	int received_bytes = socket_recv(client_socket, &(request->pid), prot_req_pid);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	uint8_t prot_req_page = 4;
	received_bytes = socket_recv(client_socket, &(request->page), prot_req_page);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	request->exec_code = SUCCESS;
	return request;
}

void delete_page_send_resp(int * client_socket, int resp_code) {
	uint8_t resp_prot_code = 2;
	int response_size = sizeof(char) * (resp_prot_code);
	void * response = malloc(response_size);
	memcpy(response, &resp_code, resp_prot_code);
	socket_write(client_socket, response, response_size);
	free(response);
}



/**	╔═══════════════════════════╗
	║ MEMORY - FINALIZE PROCESS ║
	╚═══════════════════════════╝ **/

int memory_finalize_process(int server_socket, int pid, t_log * logger) {

	/**	╔═════════════════════════╦═══════════════╗
		║ operation_code (1 byte) ║ pid (4 bytes) ║
		╚═════════════════════════╩═══════════════╝ **/

	uint8_t prot_ope_code = 1;
	uint8_t prot_pid = 4;

	uint8_t  req_ope_code = END_PROCESS_OC;
	uint32_t req_pid = pid;

	int msg_size = sizeof(char) * (prot_ope_code + prot_pid);
	void * request = malloc(msg_size);
	memcpy(request, &req_ope_code, prot_ope_code);
	memcpy(request + prot_ope_code, &req_pid, prot_pid);
	socket_send(&server_socket, request, msg_size, 0);
	free(request);

	uint8_t resp_prot_code = 2;
	uint16_t code;
	int received_bytes = socket_recv(&server_socket, &code, resp_prot_code);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ SERVER %d >> disconnected", server_socket);
		return DISCONNECTED_SERVER;
	}
	return code;
};

t_finalize_process_request * finalize_process_recv_req(int * client_socket, t_log * logger) {
	t_finalize_process_request * request = malloc(sizeof(t_finalize_process_request));
	uint8_t prot_req_pid = 4;
	int received_bytes = socket_recv(client_socket, &(request->pid), prot_req_pid);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	request->exec_code = SUCCESS;
	return request;
}

void finalize_process_send_resp(int * client_socket, int resp_code) {
	uint8_t resp_prot_code = 2;
	int response_size = sizeof(char) * (resp_prot_code);
	void * response = malloc(response_size);
	memcpy(response, &resp_code, resp_prot_code);
	socket_write(client_socket, response, response_size);
	free(response);
}
