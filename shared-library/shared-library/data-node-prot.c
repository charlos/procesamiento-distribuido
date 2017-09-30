#include "data-node-prot.h"
#include "socket.h"
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <stdarg.h>

static void check(int test, const char * message, ...) {
	if (test) {
		va_list args;
		va_start(args, message);
		vfprintf(stderr, message, args);
		va_end(args);
		fprintf(stderr, "\n");
		exit(EXIT_FAILURE);
	}
}

void * map_file(char * file_path, int flags) {
	struct stat sb;
	size_t size;
	int fd; // file descriptor
	int status;

	fd = open(file_path, flags);
	check(fd < 0, "open %s failed: %s", file_path, strerror(errno));

	status = fstat(fd, &sb);
	check(status < 0, "stat %s failed: %s", file_path, strerror(errno));
	size = sb.st_size;

	void * mapped_file_ptr = mmap((caddr_t) 0, size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
	check((mapped_file_ptr == MAP_FAILED), "mmap %s failed: %s", file_path, strerror(errno));

	return mapped_file_ptr;
}

void unmap_file(void * mapped_file, int file_size) {
	check((munmap(mapped_file, file_size) == -1), "munmap %s failed", strerror(errno));
}


/**	╔════════════════════════╗
	║ RECEIVE OPERATION CODE ║
	╚════════════════════════╝ **/

int dn_recv_operation_code(int * client_socket, t_log * logger) {
	uint8_t prot_ope_code = 1;
	uint8_t ope_code;
	int received_bytes = socket_recv(client_socket, &ope_code, prot_ope_code);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		return DISCONNECTED_CLIENT;
	}
	return ope_code;
}


/**	╔═══════════╗
	║ GET BLOCK ║
	╚═══════════╝ **/

int dn_get_block(int server_socket, int block, t_log * logger) {

	/**	╔═════════════════════════╦═══════════════════════╗
		║ operation_code (1 byte) ║ block number (4 byte) ║
		╚═════════════════════════╩═══════════════════════╝ **/

	uint8_t prot_ope_code = 1;
	uint8_t prot_block = 4;

	uint8_t req_ope_code = GET_BLOCK;
	uint32_t req_block = block;

	int msg_size = sizeof(char) * (prot_ope_code + prot_block);
	void * request = malloc(msg_size);
	memcpy(request, &req_ope_code, prot_ope_code);
	memcpy(request + prot_ope_code, &req_block, prot_block);
	socket_send(&server_socket, request, msg_size, 0);
	free(request);

	t_dn_get_block_resp * response = malloc(sizeof(t_dn_get_block_resp));
	uint8_t resp_prot_code = 2;
	int received_bytes = socket_recv(&server_socket, &(response->exec_code), resp_prot_code);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ SERVER %d >> disconnected", server_socket);
		response->exec_code = DISCONNECTED_SERVER;
		return response;
	}

	if (response->exec_code != SUCCESS)
		return response;

	response->buffer = malloc(BLOCK_SIZE);
	received_bytes = socket_recv(&server_socket, (response->buffer), BLOCK_SIZE);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ SERVER %d >> disconnected", server_socket);
		response->exec_code = DISCONNECTED_SERVER;
	}
	return response;
}

t_dn_get_block_req * dn_get_block_recv_req(int * client_socket, t_log * logger) {
	t_dn_get_block_req * request = malloc(sizeof(t_dn_get_block_req));
	uint8_t prot_block = 4;
	int received_bytes = socket_recv(client_socket, (request->block), prot_block);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	request->exec_code = SUCCESS;
	return request;
}

void dn_get_block_send_resp(int * client_socket, int resp_code, void * buffer) {
	uint8_t resp_prot_code = 2;
	int response_size = sizeof(char) * (resp_prot_code + ((resp_code == SUCCESS) ? BLOCK_SIZE : 0));
	void * response = malloc(response_size);
	memcpy(response, &resp_code, resp_prot_code);
	if (resp_code == SUCCESS) {
		memcpy(response + resp_prot_code, buffer, BLOCK_SIZE);
	}
	socket_write(client_socket, response, response_size);
	free(response);
}


/**	╔═══════════╗
	║ SET BLOCK ║
	╚═══════════╝ **/

int dn_set_block(int server_socket, int block, void * buffer, t_log * logger) {

	/**	╔═════════════════════════╦═══════════════════════╦════════╗
		║ operation_code (1 byte) ║ block number (4 byte) ║ buffer ║
		╚═════════════════════════╩═══════════════════════╩════════╝ **/

	uint8_t prot_ope_code = 1;
	uint8_t prot_block = 4;

	uint8_t req_ope_code = SET_BLOCK;
	uint32_t req_block = block;

	int msg_size = sizeof(char) * (prot_ope_code + prot_block + BLOCK_SIZE);
	void * request = malloc(msg_size);
	memcpy(request, &req_ope_code, prot_ope_code);
	memcpy(request + prot_ope_code, &req_block, prot_block);
	memcpy(request + prot_ope_code + prot_block, buffer, BLOCK_SIZE);
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

t_dn_set_block_req * dn_set_block_recv_req(int * client_socket, t_log * logger) {
	t_dn_set_block_req * request = malloc(sizeof(t_dn_set_block_req));
	uint8_t prot_block = 4;
	int received_bytes = socket_recv(client_socket, (request->block), prot_block);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	received_bytes = socket_recv(client_socket, (request->buffer), BLOCK_SIZE);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	request->exec_code = SUCCESS;
	return request;
}

void dn_set_block_send_resp(int * client_socket, int resp_code) {
	uint8_t resp_prot_code = 2;
	int response_size = sizeof(char) * (resp_prot_code);
	void * response = malloc(response_size);
	memcpy(response, &resp_code, resp_prot_code);
	socket_write(client_socket, response, response_size);
	free(response);
}
