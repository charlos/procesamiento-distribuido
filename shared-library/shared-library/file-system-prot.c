#include "file-system-prot.h"
#include "socket.h"


/**	╔════════════════════════╗
	║ RECEIVE OPERATION CODE ║
	╚════════════════════════╝ **/

int fs_recv_operation_code(int * client_socket, t_log * logger) {
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
	║ HANDSHAKE ║
	╚═══════════╝ **/

int fs_handshake(int server_socket, char type, int node_number, int blocks, t_log * logger) {

	/**	╔═════════════════════════╦═══════════════╗
		║ operation_code (1 byte) ║ type (1 byte) ║
		╚═════════════════════════╩═══════════════╝ **/

	/**
			DATA_NODE
			╔═════════════════════════╦═══════════════╦════════════════════════╦═════════════════╗
		║ operation_code (1 byte) ║ type (1 byte) ║ node number (4 bytes) ║ blocks (4 bytes) ║
		╚═════════════════════════╩═══════════════╩═══════════════════════╩══════════════════╝ **/

	uint8_t prot_ope_code = 1;
	uint8_t prot_type = 1;
	uint8_t prot_node_number = 4;
	uint8_t prot_blocks = 4;

	uint8_t req_ope_code = FS_HANDSHAKE;
	uint8_t req_type = type;
	uint32_t req_node_number = node_number;
	uint32_t req_blocks = blocks;

	int msg_size = sizeof(char) * (prot_ope_code + prot_type);
	if (type == 'd')
		msg_size += prot_node_number + prot_blocks;

	void * request = malloc(msg_size);
	memcpy(request, &req_ope_code, prot_ope_code);
	memcpy(request + prot_ope_code, &req_type, prot_type);
	if (type == 'd') {
		memcpy(request + prot_ope_code + prot_type, &req_node_number, prot_node_number);
		memcpy(request + prot_ope_code + prot_type + prot_node_number, &req_blocks, prot_blocks);
	}
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

t_fs_handshake_req * fs_handshake_recv_req(int * client_socket, t_log * logger) {
	t_fs_handshake_req * request = malloc(sizeof(t_fs_handshake_req));
	uint8_t prot_type = 1;
	int received_bytes = socket_recv(client_socket, &(request->type), prot_type);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	if ((request->type) == 'd') {
		uint8_t prot_node_number = 4;
		received_bytes = socket_recv(client_socket, &(request->node_number), prot_node_number);
		if (received_bytes <= 0) {
			if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
			request->exec_code = DISCONNECTED_CLIENT;
			return request;
		}
		uint8_t prot_blocks = 4;
		received_bytes = socket_recv(client_socket, &(request->blocks), prot_blocks);
		if (received_bytes <= 0) {
			if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
			request->exec_code = DISCONNECTED_CLIENT;
			return request;
		}
	}
	request->exec_code = SUCCESS;
	return request;
}

void fs_handshake_send_resp(int * client_socket, int resp_code) {
	uint8_t resp_prot_code = 2;
	int response_size = sizeof(char) * (resp_prot_code);
	void * response = malloc(response_size);
	memcpy(response, &resp_code, resp_prot_code);
	socket_write(client_socket, response, response_size);
	free(response);
}


/**	╔═══════════╗
	║ LOAD FILE ║
	╚═══════════╝ **/

int fs_load_file(int server_socket, char * path, char type, int buffer_size, void * buffer, t_log * logger) {

	/**	╔═════════════════════════╦═════════════════════╦══════╦═══════════════╦════════════════════════╦════════╗
    ║ operation_code (1 byte) ║ path_size (4 bytes) ║ path ║ type (1 byte) ║ buffer_size (4 bytes)  ║ buffer ║
    ╚═════════════════════════╩═════════════════════╩══════╩═══════════════╩════════════════════════╩════════╝ **/
	uint8_t prot_ope_code = 1;
	uint8_t prot_path_size = 4;
	uint8_t prot_type = 1;
	uint8_t prot_buffer_size = 4;

	uint8_t  req_ope_code = LOAD_FILE;
	uint32_t req_path_size = strlen(path) + 1;
	uint8_t req_ope_type = type;
	uint32_t req_buffer_size = buffer_size;

	int msg_size = sizeof(char) * (prot_ope_code + prot_path_size + req_path_size + prot_type + prot_buffer_size + req_buffer_size);
	void * request = malloc(msg_size);
	memcpy(request, &req_ope_code, prot_ope_code);
	memcpy(request + prot_ope_code, &req_path_size, prot_path_size);
	memcpy(request + prot_ope_code + prot_path_size, path, req_path_size);
	memcpy(request + prot_ope_code + prot_path_size + req_path_size, &req_ope_type, prot_type);
	memcpy(request + prot_ope_code + prot_path_size + req_path_size + prot_type, &req_buffer_size, prot_buffer_size);
	memcpy(request + prot_ope_code + prot_path_size + req_path_size + prot_type + prot_buffer_size, buffer, req_buffer_size);
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

t_fs_load_file_req * fs_load_file_recv_req(int * client_socket, t_log * logger) {
	t_fs_load_file_req * request = malloc(sizeof(t_fs_load_file_req));
	uint8_t prot_path_size = 4;
	uint32_t path_size;
	int received_bytes = socket_recv(client_socket, &path_size, prot_path_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	request->path = malloc(sizeof(char) * path_size);
	received_bytes = socket_recv(client_socket, (request->path), path_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	uint8_t prot_type = 1;
	received_bytes = socket_recv(client_socket, &(request->type), prot_type);
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
	received_bytes = socket_recv(client_socket, (request->buffer), buffer_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	request->exec_code = SUCCESS;
	return request;
}

void fs_load_file_send_resp(int * client_socket, int resp_code) {
	uint8_t resp_prot_code = 2;
	int response_size = sizeof(char) * (resp_prot_code);
	void * response = malloc(response_size);
	memcpy(response, &resp_code, resp_prot_code);
	socket_write(client_socket, response, response_size);
	free(response);
}


/**	╔═══════════╗
	║ READ FILE ║
	╚═══════════╝ **/

t_fs_read_file_resp * fs_read_file(int server_socket, char * path, t_log * logger) {

	/**	╔═════════════════════════╦═════════════════════╦══════╗
    ║ operation_code (1 byte) ║ path_size (4 bytes) ║ path ║
    ╚═════════════════════════╩═════════════════════╩══════╝ **/
	uint8_t prot_ope_code = 1;
	uint8_t prot_path_size = 4;

	uint8_t  req_ope_code = READ_FILE;
	uint32_t req_path_size = strlen(path) + 1;

	int msg_size = sizeof(char) * (prot_ope_code + prot_path_size + req_path_size);
	void * request = malloc(msg_size);
	memcpy(request, &req_ope_code, prot_ope_code);
	memcpy(request + prot_ope_code, &req_path_size, prot_path_size);
	memcpy(request + prot_ope_code + prot_path_size, path, req_path_size);
	socket_send(&server_socket, request, msg_size, 0);
	free(request);

	t_fs_read_file_resp * response = malloc(sizeof(t_fs_read_file_resp));
	uint8_t resp_prot_code = 2;
	int received_bytes = socket_recv(&server_socket, &(response->exec_code), resp_prot_code);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ SERVER %d >> disconnected", server_socket);
		response->exec_code = DISCONNECTED_SERVER;
		return response;
	}

	if (response->exec_code != SUCCESS)
		return response;

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
		}
	}
	return response;
}

t_fs_read_file_req * fs_read_file_recv_req(int * client_socket, t_log * logger) {
	t_fs_read_file_req * request = malloc(sizeof(t_fs_read_file_req));
	uint8_t prot_path_size = 4;
	uint32_t path_size;
	int received_bytes = socket_recv(client_socket, &path_size, prot_path_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	request->path = malloc(sizeof(char) * path_size);
	received_bytes = socket_recv(client_socket, (request->path), path_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	request->exec_code = SUCCESS;
	return request;
}

void fs_read_file_send_resp(int * client_socket, int resp_code, int buffer_size, void * buffer) {
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


/**	╔═══════════════════╗
	║ GET FILE METADATA ║
	╚═══════════════════╝ **/

t_fs_get_file_md_resp * fs_get_file_metadata(int server_socket, char * path, t_log * logger) {

	/**	╔═════════════════════════╦═════════════════════╦══════╗
    ║ operation_code (1 byte) ║ path_size (4 bytes) ║ path ║
    ╚═════════════════════════╩═════════════════════╩══════╝ **/
	uint8_t prot_ope_code = 1;
	uint8_t prot_path_size = 4;

	uint8_t  req_ope_code = GET_METADATA_FILE;
	uint32_t req_path_size = strlen(path) + 1;

	int msg_size = sizeof(char) * (prot_ope_code + prot_path_size + req_path_size);
	void * request = malloc(msg_size);
	memcpy(request, &req_ope_code, prot_ope_code);
	memcpy(request + prot_ope_code, &req_path_size, prot_path_size);
	memcpy(request + prot_ope_code + prot_path_size, path, req_path_size);
	socket_send(&server_socket, request, msg_size, 0);
	free(request);

	t_fs_get_file_md_resp * response = malloc(sizeof(t_fs_get_file_md_resp));
	uint8_t resp_prot_code = 2;
	int received_bytes = socket_recv(&server_socket, &(response->exec_code), resp_prot_code);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ SERVER %d >> disconnected", server_socket);
		response->exec_code = DISCONNECTED_SERVER;
		return response;
	}

	if (response->exec_code != SUCCESS)
		return response;

	t_fs_file_metadata * file_md = (t_fs_file_metadata *) malloc(sizeof(t_fs_file_metadata));
	file_md->path = path;
	uint8_t resp_prot_file_size = 4;
	received_bytes = socket_recv(&server_socket, &(file_md->file_size), resp_prot_file_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ SERVER %d >> disconnected", server_socket);
		response->exec_code = DISCONNECTED_SERVER;
		return response;
	}
	uint8_t resp_prot_file_type = 1;
	received_bytes = socket_recv(&server_socket, &(file_md->type), resp_prot_file_type);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ SERVER %d >> disconnected", server_socket);
		response->exec_code = DISCONNECTED_SERVER;
		return response;
	}
	int blocks;
	uint8_t resp_prot_blocks = 4;
	received_bytes = socket_recv(&server_socket, &(blocks), resp_prot_blocks);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ SERVER %d >> disconnected", server_socket);
		response->exec_code = DISCONNECTED_SERVER;
		return response;
	}
	int fb_md_size = sizeof(t_fs_file_block_metadata);
	t_fs_file_block_metadata * block_md;
	t_list * block_list = list_create();
	int i;
	for (i = 0; i < blocks; i++) {
		block_md = (t_fs_file_block_metadata *) malloc(sizeof(t_fs_file_block_metadata));
		received_bytes = socket_recv(&server_socket, block_md, fb_md_size);
		if (received_bytes <= 0) {
			if (logger) log_error(logger, "------ SERVER %d >> disconnected", server_socket);
			response->exec_code = DISCONNECTED_SERVER;
			return response;
		}
		list_add(block_list, block_md);
	}
	file_md->block_list = block_list;
	response->file_metadata = file_md;
	return response;
}

t_fs_get_file_md_req * fs_get_file_metadata_recv_req(int * client_socket, t_log * logger) {
	t_fs_get_file_md_req * request = malloc(sizeof(t_fs_get_file_md_req));
	uint8_t prot_path_size = 4;
	uint32_t path_size;
	int received_bytes = socket_recv(client_socket, &path_size, prot_path_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	request->path = malloc(sizeof(char) * path_size);
	received_bytes = socket_recv(client_socket, (request->path), path_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	request->exec_code = SUCCESS;
	return request;
}

void fs_get_file_metadata_send_resp(int * client_socket, int resp_code, t_fs_file_metadata * file_metadata) {
	uint8_t resp_prot_code = 2;
	uint8_t resp_prot_file_size = 4;
	uint8_t resp_prot_file_type = 1;
	uint8_t resp_prot_blocks = 4;

	int response_size = sizeof(char) * (resp_prot_code);
	if (resp_code == SUCCESS)
		response_size += sizeof(char) * (resp_prot_file_size + resp_prot_file_type + resp_prot_blocks
				+ ((file_metadata->block_list->elements_count) * sizeof(t_fs_file_block_metadata)));

	void * response = malloc(response_size);
	memcpy(response, &resp_code, resp_prot_code);

	if (resp_code == SUCCESS) {
		t_list * block_list = (file_metadata->block_list);
		memcpy(response + resp_prot_code, &(file_metadata->file_size), resp_prot_file_size);
		memcpy(response + resp_prot_code + resp_prot_file_size, &(file_metadata->type), resp_prot_file_type);
		memcpy(response + resp_prot_code + resp_prot_file_size + resp_prot_file_type, &(block_list->elements_count), resp_prot_blocks);

		int fb_md_size = sizeof(t_fs_file_block_metadata);
		int memcpy_pos = resp_prot_code + resp_prot_file_size + resp_prot_file_type + resp_prot_blocks;
		int index = 0;
		while (index < (block_list->elements_count)) {
			memcpy(response + memcpy_pos, ((t_fs_file_block_metadata *) list_get(block_list, index)), fb_md_size);
			memcpy_pos += fb_md_size;
			index++;
		}
	}
	socket_write(client_socket, response, response_size);
	free(response);
}
