#include "file-system-prot.h"
#include "socket.h"

void closure_bc(t_fs_copy_block *);
void closure_fbm(t_fs_file_block_metadata *);

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

int fs_handshake(int server_socket, char type, char * node_name, char * node_ip_port, int blocks, t_log * logger) {

	/**	╔═════════════════════════╦═══════════════╗
		║ operation_code (1 byte) ║ type (1 byte) ║
		╚═════════════════════════╩═══════════════╝ **/

	/**
		DATA_NODE
		╔═════════════════════════╦═══════════════╦═════════════════════════╦═══════════╦═══════════════════════╦═════════╦══════════════════╗
		║ operation_code (1 byte) ║ type (1 byte) ║ node name size (1 byte) ║ node name ║ ip:port size (1 byte) ║ ip:port ║ blocks (4 bytes) ║
		╚═════════════════════════╩═══════════════╩═════════════════════════╩═══════════╩═══════════════════════╩═════════╩══════════════════╝ **/

	uint8_t prot_ope_code = 1;
	uint8_t prot_type = 1;
	uint8_t prot_node_name_size = 1;
	uint8_t prot_node_ip_port_size = 1;
	uint8_t prot_blocks = 4;

	uint8_t req_ope_code = FS_HANDSHAKE;
	uint8_t req_type = type;
	uint8_t req_node_name_size;
	uint8_t req_node_ip_port_size;
	uint32_t req_blocks;

	if (type == 'd') {
		req_node_name_size = strlen(node_name) + 1;
		req_node_ip_port_size = strlen(node_ip_port) + 1;
		req_blocks = blocks;
	}

	int msg_size = sizeof(char) * (prot_ope_code + prot_type);
	if (type == 'd')
		msg_size += prot_node_name_size + req_node_name_size + prot_node_ip_port_size + req_node_ip_port_size + prot_blocks;

	void * request = malloc(msg_size);
	memcpy(request, &req_ope_code, prot_ope_code);
	memcpy(request + prot_ope_code, &req_type, prot_type);
	if (type == 'd') {
		memcpy(request + prot_ope_code + prot_type, &req_node_name_size, prot_node_name_size);
		memcpy(request + prot_ope_code + prot_type + prot_node_name_size, node_name, req_node_name_size);
		memcpy(request + prot_ope_code + prot_type + prot_node_name_size + req_node_name_size, &req_node_ip_port_size, prot_node_ip_port_size);
		memcpy(request + prot_ope_code + prot_type + prot_node_name_size + req_node_name_size + prot_node_ip_port_size, node_ip_port, req_node_ip_port_size);
		memcpy(request + prot_ope_code + prot_type + prot_node_name_size + req_node_name_size + prot_node_ip_port_size + req_node_ip_port_size, &req_blocks, prot_blocks);
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
		uint8_t prot_node_name_size = 1;
		uint8_t node_name_size;
		int received_bytes = socket_recv(client_socket, &node_name_size, prot_node_name_size);
		if (received_bytes <= 0) {
			if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
			request->exec_code = DISCONNECTED_CLIENT;
			return request;
		}
		request->node_name = malloc(sizeof(char) * node_name_size);
		received_bytes = socket_recv(client_socket, (request->node_name), node_name_size);
		if (received_bytes <= 0) {
			if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
			request->exec_code = DISCONNECTED_CLIENT;
			return request;
		}
		uint8_t prot_node_ip_port_size = 1;
		uint8_t node_ip_port_size;
		received_bytes = socket_recv(client_socket, &node_ip_port_size, prot_node_ip_port_size);
		if (received_bytes <= 0) {
			if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
			request->exec_code = DISCONNECTED_CLIENT;
			return request;
		}
		request->node_ip_port = malloc(sizeof(char) * node_ip_port_size);
		received_bytes = socket_recv(client_socket, (request->node_ip_port), node_ip_port_size);
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


/**	╔═════════════╗
	║ UPLOAD FILE ║
	╚═════════════╝ **/

int fs_upload_file(int server_socket, char * path, char type, int buffer_size, void * buffer, t_log * logger) {

	/**	╔═════════════════════════╦═════════════════════╦══════╦═══════════════╦════════════════════════╦════════╗
    	║ operation_code (1 byte) ║ path_size (4 bytes) ║ path ║ type (1 byte) ║ buffer_size (4 bytes)  ║ buffer ║
    	╚═════════════════════════╩═════════════════════╩══════╩═══════════════╩════════════════════════╩════════╝ **/
	uint8_t prot_ope_code = 1;
	uint8_t prot_path_size = 4;
	uint8_t prot_type = 1;
	uint8_t prot_buffer_size = 4;

	uint8_t  req_ope_code = UPLOAD_FILE;
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

t_fs_upload_file_req * fs_upload_file_recv_req(int * client_socket, t_log * logger) {
	t_fs_upload_file_req * request = malloc(sizeof(t_fs_upload_file_req));
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
	received_bytes = socket_recv(client_socket, &(request->file_size), prot_buffer_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	request->buffer = malloc(sizeof(char) * (request->file_size));
	received_bytes = socket_recv(client_socket, (request->buffer), (request->file_size));
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ CLIENT %d >> disconnected", * client_socket);
		request->exec_code = DISCONNECTED_CLIENT;
		return request;
	}
	request->exec_code = SUCCESS;
	return request;
}

void fs_upload_file_send_resp(int * client_socket, int resp_code) {
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
	║ GET METADATA FILE ║
	╚═══════════════════╝ **/

t_fs_get_md_file_resp * fs_get_metadata_file(int server_socket, char * path, t_log * logger) {

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

	t_fs_get_md_file_resp * response = malloc(sizeof(t_fs_get_md_file_resp));
	uint8_t resp_prot_code = 2;
	int received_bytes = socket_recv(&server_socket, &(response->exec_code), resp_prot_code);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ SERVER %d >> disconnected", server_socket);
		response->exec_code = DISCONNECTED_SERVER;
		return response;
	}

	if (response->exec_code != SUCCESS)
		return response;

	t_fs_metadata_file * md_file = (t_fs_metadata_file *) malloc(sizeof(t_fs_metadata_file));
	md_file->path = path;

	uint8_t resp_prot_file_size = 4;
	received_bytes = socket_recv(&server_socket, &(md_file->file_size), resp_prot_file_size);
	if (received_bytes <= 0) {
		if (logger) log_error(logger, "------ SERVER %d >> disconnected", server_socket);
		response->exec_code = DISCONNECTED_SERVER;
		return response;
	}

	uint8_t resp_prot_file_type = 1;
	received_bytes = socket_recv(&server_socket, &(md_file->type), resp_prot_file_type);
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

	t_fs_file_block_metadata * block_md;
	t_fs_copy_block * copy_md;

	uint8_t resp_prot_block_inf = sizeof(uint32_t);
	uint8_t resp_prot_block_size_inf = sizeof(uint32_t);
	uint8_t resp_prot_block_copies = 4;
	uint8_t resp_prot_block_copy_inf = sizeof(t_fs_copy_block);

	int copies;
	int i;
	int j;

	t_list * block_list = list_create();
	for (i = 0; i < blocks; i++) {

		block_md = (t_fs_file_block_metadata *) malloc(sizeof(t_fs_file_block_metadata));

		received_bytes = socket_recv(&server_socket, &(block_md->file_block), resp_prot_block_inf);
		if (received_bytes <= 0) {
			if (logger) log_error(logger, "------ SERVER %d >> disconnected", server_socket);
			list_destroy_and_destroy_elements(block_list, &closure_fbm);
			free(block_md);
			free(md_file);
			response->exec_code = DISCONNECTED_SERVER;
			return response;
		}

		received_bytes = socket_recv(&server_socket, &(block_md->size), resp_prot_block_size_inf);
		if (received_bytes <= 0) {
			if (logger) log_error(logger, "------ SERVER %d >> disconnected", server_socket);
			response->exec_code = DISCONNECTED_SERVER;
			list_destroy_and_destroy_elements(block_list, &closure_fbm);
			free(block_md);
			free(md_file);
			return response;
		}

		received_bytes = socket_recv(&server_socket, &(copies), resp_prot_block_copies);
		if (received_bytes <= 0) {
			if (logger) log_error(logger, "------ SERVER %d >> disconnected", server_socket);
			response->exec_code = DISCONNECTED_SERVER;
			list_destroy_and_destroy_elements(block_list, &closure_fbm);
			free(block_md);
			free(md_file);
			return response;
		}

		t_list * copy_list = list_create();
		for (j = 0; j < copies; j++) {

			copy_md = (t_fs_copy_block *) malloc(resp_prot_block_copy_inf);

			received_bytes = socket_recv(&server_socket, copy_md, resp_prot_block_copy_inf);
			if (received_bytes <= 0) {
				if (logger) log_error(logger, "------ SERVER %d >> disconnected", server_socket);
				response->exec_code = DISCONNECTED_SERVER;
				list_destroy_and_destroy_elements(block_list, &closure_fbm);
				list_destroy_and_destroy_elements(copy_list, &closure_bc);
				free(block_md);
				free(copy_md);
				free(md_file);
				return response;
			}
			list_add(copy_list, copy_md);
		}

		block_md->copies_list = copy_list;
		list_add(block_list, block_md);
	}
	md_file->block_list = block_list;
	response->metadata_file = md_file;
	return response;
}

t_fs_get_md_file_req * fs_get_metadata_file_recv_req(int * client_socket, t_log * logger) {
	t_fs_get_md_file_req * request = malloc(sizeof(t_fs_get_md_file_req));
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

void fs_get_metadata_file_send_resp(int * client_socket, int resp_code, t_fs_metadata_file * md_file) {

	uint8_t resp_prot_code = 2;
	uint8_t resp_prot_file_size = 4;
	uint8_t resp_prot_file_type = 1;
	uint8_t resp_prot_blocks = 4;
	// block inf
	uint8_t resp_prot_block_inf = sizeof(uint32_t);
	uint8_t resp_prot_block_size_inf = sizeof(uint32_t);
	uint8_t resp_prot_block_copies = 4;
	// block copy inf
	uint8_t resp_prot_block_copy_inf = sizeof(t_fs_copy_block);

	int i;
	int j;
	t_fs_file_block_metadata * block_md;

	int response_size = resp_prot_code;
	if (resp_code == SUCCESS) {
		response_size += resp_prot_file_size + resp_prot_file_type + resp_prot_blocks
				+ ((md_file->block_list->elements_count) * (resp_prot_block_inf + resp_prot_block_size_inf + resp_prot_block_copies));
		i = 0;
		while (i < (md_file->block_list->elements_count)) {
			block_md = (t_fs_file_block_metadata *) list_get((md_file->block_list), i);
			response_size += ((block_md->copies_list->elements_count) * resp_prot_block_copy_inf);
			i++;
		}
	}

	void * response = malloc(response_size);
	memcpy(response, &resp_code, resp_prot_code);

	if (resp_code == SUCCESS) {
		t_list * copies_list;
		t_list * block_list = (md_file->block_list);

		int memcpy_pos = resp_prot_code;
		memcpy(response + memcpy_pos, &(md_file->file_size), resp_prot_file_size);
		memcpy_pos += resp_prot_file_size;
		memcpy(response + memcpy_pos, &(md_file->type), resp_prot_file_type);
		memcpy_pos += resp_prot_file_type;
		memcpy(response + memcpy_pos, &(block_list->elements_count), resp_prot_blocks);
		memcpy_pos += resp_prot_blocks;

		i = 0;
		while (i < (block_list->elements_count)) {
			block_md = (t_fs_file_block_metadata *) list_get(block_list, i);

			memcpy(response + memcpy_pos, &(block_md->file_block), resp_prot_block_inf);
			memcpy_pos += resp_prot_block_inf;
			memcpy(response + memcpy_pos, &(block_md->size), resp_prot_block_size_inf);
			memcpy_pos += resp_prot_block_size_inf;

			copies_list = (block_md->copies_list);
			memcpy(response + memcpy_pos, &(copies_list->elements_count), resp_prot_block_copies);
			memcpy_pos += resp_prot_block_copies;

			j = 0;
			while (j < (copies_list->elements_count)) {
				memcpy(response + memcpy_pos, ((t_fs_copy_block *) list_get(copies_list, j)), resp_prot_block_copy_inf);
				memcpy_pos += resp_prot_block_copy_inf;
				j++;
			}
			i++;
		}
	}
	socket_write(client_socket, response, response_size);
	free(response);
}

void closure_fbm(t_fs_file_block_metadata * element) {
	list_destroy_and_destroy_elements((element->copies_list), &closure_bc);
}

void closure_bc(t_fs_copy_block * element) {
	if (element->node) free(element->node);
	if (element->ip_port) free(element->ip_port);
	free(element);
}
