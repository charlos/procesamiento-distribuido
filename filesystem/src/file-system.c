
#include <commons/log.h>
#include <commons/config.h>
#include <pthread.h>
#include <stdlib.h>
#include <shared-library/file-system-prot.h>
#include <shared-library/socket.h>
#include "file-system.h"

#define	SOCKET_BACKLOG 			100

int listenning_socket;
t_fs_conf * fs_conf;
t_log * logger;

void fs_console(void *);
void process_request(int *);
void handshake(int *);
void load_file(int *);
void read_file(int *);
void get_file_metadata(int *);
void load_fs_properties(void);
void create_logger(void);
void init(void);
void closure(t_fs_file_block_metadata *);

int main(int argc, char * argv[]) {
	load_fs_properties();
	create_logger();
	init();

	// console thread
	pthread_attr_t attr;
	pthread_t thread;
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
	pthread_create(&thread, &attr, &fs_console, NULL);
	pthread_attr_destroy(&attr);

	// socket thread
	int * new_sock;
	listenning_socket = open_socket(SOCKET_BACKLOG, (fs_conf->port));
	for (;;) {
		new_sock = malloc(1);
		* new_sock = accept_connection(listenning_socket);

		pthread_attr_t attr;
		pthread_t thread;
		pthread_attr_init(&attr);
		pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
		pthread_create(&thread, &attr, &process_request, (void *) new_sock);
		pthread_attr_destroy(&attr);
	}
}

/**
 * @NAME load_fs_properties
 */
void load_fs_properties(void) {
	t_config * conf = config_create("/home/utnso/file-system.cfg");
	fs_conf = malloc(sizeof(t_fs_conf));
	fs_conf->port = config_get_int_value(conf, "PUERTO");
	fs_conf->logfile = config_get_string_value(conf, "LOGFILE");
}

/**
 * @NAME create_logger
 */
void create_logger(void) {
	logger = log_create((fs_conf->logfile), "file_system_process", false, LOG_LEVEL_TRACE);
}

/**
 * @NAME init
 */
void init(void) {

}

/**
 * @NAME process_request
 */
void process_request(int * client_socket) {
	int ope_code = fs_recv_operation_code(client_socket, logger);
	while (ope_code != DISCONNECTED_CLIENT) {
		log_info(logger, " client %d >> operation code : %d", * client_socket, ope_code);
		switch (ope_code) {
		case FS_HANDSHAKE:
			handshake(client_socket);
			break;
		case LOAD_FILE:
			load_file(client_socket);
			break;
		case READ_FILE:
			read_file(client_socket);
			break;
		case GET_METADATA_FILE:
			get_file_metadata(client_socket);
			break;
		default:;
		}
		ope_code = fs_recv_operation_code(client_socket, logger);
	}
	close_client(* client_socket);
	free(client_socket);
	return;
}

/**
 * @NAME handshake
 */
void handshake(int * client_socket) {
	t_fs_handshake_req * req = fs_handshake_recv_req(client_socket, logger);
	//
	// TODO
	//
	fs_handshake_send_resp(client_socket, SUCCESS);

	free(req);
}

/**
 * @NAME load_file
 */
void load_file(int * client_socket) {
	t_fs_load_file_req * req = fs_load_file_recv_req(client_socket, logger);
	//
	// TODO
	//
	fs_load_file_send_resp(client_socket, SUCCESS);

	free(req->path);
	free(req->buffer);
	free(req);
}

/**
 * @NAME read_file
 */
void read_file(int * client_socket) {
	t_fs_read_file_req * req = fs_read_file_recv_req(client_socket, logger);
	//
	// TODO
	//

	// MOCK
	char * file_content = "YAMA FILE CONTENT SO-2C-2017\n";
	int buffer_size = strlen(file_content) + 1;
	void * buffer = malloc(buffer_size);
	memcpy(buffer, file_content, buffer_size);
	fs_read_file_send_resp(client_socket, SUCCESS, buffer_size, buffer);
	free(buffer);

	free(req->path);
	free(req);
}

/**
 * @NAME get_file_metadata
 */
void get_file_metadata(int * client_socket) {
	t_fs_get_file_md_req * req = fs_get_file_metadata_recv_req(client_socket, logger);
	//
	// TODO
	//

	// MOCK
	t_fs_file_metadata * file_md = (t_fs_file_metadata *) malloc(sizeof(t_fs_file_metadata));
	file_md->path = (req->path);
	file_md->file_size = 6144;
	file_md->type = 'b';
	file_md->block_list = list_create();

	t_fs_file_block_metadata * block_md;
	int i;
	for (i = 0; i < 6; i++) {
		block_md = (t_fs_file_block_metadata *) malloc(sizeof(t_fs_file_block_metadata));
		block_md->file_block = i;
		block_md->node = i;
		block_md->node_block = 10 + i;
		block_md->copy_node = i + 1;
		block_md->copy_node_block = 20 + i;
		block_md->size = 1024;
		list_add((file_md->block_list), block_md);
	}

	fs_get_file_metadata_send_resp(client_socket, SUCCESS, file_md);

	list_destroy_and_destroy_elements(file_md->block_list, &closure);
	free(file_md);
	free(req->path);
	free(req);
}

void closure(t_fs_file_block_metadata * block_md) {
	free(block_md);
}








/**
 * @NAME fs_console
 */
void fs_console(void * unused) {
	char * input = NULL;
	char * command = NULL;
	char * param01 = NULL;
	char * param02 = NULL;
	char * param03 = NULL;
	size_t len = 0;
	ssize_t read;
	while ((read = getline(&input, &len, stdin)) != -1) {
		if (read > 0) {
			input[read-1] = '\0';
			char * token = strtok(input, " ");
			if (token != NULL) command = token;
			token = strtok(NULL, " ");
			if (token != NULL) param01 = token;
			token = strtok(NULL, " ");
			if (token != NULL) param02 = token;
			token = strtok(NULL, " ");
			if (token != NULL) param03 = token;
		}
	}
}
