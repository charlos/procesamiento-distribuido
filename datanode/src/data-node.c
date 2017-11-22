#include <commons/config.h>
#include <errno.h>
#include <fcntl.h>
#include <shared-library/data-node-prot.h>
#include <shared-library/file-system-prot.h>
#include <shared-library/socket.h>
#include <stdarg.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include "data-node.h"

t_dn_conf * dn_conf;
t_log * logger;
int fs_socket;
void * data_bin_mf_ptr;

void set_block(void);
void ping(void);
void load_dn_properties(char *);
void init(void);
void get_block(void);
void create_logger(void);

int main(int argc, char * argv[]) {
	load_dn_properties(argv[1]); // TODO: ¿el archivo properties se pasa por parámetro?
	create_logger();
	init();

	int ope_code = dn_recv_operation_code(&fs_socket, logger);
	while (ope_code != DISCONNECTED_CLIENT) {
		log_info(logger, " client %d >> operation code : %d", fs_socket, ope_code);
		switch (ope_code) {
		case GET_BLOCK:
			get_block();
			break;
		case SET_BLOCK:
			set_block();
			break;
		case PING:
			ping();
			break;
		default:;
		}
		ope_code = dn_recv_operation_code(&fs_socket, logger);
	}
	close_client(fs_socket);
	return EXIT_SUCCESS;
}

/**
 * @NAME load_dn_properties
 */
void load_dn_properties(char * cfg_path) {
	t_config * conf = config_create(cfg_path);
	dn_conf = malloc(sizeof(t_dn_conf));
	dn_conf->node_name = string_duplicate(config_get_string_value(conf, "NOMBRE_NODO"));
	dn_conf->worker_ip = string_duplicate(config_get_string_value(conf, "IP_NODO"));
	dn_conf->worker_port = string_duplicate(config_get_string_value(conf, "PUERTO_WORKER"));
	dn_conf->fs_ip = string_duplicate(config_get_string_value(conf, "IP_FILESYSTEM"));
	dn_conf->fs_port = string_duplicate(config_get_string_value(conf, "PUERTO_FILESYSTEM"));
	dn_conf->data_bin_path = string_duplicate(config_get_string_value(conf, "RUTA_DATABIN"));
	dn_conf->logfile = string_duplicate(config_get_string_value(conf, "LOGFILE"));
	config_destroy(conf);
}

/**
 * @NAME create_logger
 */
void create_logger(void) {
	logger = log_create((dn_conf->logfile), "data_node_process", false, LOG_LEVEL_TRACE);
}

/**
 * @NAME init
 */
void init(void) {
	struct stat sb;
	if ((stat((dn_conf->data_bin_path), &sb) < 0) || (stat((dn_conf->data_bin_path), &sb) == 0 && !(S_ISREG(sb.st_mode)))) {
		// TODO: error handler
		// data.bin not exists
		exit(EXIT_FAILURE);
	}
	int file_size = sb.st_size;
	int blocks = (file_size / BLOCK_SIZE);
	fs_socket = connect_to_socket((dn_conf->fs_ip), (dn_conf->fs_port));

	char * ip_port = string_new();
	string_append(&ip_port, (dn_conf->worker_ip));
	string_append(&ip_port, ":");
	string_append(&ip_port, (dn_conf->worker_port));

	if ((fs_handshake(fs_socket, DATANODE, (dn_conf->node_name), ip_port, blocks, logger)) != SUCCESS) {
		// TODO: error handler
		// fs handshake error
		exit(EXIT_FAILURE);
	}
	free(ip_port);
	data_bin_mf_ptr = map_file(dn_conf->data_bin_path, O_RDWR);
}

/**
 * @NAME get_block
 */
void get_block(void) {
	t_dn_get_block_req * req = dn_get_block_recv_req(&fs_socket, logger);
	void * buffer = malloc(BLOCK_SIZE);
	memcpy(buffer, data_bin_mf_ptr + (BLOCK_SIZE * (req->block)), BLOCK_SIZE);
	dn_get_block_send_resp(&fs_socket, SUCCESS, buffer);
	free(buffer);
	free(req);
}

/**
 * @NAME set_block
 */
void set_block(void) {
	t_dn_set_block_req * req = dn_set_block_recv_req(&fs_socket, logger);
	memcpy(data_bin_mf_ptr + (BLOCK_SIZE * (req->block)), (req->buffer), BLOCK_SIZE);
	dn_set_block_send_resp(&fs_socket, SUCCESS);
	free(req->buffer);
	free(req);
}

/**
 * @NAME ping
 */
void ping(void) {
	dn_ping_send_resp(&fs_socket, SUCCESS);
}
