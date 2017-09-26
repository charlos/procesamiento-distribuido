
#include <shared-library/data-node-prot.h>
#include <shared-library/file-system-prot.h>
#include <commons/config.h>
#include <shared-library/socket.h>
#include <sys/stat.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <stdarg.h>
#include "data-node.h"

t_dn_conf * dn_conf;
t_log * logger;
int fs_socket;
void * data_bin_mf_ptr;

void load_dn_properties(void);
void create_logger(void);
void init(void);
void get_block(int *);
void set_block(int *);

int main(int argc, char * argv[]) {
	load_dn_properties();
	create_logger();
	init();

	//
	// TODO: FS intergration
	//

}

/**
 * @NAME load_dn_properties
 */
void load_dn_properties(void) {
	t_config * conf = config_create("/home/utnso/node.cfg");
	dn_conf = malloc(sizeof(t_dn_conf));
	dn_conf->node_name = config_get_string_value(conf, "NOMBRE_NODO");
	dn_conf->port = config_get_string_value(conf, "PUERTO_DATANODE");
	dn_conf->fs_ip = config_get_string_value(conf, "IP_FILESYSTEM");
	dn_conf->fs_port = config_get_string_value(conf, "PUERTO_FILESYSTEM");
	dn_conf->data_bin_path = config_get_string_value(conf, "RUTA_DATABIN");
	dn_conf->logfile = config_get_string_value(conf, "LOGFILE");
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
	}

	int file_size = sb.st_size;
	int blocks = (file_size / BLOCK_SIZE);

	//
	// TODO: FS intergration
	//
	fs_socket = connect_to_socket((dn_conf->fs_ip), (dn_conf->fs_port));
	int resp_code = fs_handshake(fs_socket, 'd', (dn_conf->node_name), blocks, logger);

	data_bin_mf_ptr = map_file(dn_conf->data_bin_path, O_RDWR);
}

/**
 * @NAME get_block
 */
void get_block(int * client_socket) {
	t_dn_get_block_req * req = dn_get_block_recv_req(client_socket, logger);
	void * buffer = malloc(BLOCK_SIZE);
	memcpy(buffer, data_bin_mf_ptr + (BLOCK_SIZE * (req->block)), BLOCK_SIZE);
	dn_get_block_send_resp(client_socket, SUCCESS, buffer);
	free(buffer);
	free(req);
}

/**
 * @NAME set_block
 */
void set_block(int * client_socket) {
	t_dn_set_block_req * req = dn_set_block_recv_req(client_socket, logger);
	memcpy(data_bin_mf_ptr + (BLOCK_SIZE * (req->block)), (req->buffer), BLOCK_SIZE);
	dn_set_block_send_resp(client_socket, SUCCESS);
	free(req->buffer);
	free(req);
}
