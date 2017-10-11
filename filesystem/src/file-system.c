#include <commons/bitarray.h>
#include <commons/config.h>
#include <commons/config.h>
#include <commons/log.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <pthread.h>
#include <pthread.h>
#include <shared-library/data-node-prot.h>
#include <shared-library/file-system-prot.h>
#include <shared-library/socket.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <readline/readline.h>
#include <readline/history.h>
#include "file-system.h"

#define	SOCKET_BACKLOG 			100
#define	DIRECTORIES_AMOUNT  	100
#define	LOCK_READ 				0
#define	LOCK_WRITE 				1
#define	UNLOCK 					2
#define ROOT					0
#define STEADY 					's'
#define UNSTEADY				'u'
#define BINARY 					'b'
#define TEXT 					't'

int listenning_socket;
t_fs_conf * fs_conf;
t_log * logger;

char status = UNSTEADY; 	// FS state
void * directories_mf_ptr; 	// directories mapped file ptr
t_config * nodes_table; 	// nodes table
t_list * nodes_list;		// nodes list

pthread_rwlock_t * directories_locks;
pthread_mutex_t nodes_table_m_lock;

void write_file(void *, t_config *, int, t_list *);
void upload_file(int *);
void set_file_block(t_config *, int, void *);
void read_file(int *);
void process_request(int *);
void move(char *, char *);
void load_fs_properties(void);
void init_locks(void);
void init(bool);
void get_metadata_file(int *);
void fs_rename(char *, char *);
void fs_make_dir(char *);
void fs_console(void *);
void create_logger(void);
void create_bitmap_for_node(char *, int);
void cpto(char *, char *);
void cpfrom(char *, char *, char);
void closure(void *);
void clean_dir(char *);
void check_fs_status(void);
void add_node(t_list *, char *, int);
t_list * calc_required_blocks_for_txt_file(char *, int);
t_list * calc_required_blocks_for_binary_file(int file_size);
int uploading_file(t_fs_upload_file_req * req);
int rw_lock_unlock(pthread_rwlock_t *, int, int);
int rename_file(int, char *, char *);
int rename_dir(int, char *);
int make_dir(int, char *);
int load_bitmap_node(int *, bool, char *, int);
int get_line_length(char *, int, int);
int get_dir_index_from_table(char *, int, int, t_list *);
int get_dir_index(char *, int, t_list *);
int get_datanode_fd(char *);
int cpy_to_local_dir(char *, char *, char *);
int connect_node(int *, char *, int);
int assign_node_block(char *);
int assign_blocks_to_file(t_config **, int, char *, char, int, t_list *);
char * assign_node(char *);
bool in_ignore_list(int, t_list *);
bool dir_exists(int, int, char *);

void rm_file(char *);
void add_to_release_list(t_list *, char *);
void free_node_block(char *, int);
int get_released_size(t_list *, char *);
void closure_trs(t_fs_to_release *);

int main(int argc, char * argv[]) {
	bool clean_fs = true; // TODO: add clean flag

	load_fs_properties();
	create_logger();
	init(clean_fs);

	// console thread
	pthread_attr_t attr;
	pthread_t thread;
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
	pthread_create(&thread, &attr, &fs_console, NULL);
	pthread_attr_destroy(&attr);

	int * new_client;
	t_fs_handshake_req * hs_req;
	int hs_result;

	// socket thread
	listenning_socket = open_socket(SOCKET_BACKLOG, (fs_conf->port));
	for (;;) {

		new_client = malloc(1);
		* new_client = accept_connection(listenning_socket);
		hs_result = ERROR;

		if ((fs_recv_operation_code(new_client, logger)) == FS_HANDSHAKE) {
			// handshake
			hs_req = fs_handshake_recv_req(new_client, logger);
			if (hs_req->type == DATANODE) {
				hs_result = connect_node(new_client, hs_req->node_name, hs_req->blocks);
				fs_handshake_send_resp(new_client, hs_result);
				if (hs_result != SUCCESS) {
					close_client(* new_client);
					free(new_client);
				}
				free(hs_req->node_name);
				free(hs_req);
				continue;
			} else {
				switch (hs_req->type) {
				case YAMA:
					// TODO
					hs_result = SUCCESS;
					fs_handshake_send_resp(new_client, hs_result);
					free(hs_req);
					break;
				case WORKER:
					// TODO
					hs_result = SUCCESS;
					fs_handshake_send_resp(new_client, hs_result);
					free(hs_req);
					break;
				default:;
				}
			}
		} else {
			close_client(* new_client);
			free(new_client);
			continue;
		}
		if (hs_result == SUCCESS) {
			pthread_attr_t attr;
			pthread_t thread;
			pthread_attr_init(&attr);
			pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
			pthread_create(&thread, &attr, &process_request, (void *) new_client);
			pthread_attr_destroy(&attr);
		}
	}
}

/**
 * @NAME load_fs_properties
 */
void load_fs_properties(void) {
	t_config * conf = config_create("/home/utnso/file-system.cfg");
	fs_conf = malloc(sizeof(t_fs_conf));
	fs_conf->port = config_get_int_value(conf, "PUERTO");
	fs_conf->mount_point = config_get_string_value(conf, "MONTAJE");
	fs_conf->logfile = config_get_string_value(conf, "LOGFILE");
}

/**
 * @NAME create_logger
 */
void create_logger(void) {
	logger = log_create((fs_conf->logfile), "file_system_process", false, LOG_LEVEL_TRACE);
}

/**
 * @NAME check
 */
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

/**
 * @NAME init
 */
void init(bool clean_fs) {

	if (clean_fs)
		clean_dir((fs_conf->mount_point));

	struct stat sb;
	// metadata directory
	char * metadata_path = string_from_format("%s/metadata", (fs_conf->mount_point));
	if ((stat(metadata_path, &sb) < 0) || (stat(metadata_path, &sb) == 0 && !(S_ISDIR(sb.st_mode))))
		mkdir(metadata_path, S_IRWXU | S_IRWXG | S_IRWXO);

	// files directory
	char * files_path = string_from_format("%s/archivos", metadata_path);
	if ((stat(files_path, &sb) < 0) || (stat(files_path, &sb) == 0 && !(S_ISDIR(sb.st_mode))))
		mkdir(files_path, S_IRWXU | S_IRWXG | S_IRWXO);

	// root files directory
	char * root_files_path = string_from_format("%s/%d", files_path, ROOT);
	if ((stat(root_files_path, &sb) < 0) || (stat(root_files_path, &sb) == 0 && !(S_ISDIR(sb.st_mode))))
		mkdir(root_files_path, S_IRWXU | S_IRWXG | S_IRWXO);

	// directories file
	char * directories_file_path = string_from_format("%s/directorios.dat", metadata_path);
	if ((stat(directories_file_path, &sb) < 0) || (stat(directories_file_path, &sb) == 0 && !(S_ISREG(sb.st_mode)))) {
		FILE * dir_file = fopen(directories_file_path, "wb");
		t_fs_directory * dir = (t_fs_directory *) malloc(sizeof(t_fs_directory));
		dir->index = 0;
		strcpy(&(dir->name),"root");
		dir->parent_dir = -1;
		fwrite(dir, sizeof(t_fs_directory), 1, dir_file);
		strcpy(&(dir->name),"\0");
		int index = 1;
		while (index < DIRECTORIES_AMOUNT) {
			dir->index = index;
			fwrite(dir, sizeof(t_fs_directory), 1, dir_file);
			index++;
		}
		free(dir);
		fclose(dir_file);
	}

	directories_mf_ptr = map_file(directories_file_path, O_RDWR);

	// nodes file
	char * nodes_table_file_path = string_from_format("%s/nodos.bin", metadata_path);
	if ((stat(nodes_table_file_path, &sb) < 0) || (stat(nodes_table_file_path, &sb) == 0 && !(S_ISREG(sb.st_mode)))) {
		FILE * nodes_file = fopen(nodes_table_file_path, "w");
		fprintf(nodes_file,"TAMANIO=0\n");
		fprintf(nodes_file,"LIBRE=0\n");
		fprintf(nodes_file,"NODOS=[]\0");
		fclose(nodes_file);
	}

	nodes_table = config_create(nodes_table_file_path);

	// node bitmap directory
	char * nodes_bitmap_path = string_from_format("%s/bitmaps", metadata_path);
	if ((stat(nodes_bitmap_path, &sb) < 0) || (stat(nodes_bitmap_path, &sb) == 0 && !(S_ISDIR(sb.st_mode))))
		mkdir(nodes_bitmap_path, S_IRWXU | S_IRWXG | S_IRWXO);

	// creating node list
	nodes_list = list_create();

	init_locks();

	free(nodes_bitmap_path);
	free(nodes_table_file_path);
	free(directories_file_path);
	free(root_files_path);
	free(files_path);
	free(metadata_path);
}

/**
 * @NAME clean_dir
 */
void clean_dir(char * dir_path) {
	struct dirent * ent;
	char * ent_path;
	DIR * dir = opendir(dir_path);
	while ((ent = readdir(dir)) != NULL) {
		if ((strcmp(ent->d_name, ".") != 0) && (strcmp(ent->d_name, "..") != 0)) {
			ent_path = string_from_format("%s/%s", dir_path, (ent->d_name));
			if ((ent->d_type) == DT_DIR) {
				clean_dir(ent_path);
			}
			remove(ent_path);
			free(ent_path);
		}
	}
	closedir(dir);
}

/**
 * @NAME init_locks
 */
void init_locks(void) {
	int i;
	directories_locks = (pthread_rwlock_t *) malloc(DIRECTORIES_AMOUNT * sizeof(pthread_rwlock_t));
	for (i = 0 ;i < DIRECTORIES_AMOUNT; i++) {
		check((pthread_rwlock_init(directories_locks + i, NULL) != 0), "Error starting cache memory rw lock %d", i);
	}
	pthread_mutex_init(&nodes_table_m_lock, NULL);
}

/**
 * @NAME rw_lock_unlock
 */
int rw_lock_unlock(pthread_rwlock_t * locks, int action, int entry) {
	switch (action) {
	case LOCK_READ :
		pthread_rwlock_rdlock(locks + entry);
		break;
	case LOCK_WRITE :
		pthread_rwlock_wrlock(locks + entry);
		break;
	case UNLOCK :
		pthread_rwlock_unlock(locks + entry);
		break;
	}
	return EXIT_SUCCESS;
}

/**
 * @NAME process_request
 */
void process_request(int * client_socket) {
	int ope_code = fs_recv_operation_code(client_socket, logger);
	while (ope_code != DISCONNECTED_CLIENT) {
		log_info(logger, " client %d >> operation code : %d", * client_socket, ope_code);
		switch (ope_code) {
		case UPLOAD_FILE:
			upload_file(client_socket);
			break;
		case READ_FILE:
			read_file(client_socket);
			break;
		case GET_METADATA_FILE:
			get_metadata_file(client_socket);
			break;
		default:;
		}
		ope_code = fs_recv_operation_code(client_socket, logger);
	}
	close_client(* client_socket);
	free(client_socket);
}

/**
 * @NAME connect_node
 */
int connect_node(int * client_socket, char * node_name, int blocks) {

	pthread_mutex_lock(&nodes_table_m_lock);
	char ** nodes = config_get_array_value(nodes_table, "NODOS");
	t_list * nodes_table_list = list_create();
	bool exits = false;
	int pos = 0;
	while (nodes[pos] != NULL) {
		list_add(nodes_table_list, string_duplicate(nodes[pos]));
		if (!exits)
			exits = (strcmp(node_name, nodes[pos]) == 0);
		free(nodes[pos]);
		pos++;
	}
	free(nodes);

	if (!exits) {
		add_node(nodes_table_list, node_name, blocks);
		create_bitmap_for_node(node_name, blocks);
	}
	int exec = load_bitmap_node(client_socket, !exits, node_name, blocks);
	if (exec == SUCCESS)
		check_fs_status();
	pthread_mutex_unlock(&nodes_table_m_lock);

	list_destroy_and_destroy_elements(nodes_table_list, &closure);
	return exec;
}

/**
 * @NAME add_node
 */
void add_node(t_list * nodes_table_list, char * new_node_name, int blocks) {

	char * data_bin_path = string_from_format("%s/metadata/nodos.bin", (fs_conf->mount_point));
	char * temp_data_bin_path = string_from_format("%s/metadata/temp.bin", (fs_conf->mount_point));

	FILE * temp = fopen(temp_data_bin_path, "w");

	fprintf(temp,"TAMANIO=%d\n", ((config_get_int_value(nodes_table, "TAMANIO")) + blocks));
	fprintf(temp,"LIBRE=%d\n", ((config_get_int_value(nodes_table, "LIBRE")) + blocks));

	int index;
	char * node_list_str = string_new();
	if (nodes_table_list->elements_count > 0) {
		string_append_with_format(&node_list_str, "[%s", (char *) (list_get(nodes_table_list, 0)));
		index = 1;
		while (index < (nodes_table_list->elements_count)) {
			string_append_with_format(&node_list_str, ",%s", (char *) (list_get(nodes_table_list, index)));
			index++;
		}
		string_append_with_format(&node_list_str, ",%s]", new_node_name);
		fprintf(temp,"NODOS=%s\n", node_list_str);
	} else {
		string_append_with_format(&node_list_str, "[%s]", new_node_name);
		fprintf(temp,"NODOS=%s\n", node_list_str);
	}
	free(node_list_str);

	index = 0;
	char * key;
	char * node;
	while (index < (nodes_table_list->elements_count)) {
		node = (char *) (list_get(nodes_table_list, index));
		key = string_from_format("%sTotal", node);
		fprintf(temp,"%s=%d\n", key, (config_get_int_value(nodes_table, key)));
		free(key);
		key = string_from_format("%sLibre", node);
		fprintf(temp,"%s=%d\n", key, (config_get_int_value(nodes_table, key)));
		free(key);
		index++;
	}
	fprintf(temp,"%sTotal=%d\n", new_node_name, blocks);
	fprintf(temp,"%sLibre=%d", new_node_name, blocks);
	fclose(temp);

	config_destroy(nodes_table);
	remove(data_bin_path);
	rename(temp_data_bin_path, data_bin_path);
	nodes_table = config_create(data_bin_path);
	free(temp_data_bin_path);
	free(data_bin_path);
}

/**
 * @NAME create_bitmap_for_node
 */
void create_bitmap_for_node(char * new_node_name, int blocks) {
	struct stat sb;
	char * bitmap_file_path = string_from_format("%s/metadata/bitmaps/%s.bin", (fs_conf->mount_point), new_node_name);
	if ((stat(bitmap_file_path, &sb) < 0) || (stat(bitmap_file_path, &sb) == 0 && !(S_ISREG(sb.st_mode)))) {
		FILE * bitmap_file = fopen(bitmap_file_path, "wb");
		int j = (blocks / 8) + (((blocks % 8) > 0) ? 1 : 0); // 1 byte = 8 bits
		char ch = '\0';
		int i = 0;
		while (i < j) {
			fwrite(&ch, sizeof(char), 1, bitmap_file);
			i++;
		}
		fclose(bitmap_file);
	}
	free(bitmap_file_path);
}

/**
 * @NAME load_bitmap_node
 */
int load_bitmap_node(int * node_fd, bool clean_bitmap, char * node_name, int blocks) {

	t_fs_node * node;
	bool loaded = false;
	int index = 0;
	while (index < (nodes_list->elements_count)) {
		node = (t_fs_node *) list_get(nodes_list, index);
		if (strcmp(node_name, (node->node_name)) == 0) {
			loaded = true;
			break;
		}
		index++;
	}

	if (loaded)
		return ALREADY_CONNECTED;

	char * bitmap_file_path = string_from_format("%s/metadata/bitmaps/%s.bin", (fs_conf->mount_point), node_name);
	t_bitarray * bitmap = bitarray_create_with_mode(map_file(bitmap_file_path, O_RDWR), blocks, MSB_FIRST);
	if (clean_bitmap) {
		int pos = 0;
		while (pos < blocks) {
			bitarray_clean_bit(bitmap, pos);
			pos++;
		}
	}

	node = (t_fs_node *) malloc(sizeof(t_fs_node));
	node->node_name = string_duplicate(node_name);
	node->bitmap = bitmap;
	node->size = blocks;
	node->fd = *node_fd;
	list_add(nodes_list, node);
	free(bitmap_file_path);
	return SUCCESS;
}

/**
 * @NAME check_fs_status
 */
void check_fs_status(void) {

	if ((nodes_list->elements_count) < 1) {
		status = UNSTEADY; // at least two connected nodes
		return;
	}

	bool empty_dir;
	bool all_nodes_connected;
	bool connected_node;
	char * dir_path;
	char * md_file_path;
	char * key_cpy0;
	char * key_cpy1;
	char ** data;
	int block;

	t_config * md_file;
	DIR * dir;
	struct dirent * ent;

	t_fs_directory * fs_dir = (t_fs_directory *) directories_mf_ptr;
	int index = 0;
	while (index < DIRECTORIES_AMOUNT) {
		rw_lock_unlock(directories_locks, LOCK_READ, index);
		empty_dir = true;
		if (fs_dir->parent_dir >= 0) {
			dir_path = string_from_format("%s/metadata/archivos/%d", (fs_conf->mount_point), index);
			dir = opendir(dir_path);
			while ((ent = readdir(dir)) != NULL) {
				if ((strcmp(ent->d_name, ".") != 0) && (strcmp(ent->d_name, "..") != 0)) {
					empty_dir = false;
					all_nodes_connected = true;
					md_file_path = string_from_format("%s/%s", dir_path, (ent->d_name));
					md_file = config_create(md_file_path);
					free(md_file_path);
					block = 0;
					key_cpy0 = string_from_format("BLOQUE%dCOPIA0", block);
					key_cpy1 = string_from_format("BLOQUE%dCOPIA1", block);
					while (config_has_property(md_file, key_cpy0) || config_has_property(md_file, key_cpy1)) {
						connected_node = false;
						if (config_has_property(md_file, key_cpy0)) {
							data = config_get_array_value(md_file, key_cpy0);
							connected_node = (get_datanode_fd(data[0]) != DISCONNECTED_NODE);
							free(data[0]);
							free(data[1]);
							free(data);
						}
						if (!connected_node && config_has_property(md_file, key_cpy1)) {
							data = config_get_array_value(md_file, key_cpy1);
							connected_node = (get_datanode_fd(data[0]) != DISCONNECTED_NODE);
							free(data[0]);
							free(data[1]);
							free(data);
						}
						if (!connected_node) {
							all_nodes_connected = false;
							break;
						}
						block++;
						free(key_cpy1);
						free(key_cpy0);
						key_cpy0 = string_from_format("BLOQUE%dCOPIA0", block);
						key_cpy1 = string_from_format("BLOQUE%dCOPIA1", block);
					}
					free(key_cpy1);
					free(key_cpy0);
					config_destroy(md_file);
				}
				if (!empty_dir && !all_nodes_connected)
					break;
			}
			closedir(dir);
			free(dir_path);
		}
		rw_lock_unlock(directories_locks, UNLOCK, index);
		if (!empty_dir && !all_nodes_connected) {
			status = UNSTEADY;
			return;
		}
		index++;
		fs_dir++;
	}
	status = STEADY;
}

/**
 * @NAME upload_file
 */
void upload_file(int * client_socket) {
	t_fs_upload_file_req * req = fs_upload_file_recv_req(client_socket, logger);
	if (req->exec_code != DISCONNECTED_CLIENT)
		fs_upload_file_send_resp(client_socket, uploading_file(req));
	free(req->path);
	free(req->buffer);
	free(req);
}

/**
 * @NAME read_file
 */
void read_file(int * client_socket) {
	t_fs_read_file_req * req = fs_read_file_recv_req(client_socket, logger);

	char * dir_c = string_duplicate(req->path);
	char * base_c = string_duplicate(req->path);
	char * dir = dirname(dir_c);
	char * file_name = basename(base_c);

	int dir_index = get_dir_index(dir, LOCK_WRITE, NULL);
	if (dir_index == ENOTDIR) {
		free(base_c);
		free(dir_c);
		free(req->path);
		free(req);
		fs_read_file_send_resp(client_socket, ENOTDIR, 0, NULL);
		return;
	}
	struct stat sb;
	char * md_file_path = string_from_format("%s/metadata/archivos/%d/%s", (fs_conf->mount_point), dir_index, file_name);
	if ((stat(md_file_path, &sb) < 0) || (stat(md_file_path, &sb) == 0 && !(S_ISREG(sb.st_mode)))) {
		rw_lock_unlock(directories_locks, UNLOCK, dir_index);
		free(md_file_path);
		free(base_c);
		free(dir_c);
		free(req->path);
		free(req);
		fs_read_file_send_resp(client_socket, ENOENT, 0, NULL);
		return;
	}
	free(req->path);
	free(req);

	//
	// metadata file
	//
	t_config * md_file = config_create(md_file_path);
	free(md_file_path);
	int file_size = config_get_int_value(md_file, "TAMANIO");
	void * buffer = malloc(file_size);

	pthread_mutex_lock(&nodes_table_m_lock);
	t_dn_get_block_resp * dn_block;
	int datanode_fd;
	int bytes;
	int readed_bytes = 0;
	int block = 0;
	char * key_cpy0 = string_from_format("BLOQUE%dCOPIA0", block);
	char * key_cpy1 = string_from_format("BLOQUE%dCOPIA1", block);
	char * bytes_key = string_from_format("BLOQUE%dBYTES", block);
	char ** data;

	while (config_has_property(md_file, key_cpy0) || config_has_property(md_file, key_cpy1)) {

		if (config_has_property(md_file, key_cpy0)) {
			data = config_get_array_value(md_file, key_cpy0);
			datanode_fd = get_datanode_fd(data[0]);
		}
		if (config_has_property(md_file, key_cpy0) && datanode_fd == DISCONNECTED_NODE) {
			free(data[0]);
			free(data[1]);
			free(data);
			data = config_get_array_value(md_file, key_cpy1);
			datanode_fd = get_datanode_fd(data[0]);
		} else if (!config_has_property(md_file, key_cpy0)) {
			data = config_get_array_value(md_file, key_cpy1);
			datanode_fd = get_datanode_fd(data[0]);
		}

		bytes = config_get_int_value(md_file, bytes_key);
		dn_block = dn_get_block(datanode_fd, atoi(data[1]), logger);
		memcpy(buffer, (dn_block->buffer), bytes);
		readed_bytes += bytes;

		free(dn_block->buffer);
		free(dn_block);
		free(data[0]);
		free(data[1]);
		free(data);
		free(bytes_key);
		free(key_cpy1);
		free(key_cpy0);
		block++;
		key_cpy0 = string_from_format("BLOQUE%dCOPIA0", block);
		key_cpy1 = string_from_format("BLOQUE%dCOPIA1", block);
		bytes_key = string_from_format("BLOQUE%dBYTES", block);
	}
	free(bytes_key);
	free(key_cpy1);
	free(key_cpy0);
	pthread_mutex_unlock(&nodes_table_m_lock);
	config_destroy(md_file);
	rw_lock_unlock(directories_locks, UNLOCK, dir_index);

	if (readed_bytes == file_size) {
		fs_read_file_send_resp(client_socket, SUCCESS, file_size, buffer);
	} else {
		fs_read_file_send_resp(client_socket, CORRUPTED_FILE, 0, NULL);
	}
	free(buffer);
	free(base_c);
	free(dir_c);
}

/**
 * @NAME get_metadata_file
 */
void get_metadata_file(int * client_socket) {
	t_fs_get_md_file_req * req = fs_get_metadata_file_recv_req(client_socket, logger);

	char * dir_c = string_duplicate(req->path);
	char * base_c = string_duplicate(req->path);
	char * dir = dirname(dir_c);
	char * file_name = basename(base_c);

	int dir_index = get_dir_index(dir, LOCK_WRITE, NULL);
	if (dir_index == ENOTDIR) {
		free(base_c);
		free(dir_c);
		free(req->path);
		free(req);
		fs_get_metadata_file_send_resp(client_socket, ENOTDIR, NULL);
		return;
	}

	struct stat sb;
	char * md_file_path = string_from_format("%s/metadata/archivos/%d/%s", (fs_conf->mount_point), dir_index, file_name);
	if ((stat(md_file_path, &sb) < 0) || (stat(md_file_path, &sb) == 0 && !(S_ISREG(sb.st_mode)))) {
		rw_lock_unlock(directories_locks, UNLOCK, dir_index);
		free(md_file_path);
		free(base_c);
		free(dir_c);
		free(req->path);
		free(req);
		fs_get_metadata_file_send_resp(client_socket, ENOENT, NULL);
		return;
	}

	t_config * md_file_cfg = config_create(md_file_path);
	free(md_file_path);

	char * type = config_get_string_value(md_file_cfg, "TIPO");
	t_fs_metadata_file * md_file = (t_fs_metadata_file *) malloc(sizeof(t_fs_metadata_file));
	md_file->path = (req->path);
	md_file->file_size = config_get_int_value(md_file_cfg, "TAMANIO");
	md_file->type = (strcmp(type, "TEXTO") == 0) ? TEXT : BINARY;
	md_file->block_list = list_create();
	free(type);

	t_fs_file_block_metadata * block_md;
	int block = 0;
	char * key_cpy0 = string_from_format("BLOQUE%dCOPIA0", block);
	char * key_cpy1 = string_from_format("BLOQUE%dCOPIA1", block);
	char * bytes_key = string_from_format("BLOQUE%dBYTES", block);
	char ** data;

	pthread_mutex_lock(&nodes_table_m_lock);
	while (config_has_property(md_file_cfg, key_cpy0) || config_has_property(md_file_cfg, key_cpy1)) {
		block_md = (t_fs_file_block_metadata *) malloc(sizeof(t_fs_file_block_metadata));
		block_md->file_block = block;

		if (config_has_property(md_file_cfg, key_cpy0)) {
			data = config_get_array_value(md_file_cfg, key_cpy0);
			if (get_datanode_fd(data[0]) != DISCONNECTED_NODE) {
				strcpy(&(block_md->node), data[0]);
				block_md->node_block = atoi(data[1]);
			} else {
				strcpy(&(block_md->node),"\0");
				block_md->node_block = -1;
			}
			free(data[0]);
			free(data[1]);
			free(data);
		}

		if (config_has_property(md_file_cfg, key_cpy1)) {
			data = config_get_array_value(md_file_cfg, key_cpy1);
			if (get_datanode_fd(data[0]) != DISCONNECTED_NODE) {
				strcpy(&(block_md->copy_node), data[0]);
				block_md->copy_node_block = atoi(data[1]);
			} else {
				strcpy(&(block_md->copy_node),"\0");
				block_md->copy_node_block = -1;
			}
			free(data[0]);
			free(data[1]);
			free(data);
		}

		block_md->size = config_get_int_value(md_file_cfg, bytes_key);
		list_add((md_file->block_list), block_md);

		free(bytes_key);
		free(key_cpy1);
		free(key_cpy0);
		block++;
		key_cpy0 = string_from_format("BLOQUE%dCOPIA0", block);
		key_cpy1 = string_from_format("BLOQUE%dCOPIA1", block);
		bytes_key = string_from_format("BLOQUE%dBYTES", block);
	}
	free(bytes_key);
	free(key_cpy1);
	free(key_cpy0);
	pthread_mutex_unlock(&nodes_table_m_lock);

	fs_get_metadata_file_send_resp(client_socket, SUCCESS, md_file);
	list_destroy_and_destroy_elements(md_file->block_list, &closure);
	free(md_file);

	config_destroy(md_file_cfg);
	rw_lock_unlock(directories_locks, UNLOCK, dir_index);
	free(base_c);
	free(dir_c);
	free(req->path);
	free(req);
}

/**
 * @NAME closure
 */
void closure(void * node) {
	free(node);
}












//	╔══════════════════════════════════════════════════════════════╗
//	║ COMMAND: CPFROM                                              ║
//	╚══════════════════════════════════════════════════════════════╝

/**
 * @NAME cpfrom
 */
void cpfrom(char * file_path, char * yamafs_dir, char type) {
	struct stat sb;
	if ((stat(file_path, &sb) < 0) || (stat(file_path, &sb) == 0 && !(S_ISREG(sb.st_mode)))) {
		printf("error: file doesn't exist.\nplease try again...\n");
		return;
	}

	char * file_path_c = string_duplicate(file_path);
	char * file_name = basename(file_path_c);

	t_fs_upload_file_req * req = malloc(sizeof(t_fs_upload_file_req));
	req->path = string_new();
	string_append(&(req->path), yamafs_dir);
	string_append(&(req->path), file_name);
	req->file_size = sb.st_size;
	req->type = type;
	req->buffer = map_file(file_path, O_RDWR);

	switch (uploading_file(req)) {
	case SUCCESS:
		printf("file uploaded successfully!\n");
		break;
	case ENOTDIR:
		printf("error: yamafs dir not exists.\nplease try again...\n");
		break;
	case EEXIST:
		printf("error: file already exists in yamafs.\nplease try again...\n");
		break;
	default:;
	}

	unmap_file(req->buffer, req->file_size);
	free(req->path);
	free(req);
	free(file_path_c);
}

/**
 * @NAME uploading_file
 */
int uploading_file(t_fs_upload_file_req * req) {

	char * dir_c = string_duplicate(req->path);
	char * base_c = string_duplicate(req->path);
	char * dir = dirname(dir_c);
	char * file = basename(base_c);

	int dir_index = get_dir_index(dir, LOCK_WRITE, NULL);
	if (dir_index == ENOTDIR) {
		free(base_c);
		free(dir_c);
		return ENOTDIR;
	}
	struct stat sb;
	char * md_file_path = string_from_format("%s/metadata/archivos/%d/%s", (fs_conf->mount_point), dir_index, file);
	if ((stat(md_file_path, &sb) == 0) && (S_ISREG(sb.st_mode))) {
		rw_lock_unlock(directories_locks, UNLOCK, dir_index);
		free(md_file_path);
		free(base_c);
		free(dir_c);
		return EEXIST;
	}
	free(md_file_path);

	t_list * required_blocks;
	if ((req->type) == TEXT) {
		required_blocks = calc_required_blocks_for_txt_file((req->buffer), (req->file_size));
	} else {
		required_blocks = calc_required_blocks_for_binary_file((req->file_size));
	}

	t_config * md_file;
	pthread_mutex_lock(&nodes_table_m_lock);
	int exec = assign_blocks_to_file(&md_file, dir_index, file, (req->type), (req->file_size), required_blocks);
	if (exec == SUCCESS) {
		write_file((req->buffer), md_file, (req->file_size), required_blocks);
		config_destroy(md_file);
	}
	pthread_mutex_unlock(&nodes_table_m_lock);

	list_destroy_and_destroy_elements(required_blocks, &closure);
	rw_lock_unlock(directories_locks, UNLOCK, dir_index);
	free(base_c);
	free(dir_c);
	return exec;
}

/**
 * @NAME get_dir_index
 */
int get_dir_index(char * path, int lock_type, t_list * ignore_list) {
	if (strcmp(path, "/") == 0) {
		if (ignore_list && in_ignore_list(ROOT, ignore_list)) {
			return ENOTDIR;
		}
		rw_lock_unlock(directories_locks, lock_type, ROOT);
		return ROOT;
	}
	int dir_index;
	int pd_index = ROOT;
	char * path_c = string_duplicate(path);
	char * dir = strtok(path_c, "/");
	while (dir != NULL) {
		dir_index = get_dir_index_from_table(dir, pd_index, lock_type, ignore_list);
		if (pd_index != ROOT)
			rw_lock_unlock(directories_locks, UNLOCK, pd_index);
		if (dir_index == ENOTDIR)
			break;
		pd_index = dir_index;
		dir = strtok(NULL, "/");
	}
	free(path_c);
	return dir_index;
}

/**
 * @NAME get_dir_index_from_table
 */
int get_dir_index_from_table(char * dir, int parent_index, int lock_type, t_list * ignore_list) {
	t_fs_directory * fs_dir = (t_fs_directory *) directories_mf_ptr;
	int index = 0;
	while (index < DIRECTORIES_AMOUNT) {
		if (index == parent_index || (ignore_list && in_ignore_list(index, ignore_list))){
			index++;
			fs_dir++;
			continue;
		}
		rw_lock_unlock(directories_locks, lock_type, index);
		if (((fs_dir->parent_dir) >= 0) && (((fs_dir->parent_dir) == parent_index) && (strcmp((char *)(fs_dir->name), dir) == 0)))
			return index;
		rw_lock_unlock(directories_locks, UNLOCK, index);
		index++;
		fs_dir++;
	}
	return ENOTDIR;
}

/**
 * @NAME in_ignore_list
 */
bool in_ignore_list(int index, t_list * ignore_list) {
	int i = 0;
	while (i < (ignore_list->elements_count)) {
		if (((int) list_get(ignore_list, i)) == index) {
			return true;
		}
		i++;
	}
	return false;
}

/**
 * @NAME calc_required_blocks_for_txt_file
 */
t_list * calc_required_blocks_for_txt_file(char * file, int file_size) {
	t_list * required_blocks = list_create();
	t_fs_required_block * required;
	int block = 0;
	int stored = 0;
	int line_size;
	int file_pos = 0;
	while (file_pos < file_size) {
		line_size = get_line_length(file, file_pos, file_size);
		stored += line_size;
		if (stored > BLOCK_SIZE) {
			required = (t_fs_required_block *) malloc(sizeof(t_fs_required_block));
			required->block = block;
			required->bytes = stored - line_size;
			list_add(required_blocks, required);
			block++;
			stored = line_size;
		}
		file_pos += line_size;
	}
	required = (t_fs_required_block *) malloc(sizeof(t_fs_required_block));
	required->block = block;
	required->bytes = stored;
	list_add(required_blocks, required);
	return required_blocks;
}

/**
 * @NAME calc_required_blocks_for_binary_file
 */
t_list * calc_required_blocks_for_binary_file(int file_size) {
	t_list * required_blocks = list_create();
	t_fs_required_block * required;
	int bytes_to_write = file_size;
	int bytes;
	int block = 0;
	while (bytes_to_write > 0) {
		bytes = (bytes_to_write >= BLOCK_SIZE) ? BLOCK_SIZE : bytes_to_write;
		required = (t_fs_required_block *) malloc(sizeof(t_fs_required_block));
		required->block = block;
		required->bytes = bytes;
		list_add(required_blocks, required);
		block++;
		bytes_to_write -= BLOCK_SIZE;
	}
	return required_blocks;
}

/**
 * @NAME get_line_length
 */
int get_line_length(char * file, int file_pos, int file_size) {
	int j = 0;
	int i = file_pos;
	while (i  < file_size) {
		j++;
		if (file[i] == '\n')
			break;
		i++;
	}
	return j;
}

/**
 * @NAME assign_blocks_to_file
 */
int assign_blocks_to_file(t_config ** file, int dir_index, char * file_name, char type, int file_size, t_list * required_blocks) {

	int block = 0;
	char * cpy_01_node;
	char * cpy_02_node;
	while (block < (required_blocks->elements_count)) {
		cpy_01_node = assign_node(NULL);
		if (!cpy_01_node) {
			return ENOSPC;
		}
		cpy_02_node = assign_node(cpy_01_node);
		if (!cpy_02_node) {
			free(cpy_01_node);
			return ENOSPC;
		}
		free(cpy_01_node);
		free(cpy_02_node);
		block++;
	}

	char * md_file_path = string_from_format("%s/metadata/archivos/%d/%s", (fs_conf->mount_point), dir_index, file_name);
	FILE * md_file = fopen(md_file_path, "w");
	fprintf(md_file,"TAMANIO=%d\n", file_size);

	char * key;
	char * value_str;
	int assigned_block;

	t_fs_required_block * required_block;
	block = 0;
	while (block < (required_blocks->elements_count)) {
		required_block = (t_fs_required_block *) list_get(required_blocks, block);

		cpy_01_node = assign_node(NULL);
		cpy_02_node = assign_node(cpy_01_node);

		key = string_from_format("%sLibre", cpy_01_node);
		value_str = string_itoa((config_get_int_value(nodes_table, key)) - 1);
		config_set_value(nodes_table, key, value_str);
		free(value_str);
		free(key);

		key = string_from_format("%sLibre", cpy_02_node);
		value_str = string_itoa((config_get_int_value(nodes_table, key)) - 1);
		config_set_value(nodes_table, key, value_str);
		free(value_str);
		free(key);

		value_str = string_itoa((config_get_int_value(nodes_table, "LIBRE")) - 2);
		config_set_value(nodes_table, "LIBRE", value_str);
		free(value_str);

		config_save(nodes_table);

		assigned_block = assign_node_block(cpy_01_node);
		fprintf(md_file, "BLOQUE%dCOPIA0=[%s, %d]\n", (required_block->block), cpy_01_node, assigned_block);
		assigned_block = assign_node_block(cpy_02_node);
		fprintf(md_file, "BLOQUE%dCOPIA1=[%s, %d]\n", (required_block->block), cpy_02_node, assigned_block);
		fprintf(md_file, "BLOQUE%dBYTES=%d\n", (required_block->block), (required_block->bytes));

		free(cpy_02_node);
		free(cpy_01_node);
		block++;
	}
	fprintf(md_file,"TIPO=%s", ((type == BINARY) ? "BINARIO" : "TEXTO"));
	fclose(md_file);

	* file = config_create(md_file_path);
	free(md_file_path);
	return SUCCESS;
}

/**
 * @NAME assign_node
 */
char * assign_node(char * unwanted_node) {
	char * selected_node = NULL;
	char * key;
	int max = -1;
	int free_blocks;
	int index = 0;
	t_fs_node * node;
	while (index < (nodes_list->elements_count)) {
		node = (t_fs_node *) list_get(nodes_list, index);
		if (unwanted_node && (strcmp((node->node_name), unwanted_node) == 0)) {
			index++;
		} else {
			key = string_from_format("%sLibre", (node->node_name));
			free_blocks = config_get_int_value(nodes_table, key);
			if ((free_blocks > 0) && ((max < 0) || (free_blocks > max))) {
				if (selected_node)
					free(selected_node);
				max = free_blocks;
				selected_node = string_duplicate(node->node_name);
			}
			free(key);
			index++;
		}
	}
	return selected_node;
}

/**
 * @NAME assign_node_block
 */
int assign_node_block(char * node_name) {
	int block;
	t_fs_node * node;
	int index = 0;
	while (index < (nodes_list->elements_count)) {
		node = (t_fs_node *) list_get(nodes_list, index);
		if (strcmp(node_name, (node->node_name)) == 0) {
			bool its_busy;
			int pos = 0;
			while (pos < (node->size)) {
				its_busy = bitarray_test_bit(node->bitmap, pos);
				if (!its_busy) {
					bitarray_set_bit(node->bitmap, pos);
					block = pos;
					break;
				}
				pos++;
			}
			break;
		}
		index++;
	}
	return block;
}

/**
 * @NAME write_file
 */
void write_file(void * file, t_config * md_file, int file_size, t_list * required_blocks) {
	void * block = malloc(BLOCK_SIZE);
	t_fs_required_block * required_block;
	int writed_bytes = 0;
	int index = 0;
	while ((writed_bytes < file_size) && (index < (required_blocks->elements_count))) {
		required_block = (t_fs_required_block *) list_get(required_blocks, index);
		memset(block, 0, BLOCK_SIZE);
		memcpy(block, file + writed_bytes, (required_block->bytes));
		set_file_block(md_file, (required_block->block), block);
		writed_bytes += (required_block->bytes);
		index++;
	}
	free(block);
}

/**
 * @NAME set_file_block
 */
void set_file_block(t_config * file, int block_number, void * block) {
	char * key = string_from_format("BLOQUE%dCOPIA0", block_number);
	char ** data = config_get_array_value(file, key);
	dn_set_block(get_datanode_fd(data[0]), atoi(data[1]), block, logger);
	free(data[0]);
	free(data[1]);
	free(data);
	free(key);

	key = string_from_format("BLOQUE%dCOPIA1", block_number);
	data = config_get_array_value(file, key);
	dn_set_block(get_datanode_fd(data[0]), atoi(data[1]), block, logger);
	free(data[0]);
	free(data[1]);
	free(data);
	free(key);
}

/**
 * @NAME get_datanode_fd
 */
int get_datanode_fd(char * node_name) {
	t_fs_node * node;
	int index = 0;
	while (index < (nodes_list->elements_count)) {
		node = (t_fs_node *) list_get(nodes_list, index);
		if (strcmp(node_name, (node->node_name)) == 0) {
			return node->fd;
		}
		index++;
	}
	return DISCONNECTED_NODE;
}




//	╔══════════════════════════════════════════════════════════════╗
//	║ COMMAND: CPTO                                                ║
//	╚══════════════════════════════════════════════════════════════╝

/**
 * @NAME cpto
 */
void cpto(char * yamafs_file_path, char * local_dir) {
	struct stat sb;
	if ((stat(local_dir, &sb) < 0) || (stat(local_dir, &sb) == 0 && !(S_ISDIR(sb.st_mode)))) {
		printf("error: local directory doesn't exist.\nplease try again...\n");
		return;
	}

	char * dir_c = string_duplicate(yamafs_file_path);
	char * base_c = string_duplicate(yamafs_file_path);
	char * yamafs_dir = dirname(dir_c);
	char * yamafs_file = basename(base_c);

	int dir_index = get_dir_index(yamafs_dir, LOCK_READ, NULL);
	if (dir_index == ENOTDIR) {
		free(dir_c);
		free(base_c);
		printf("error: yamafs file doesn't exist.\nplease try again...\n");
		return;
	}

	char * md_file_path = string_from_format("%s/metadata/archivos/%d/%s", (fs_conf->mount_point), dir_index, yamafs_file);
	if ((stat(md_file_path, &sb) < 0) || (stat(md_file_path, &sb) == 0 && !(S_ISREG(sb.st_mode)))) {
		rw_lock_unlock(directories_locks, UNLOCK, dir_index);
		free(md_file_path);
		free(dir_c);
		free(base_c);
		printf("error: yamafs file doesn't exist.\nplease try again...\n");
		return;
	}

	switch (cpy_to_local_dir(md_file_path, local_dir, yamafs_file)) {
	case SUCCESS:
		printf("file copied successfully!\n");
		break;
	case CORRUPTED_FILE:
		printf("error: corrupted yamafs file.\n");
		break;
	default:;
	}

	rw_lock_unlock(directories_locks, UNLOCK, dir_index);
	free(md_file_path);
	free(dir_c);
	free(base_c);
}

/**
 * @NAME cpy_to_local_dir
 */
int cpy_to_local_dir(char * md_file_path, char * local_dir, char * yamafs_file) {
	t_config * md_file = config_create(md_file_path);
	int file_size = config_get_int_value(md_file, "TAMANIO");

	char * local_file_path = string_from_format("%s/%s", local_dir, yamafs_file);
	FILE * fs_file = fopen(local_file_path, "wb");

	pthread_mutex_lock(&nodes_table_m_lock);
	t_dn_get_block_resp * dn_block;
	int datanode_fd;
	int bytes;
	int readed_bytes = 0;
	int block = 0;
	char * key_cpy0 = string_from_format("BLOQUE%dCOPIA0", block);
	char * key_cpy1 = string_from_format("BLOQUE%dCOPIA1", block);
	char * bytes_key = string_from_format("BLOQUE%dBYTES", block);
	char ** data;

	while (config_has_property(md_file, key_cpy0) || config_has_property(md_file, key_cpy1)) {

		if (config_has_property(md_file, key_cpy0)) {
			data = config_get_array_value(md_file, key_cpy0);
			datanode_fd = get_datanode_fd(data[0]);
		}
		if (config_has_property(md_file, key_cpy0) && datanode_fd == DISCONNECTED_NODE) {
			free(data[0]);
			free(data[1]);
			free(data);
			data = config_get_array_value(md_file, key_cpy1);
			datanode_fd = get_datanode_fd(data[0]);
		} else if (!config_has_property(md_file, key_cpy0)) {
			data = config_get_array_value(md_file, key_cpy1);
			datanode_fd = get_datanode_fd(data[0]);
		}

		bytes = config_get_int_value(md_file, bytes_key);
		dn_block = dn_get_block(datanode_fd, atoi(data[1]), logger);
		fwrite((dn_block->buffer), bytes, 1, fs_file);
		readed_bytes += bytes;

		free(dn_block->buffer);
		free(dn_block);
		free(data[0]);
		free(data[1]);
		free(data);
		free(bytes_key);
		free(key_cpy1);
		free(key_cpy0);
		block++;
		key_cpy0 = string_from_format("BLOQUE%dCOPIA0", block);
		key_cpy1 = string_from_format("BLOQUE%dCOPIA1", block);
		bytes_key = string_from_format("BLOQUE%dBYTES", block);
	}
	free(bytes_key);
	free(key_cpy1);
	free(key_cpy0);
	pthread_mutex_unlock(&nodes_table_m_lock);

	fclose(fs_file);
	config_destroy(md_file);

	if (readed_bytes == file_size) {
		free(local_file_path);
		return SUCCESS;
	} else {
		remove(local_file_path);
		free(local_file_path);
		return CORRUPTED_FILE;
	}
}




//	╔══════════════════════════════════════════════════════════════╗
//	║ COMMAND: RENAME                                              ║
//	╚══════════════════════════════════════════════════════════════╝

/**
 * @NAME fs_rename
 */
void fs_rename(char * yamafs_path, char * new_name) {

	int dir_index = get_dir_index(yamafs_path, LOCK_WRITE, NULL);

	if (dir_index == ENOTDIR) {
		// is a file
		char * dir_c = string_duplicate(yamafs_path);
		char * base_c = string_duplicate(yamafs_path);
		char * yamafs_dir = dirname(dir_c);
		char * yamafs_file = basename(base_c);

		dir_index = get_dir_index(yamafs_dir, LOCK_WRITE, NULL);
		if (dir_index == ENOTDIR) {
			free(base_c);
			free(dir_c);
			printf("error: file/directory doesn't exist.\nplease try again...\n");
			return;
		}

		struct stat sb;
		char * md_file_path = string_from_format("%s/metadata/archivos/%d/%s", (fs_conf->mount_point), dir_index, yamafs_file);
		if ((stat(md_file_path, &sb) < 0) || (stat(md_file_path, &sb) == 0 && !(S_ISREG(sb.st_mode)))) {
			rw_lock_unlock(directories_locks, UNLOCK, dir_index);
			free(md_file_path);
			free(base_c);
			free(dir_c);
			printf("error: file/directory doesn't exist.\nplease try again...\n");
			return;
		}

		switch (rename_file(dir_index, yamafs_file, new_name)) {
		case SUCCESS:
			printf("the file was renamed successfully!\n");
			break;
		case EEXIST:
			printf("error: file/directory already exists.\nplease try again...\n");
			break;
		case ERROR:
			perror("error");
			break;
		default:;
		}

		rw_lock_unlock(directories_locks, UNLOCK, dir_index);
		free(md_file_path);
		free(base_c);
		free(dir_c);
	} else {
		// is a directory
		if (dir_index == ROOT) {
			rw_lock_unlock(directories_locks, UNLOCK, dir_index);
			printf("error: root directory cannot be renamed.\nplease try again...\n");
			return;
		}
		switch (rename_dir(dir_index, new_name)) {
		case SUCCESS:
			printf("the directory was renamed successfully!\n");
			break;
		case EEXIST:
			printf("error: file/directory already exists.\nplease try again...\n");
			break;
		default:;
		}
		rw_lock_unlock(directories_locks, UNLOCK, dir_index);
	}
}

/**
 * @NAME rename_file
 */
int rename_file(int dir_index, char * yamafs_file, char * new_file_name) {
	char * old_md_file_path = string_from_format("%s/metadata/archivos/%d/%s", (fs_conf->mount_point), dir_index, yamafs_file);
	char * new_md_file_path = string_from_format("%s/metadata/archivos/%d/%s", (fs_conf->mount_point), dir_index, new_file_name);
	struct stat sb;
	if ((stat(new_md_file_path, &sb) == 0) && (S_ISREG(sb.st_mode))) {
		free(new_md_file_path);
		free(old_md_file_path);
		return EEXIST;
	}
	int exec = rename(old_md_file_path, new_md_file_path);
	free(new_md_file_path);
	free(old_md_file_path);
	if (exec < 0)
		return ERROR;
	return SUCCESS;
}

/**
 * @NAME rename_dir
 */
int rename_dir(int dir_index, char * new_dir_name) {
	t_fs_directory * dir = (t_fs_directory *) (directories_mf_ptr + (sizeof(t_fs_directory) * dir_index));
	rw_lock_unlock(directories_locks, LOCK_WRITE, (dir->parent_dir));
	if (dir_exists(dir_index, (dir->parent_dir), new_dir_name)) {
		rw_lock_unlock(directories_locks, UNLOCK, (dir->parent_dir));
		return EEXIST;
	}
	rw_lock_unlock(directories_locks, UNLOCK, (dir->parent_dir));
	strcpy(&(dir->name), new_dir_name);
	return SUCCESS;
}

/**
 * @NAME dir_exists
 */
bool dir_exists(int dir_index, int parent_dir_index, char * dir_name) {
	t_fs_directory * fs_dir = (t_fs_directory *) directories_mf_ptr;
	int dir_exists = false;
	int index = 0;
	while (!dir_exists && index < DIRECTORIES_AMOUNT) {
		if ((index == dir_index) || (index == parent_dir_index)) {
			index++;
			fs_dir++;
			continue;
		}
		rw_lock_unlock(directories_locks, LOCK_READ, index);
		dir_exists = ((fs_dir->parent_dir) >= 0)
						&& ((fs_dir->parent_dir) == parent_dir_index)
							&& (strcmp((char *)(fs_dir->name), dir_name) == 0);
		rw_lock_unlock(directories_locks, UNLOCK, index);
		index++;
		fs_dir++;
	}
	return dir_exists;
}




//	╔══════════════════════════════════════════════════════════════╗
//	║ COMMAND: MKDIR                                               ║
//	╚══════════════════════════════════════════════════════════════╝

/**
 * @NAME fs_make_dir
 */
void fs_make_dir(char * path_dir) {
	int dir_index = get_dir_index(path_dir, LOCK_READ, NULL);
	if (dir_index == ENOTDIR) {

		char * dir_c = string_duplicate(path_dir);
		char * base_c = string_duplicate(path_dir);
		char * parent = dirname(dir_c);
		char * dir = basename(base_c);

		int parent_index = get_dir_index(parent, LOCK_WRITE, NULL);
		if (parent_index == ENOTDIR) {
			free(base_c);
			free(dir_c);
			printf("error: parent directory doesn't exist.\nplease try again...\n");
			return;
		}

		switch (make_dir(parent_index, dir)) {
		case SUCCESS:
			printf("the directory was created successfully!\n");
			break;
		case ENOSPC:
			printf("error: no space left on device.\n");
			break;
		default:;
		}

		rw_lock_unlock(directories_locks, UNLOCK, parent_index);
		free(base_c);
		free(dir_c);
	} else {
		rw_lock_unlock(directories_locks, UNLOCK, dir_index);
		printf("error: directory already exists.\nplease try again...\n");
	}
}

/**
 * @NAME make_dir
 */
int make_dir(int parent_dir, char * dir) {
	t_fs_directory * fs_dir = (t_fs_directory *) directories_mf_ptr;
	int index = 0;
	while (index < DIRECTORIES_AMOUNT) {
		if (index == parent_dir){
			index++;
			fs_dir++;
			continue;
		}

		rw_lock_unlock(directories_locks, LOCK_WRITE, index);
		if ((fs_dir->parent_dir) < 0) {
			strcpy(&(fs_dir->name), dir);
			fs_dir->parent_dir = parent_dir;

			struct stat sb;
			char * dir_path = string_from_format("%s/metadata/archivos/%d", (fs_conf->mount_point), index);
			if ((stat(dir_path, &sb) < 0) || (stat(dir_path, &sb) == 0 && !(S_ISDIR(sb.st_mode))))
				mkdir(dir_path, S_IRWXU | S_IRWXG | S_IRWXO);
			free(dir_path);

			rw_lock_unlock(directories_locks, UNLOCK, index);
			return SUCCESS;
		}
		rw_lock_unlock(directories_locks, UNLOCK, index);
		index++;
		fs_dir++;
	}
	return ENOSPC;
}




//	╔══════════════════════════════════════════════════════════════╗
//	║ COMMAND: MV                                                  ║
//	╚══════════════════════════════════════════════════════════════╝

/**
 * @NAME move
 */
void move(char * path, char * dest_dir_path) {
	if (strcmp(path, "/") == 0) {
		printf("error: root directory cannot be moved.\nplease try again...\n");
		return;
	}
	if (strcmp(path, dest_dir_path) == 0) {
		printf("error: the source path can not be the same as target path.\nplease try again...\n");
		return;
	}
	int dest_dir_index = get_dir_index(dest_dir_path, LOCK_WRITE, NULL);
	if (dest_dir_index == ENOTDIR) {
		printf("error: destination directory doesn't exist.\nplease try again...\n");
		return;
	}
	t_list * ignore_list = list_create();
	list_add(ignore_list, dest_dir_index);

	int dir_index = get_dir_index(path, LOCK_WRITE, ignore_list);
	if (dir_index == ENOTDIR) {
		// is a file
		char * dir_c = string_duplicate(path);
		char * base_c = string_duplicate(path);
		char * yamafs_dir = dirname(dir_c);
		char * yamafs_file = basename(base_c);

		dir_index = get_dir_index(yamafs_dir, LOCK_WRITE, ignore_list);
		if (dir_index == ENOTDIR) {
			rw_lock_unlock(directories_locks, UNLOCK, dest_dir_index);
			list_destroy(ignore_list);
			free(base_c);
			free(dir_c);
			printf("error: file directory doesn't exist.\nplease try again...\n");
			return;
		}

		struct stat sb;
		char * old_md_file_path = string_from_format("%s/metadata/archivos/%d/%s", (fs_conf->mount_point), dir_index, yamafs_file);
		if ((stat(old_md_file_path, &sb) < 0) || (stat(old_md_file_path, &sb) == 0 && !(S_ISREG(sb.st_mode)))) {
			rw_lock_unlock(directories_locks, UNLOCK, dest_dir_index);
			rw_lock_unlock(directories_locks, UNLOCK, dir_index);
			list_destroy(ignore_list);
			free(old_md_file_path);
			free(base_c);
			free(dir_c);
			printf("error: file doesn't exist.\nplease try again...\n");
			return;
		}

		char * new_md_file_path = string_from_format("%s/metadata/archivos/%d/%s", (fs_conf->mount_point), dest_dir_index, yamafs_file);
		if ((stat(new_md_file_path, &sb) == 0) && (S_ISREG(sb.st_mode))) {
			rw_lock_unlock(directories_locks, UNLOCK, dest_dir_index);
			rw_lock_unlock(directories_locks, UNLOCK, dir_index);
			list_destroy(ignore_list);
			free(new_md_file_path);
			free(old_md_file_path);
			free(base_c);
			free(dir_c);
			printf("error: file already exists on target directory.\nplease try again...\n");
			return;
		}

		if (rename(old_md_file_path, new_md_file_path) < 0) {
			perror("error");
		} else {
			printf("the file was moved successfully!\n");
		}

		rw_lock_unlock(directories_locks, UNLOCK, dest_dir_index);
		rw_lock_unlock(directories_locks, UNLOCK, dir_index);
		list_destroy(ignore_list);
		free(new_md_file_path);
		free(old_md_file_path);
		free(base_c);
		free(dir_c);
	} else {
		// is a directory
		t_fs_directory * dir = (t_fs_directory *) (directories_mf_ptr + (sizeof(t_fs_directory) * dir_index));
		if (!dir_exists(dir_index, dest_dir_index, (dir->name))) {
			dir->parent_dir = dest_dir_index;
			printf("the directory was moved successfully!\n");
		} else {
			printf("error: directory already exists on target directory.\nplease try again...\n");
		}
		rw_lock_unlock(directories_locks, UNLOCK, dir_index);
		rw_lock_unlock(directories_locks, UNLOCK, dest_dir_index);
		list_destroy(ignore_list);
	}
}




//	╔══════════════════════════════════════════════════════════════╗
//	║ COMMAND: LS                                                  ║
//	╚══════════════════════════════════════════════════════════════╝

/**
 * @NAME ls
 */
void ls (char * dir_path) {
	int dir_index = get_dir_index(dir_path, LOCK_READ, NULL);
	if (dir_index == ENOTDIR) {
		printf("error: directory not exists.\nplease try again...\n");
		return;
	}

	char * yamafs_dir_path = string_from_format("%s/metadata/archivos/%d", (fs_conf->mount_point), dir_index);
	struct dirent * ent;
	DIR * yamafs_dir = opendir(yamafs_dir_path);
	while ((ent = readdir(yamafs_dir)) != NULL) {
		if ((strcmp(ent->d_name, ".") != 0) && (strcmp(ent->d_name, "..") != 0)) {
			printf("-> %s (regular file)\n", (ent->d_name));
		}
	}
	closedir(yamafs_dir);
	free(yamafs_dir_path);

	t_fs_directory * fs_dir = (t_fs_directory *) directories_mf_ptr;
	int index = 0;
	while (index < DIRECTORIES_AMOUNT) {
		if (index == dir_index) {
			index++;
			fs_dir++;
			continue;
		}
		rw_lock_unlock(directories_locks, LOCK_READ, index);
		if (((fs_dir->parent_dir) >= 0) && ((fs_dir->parent_dir) == dir_index)) {
			printf("-> %s (directory)\n", (fs_dir->name));
		}
		rw_lock_unlock(directories_locks, UNLOCK, index);
		index++;
		fs_dir++;
	}
	rw_lock_unlock(directories_locks, UNLOCK, dir_index);
}




//	╔══════════════════════════════════════════════════════════════╗
//	║ COMMAND: FORMAT                                              ║
//	╚══════════════════════════════════════════════════════════════╝

/**
 * @NAME format
 */
void format() {

	pthread_mutex_lock(&nodes_table_m_lock);
	t_list * nodes_table_list = list_create();
	char ** nodes = config_get_array_value(nodes_table, "NODOS");
	int pos = 0;
	while (nodes[pos] != NULL) {
		list_add(nodes_table_list, string_duplicate(nodes[pos]));
		free(nodes[pos]);
		pos++;
	}
	free(nodes);

	char * data_bin_path = string_from_format("%s/metadata/nodos.bin", (fs_conf->mount_point));
	char * temp_data_bin_path = string_from_format("%s/metadata/temp.bin", (fs_conf->mount_point));
	FILE * temp = fopen(temp_data_bin_path, "w");

	char * node_list_str = string_new();
	string_append_with_format(&node_list_str, "[%s", (char *) (list_get(nodes_table_list, 0)));
	int index = 1;
	while (index < (nodes_table_list->elements_count)) {
		string_append_with_format(&node_list_str, ",%s", (char *) (list_get(nodes_table_list, index)));
		index++;
	}
	string_append(&node_list_str, "]");
	free(node_list_str);

	char * key;
	char * node;
	int node_size;
	index = 0;
	while (index < (nodes_table_list->elements_count)) {
		node = (char *) (list_get(nodes_table_list, index));
		key = string_from_format("%sTotal", node);
		node_size = config_get_int_value(nodes_table, key);
		fprintf(temp,"%s=%d\n", key, node_size);
		free(key);
		key = string_from_format("%sLibre", node);
		fprintf(temp,"%s=%d\n", key, node_size);
		free(key);
		index++;
	}
	fprintf(temp,"TAMANIO=%d\n", config_get_int_value(nodes_table, "TAMANIO"));
	fprintf(temp,"LIBRE=%d", config_get_int_value(nodes_table, "TAMANIO"));
	fclose(temp);

	config_destroy(nodes_table);
	remove(data_bin_path);
	rename(temp_data_bin_path, data_bin_path);
	nodes_table = config_create(data_bin_path);

	list_destroy_and_destroy_elements(nodes_table_list, &closure);
	free(data_bin_path);
	free(temp_data_bin_path);

	t_fs_node * connected_node;
	index = 0;
	while (index < (nodes_list->elements_count)) {
		connected_node = (t_fs_node *) list_get(nodes_list, index);
		pos = 0;
		while (pos < (connected_node->size)) {
			bitarray_clean_bit((connected_node->bitmap), pos);
			pos++;
		}
		index++;
	}
	pthread_mutex_unlock(&nodes_table_m_lock);

	rw_lock_unlock(directories_locks, LOCK_WRITE, ROOT);
	char * dir_path = string_from_format("%s/metadata/archivos/%d", (fs_conf->mount_point), ROOT);
	clean_dir(dir_path);
	free(dir_path);
	rw_lock_unlock(directories_locks, UNLOCK, ROOT);

	struct stat sb;
	t_fs_directory * fs_dir = (t_fs_directory *) directories_mf_ptr;
	fs_dir++;
	index = 1;
	while (index < DIRECTORIES_AMOUNT) {
		rw_lock_unlock(directories_locks, LOCK_WRITE, index);
		dir_path = string_from_format("%s/metadata/archivos/%d", (fs_conf->mount_point), index);
		if (stat(dir_path, &sb) == 0 && (S_ISDIR(sb.st_mode)))
			clean_dir(dir_path);
		free(dir_path);
		fs_dir->parent_dir = -1;
		strcpy(&(fs_dir->name),"\0");
		rw_lock_unlock(directories_locks, UNLOCK, index);
		index++;
		fs_dir++;
	}
}




//	╔══════════════════════════════════════════════════════════════╗
//	║ COMMAND: RM FILE                                             ║
//	╚══════════════════════════════════════════════════════════════╝

/**
 * @NAME rm_file
 */
void rm_file(char * file_path) {

	char * dir_c = string_duplicate(file_path);
	char * base_c = string_duplicate(file_path);
	char * yamafs_dir = dirname(dir_c);
	char * yamafs_file = basename(base_c);

	int dir_index = get_dir_index(yamafs_dir, LOCK_WRITE, NULL);
	if (dir_index == ENOTDIR) {
		free(base_c);
		free(dir_c);
		printf("error: file directory doesn't exist.\nplease try again...\n");
		return;
	}

	struct stat sb;
	char * md_file_path = string_from_format("%s/metadata/archivos/%d/%s", (fs_conf->mount_point), dir_index, yamafs_file);
	if ((stat(md_file_path, &sb) < 0) || (stat(md_file_path, &sb) == 0 && !(S_ISREG(sb.st_mode)))) {
		rw_lock_unlock(directories_locks, UNLOCK, dir_index);
		free(md_file_path);
		free(base_c);
		free(dir_c);
		printf("error: file doesn't exist.\nplease try again...\n");
		return;
	}
	free(base_c);
	free(dir_c);

	t_config * md_file_cfg = config_create(md_file_path);

	int block = 0;
	char * key_cpy0 = string_from_format("BLOQUE%dCOPIA0", block);
	char * key_cpy1 = string_from_format("BLOQUE%dCOPIA1", block);
	char ** data;
	t_list * release_list = list_create();

	pthread_mutex_lock(&nodes_table_m_lock);
	//
	// bitmap node update
	//
	while (config_has_property(md_file_cfg, key_cpy0) || config_has_property(md_file_cfg, key_cpy1)) {
		if (config_has_property(md_file_cfg, key_cpy0)) {
			data = config_get_array_value(md_file_cfg, key_cpy0);
			add_to_release_list(release_list, data[0]);
			free_node_block(data[0], atoi(data[1]));
			free(data[0]);
			free(data[1]);
			free(data);
		}
		if (config_has_property(md_file_cfg, key_cpy1)) {
			data = config_get_array_value(md_file_cfg, key_cpy1);
			add_to_release_list(release_list, data[0]);
			free_node_block(data[0], atoi(data[1]));
			free(data[0]);
			free(data[1]);
			free(data);
		}
		free(key_cpy1);
		free(key_cpy0);
		block++;
		key_cpy0 = string_from_format("BLOQUE%dCOPIA0", block);
		key_cpy1 = string_from_format("BLOQUE%dCOPIA1", block);
	}
	free(key_cpy1);
	free(key_cpy0);
	config_destroy(md_file_cfg);
	remove(md_file_path);
	free(md_file_path);
	//
	// node table update
	//
	char * data_bin_path = string_from_format("%s/metadata/nodos.bin", (fs_conf->mount_point));
	char * temp_data_bin_path = string_from_format("%s/metadata/temp.bin", (fs_conf->mount_point));
	FILE * temp = fopen(temp_data_bin_path, "w");

	char ** nodes = config_get_array_value(nodes_table, "NODOS");
	char * node_list_str = string_new();
	string_append_with_format(&node_list_str, "[%s", nodes[0]);
	int index = 1;
	while (nodes[index] != NULL) {
		string_append_with_format(&node_list_str, ",%s", nodes[index]);
		index++;
	}
	string_append(&node_list_str, "]");
	fprintf(temp,"NODOS=%s\n", node_list_str);
	free(node_list_str);

	int fs_free_size = config_get_int_value(nodes_table, "LIBRE");
	int node_free_size;
	char * key;
	index = 0;
	while (nodes[index] != NULL) {
		key = string_from_format("%sTotal", nodes[index]);
		fprintf(temp,"%s=%d\n", key, (config_get_int_value(nodes_table, key)));
		free(key);
		key = string_from_format("%sLibre", nodes[index]);
		node_free_size = config_get_int_value(nodes_table, key) + get_released_size(release_list, nodes[index]);
		fs_free_size += node_free_size;
		fprintf(temp,"%s=%d\n", key, node_free_size);
		free(key);
		index++;
	}

	fprintf(temp,"TAMANIO=%d\n", config_get_int_value(nodes_table, "TAMANIO"));
	fprintf(temp,"LIBRE=%d", fs_free_size);
	fclose(temp);

	config_destroy(nodes_table);
	remove(data_bin_path);
	rename(temp_data_bin_path, data_bin_path);
	nodes_table = config_create(data_bin_path);
	pthread_mutex_unlock(&nodes_table_m_lock);

	list_destroy_and_destroy_elements(release_list, &closure_trs);
	rw_lock_unlock(directories_locks, UNLOCK, dir_index);
	index = 0;
	while (nodes[index] != NULL) {
		free(nodes[index]);
		index++;
	}
	free(nodes);
	free(temp_data_bin_path);
	free(data_bin_path);
}

/**
 * @NAME add_to_release_list
 */
void add_to_release_list(t_list * release_list, char * node) {
	t_fs_node * connected_node;
	int index = 0;
	while (index < (nodes_list->elements_count)) {
		connected_node = (t_fs_node *) list_get(nodes_list, index);
		if (strcmp(node, (connected_node->node_name)) == 0) {
			t_fs_to_release * to_release;
			index = 0;
			while (index < (release_list->elements_count)) {
				to_release = (t_fs_to_release *) list_get(release_list, index);
				if (strcmp(node, (to_release->node)) == 0) {
					(to_release->blocks)++;
					return;
				}
				index++;
			}
			to_release = malloc(sizeof(t_fs_to_release));
			to_release->node = string_duplicate(node);
			to_release->blocks = 1;
			list_add(release_list, to_release);
			return;
		}
		index++;
	}
}

/**
 * @NAME free_node_block
 */
void free_node_block(char * node, int block) {
	t_fs_node * connected_node;
	int index = 0;
	while (index < (nodes_list->elements_count)) {
		connected_node = (t_fs_node *) list_get(nodes_list, index);
		if (strcmp(node, (connected_node->node_name)) == 0) {
			bitarray_clean_bit((connected_node->bitmap), block);
			return;
		}
		index++;
	}
}

/**
 * @NAME get_released_size
 */
int get_released_size(t_list * release_list, char * node) {
	t_fs_to_release * to_release;
	int index = 0;
	while (index < (release_list->elements_count)) {
		to_release = (t_fs_to_release *) list_get(release_list, index);
		if (strcmp(node, (to_release->node)) == 0) {
			return (to_release->blocks);
		}
		index++;
	}
	return 0;
}

/**
 * @NAME closure_trs
 */
void closure_trs(t_fs_to_release * to_r) {
	free(to_r->node);
	free(to_r);
}





















//	╔═════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
//	║                                                     CONSOLE                                                     ║
//	╚═════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝

char ** fileman_completion(char* line, int start, int end);
int execute_line(char * line);
t_command * find_command(char * line);
void cpfrom_wrapper(char ** args);
void cpto_wrapper(char ** args);
void rename_wrapper(char **args);
void mkdir_wrapper(char **args);
void mv_wrapper(char **args);

t_command comandos[] = {
		{"cpfrom", cpfrom_wrapper},
		{"cpto", cpto_wrapper},
		{"rename", rename_wrapper},
		{"mv", mv_wrapper},
		{"mkdir", mkdir_wrapper},
		{ (char *)NULL, (Function *)NULL}
};

/**
 * @NAME fs_console
 */
void fs_console(void * unused) { // TODO

	rl_attempted_completion_function = (CPPFunction *)fileman_completion;

	while(1){
		char* line = readline("Ingrese comando:\n>");
		if(strcmp(line, "exit") == 0){
			free(line);
			break;
		}
		int a = execute_line(line);
		if (line) add_history(line);
		free(line);
	}
}

int execute_line(char* line){

	char* line_aux = string_duplicate(line);
	int i=0;
	while(line_aux[i] != ' ')i++;
	char* word = malloc(sizeof(char)*i);
	strncpy(word, line_aux, i);
	word[i] = '\0';

	t_command *command = find_command (word);

	i++;
	char **args = string_split(line_aux + i, " ");
	if (!command){
		fprintf (stderr, "%s: No such command for YAMA FileSystem Console.\n", word);
		return (-1);
	}
	free(word);
	free(line_aux);
	/* Call the function. */
	(*(command->funcion)) (args);
	return 1;
}

t_command *find_command(char* line){
	register int i;

	for (i = 0; comandos[i].name; i++)
		if (strcmp (line, comandos[i].name) == 0)
			return (&comandos[i]);

	return ((t_command *)NULL);
}

char* command_generator(char* text, int state) {
	static int list_index, len;
	char *name;

	/* If this is a new word to complete, initialize now.  This includes
	     saving the length of TEXT for efficiency, and initializing the index
	     variable to 0. */
	if (!state)
	{
		list_index = 0;
		len = strlen (text);
	}

	/* Return the next name which partially matches from the command list. */
	while (name = comandos[list_index].name)
	{
		list_index++;

		if (strncmp (name, text, len) == 0)
			return (string_duplicate(name));
	}

	/* If no names matched, then return NULL. */
	return ((char *)NULL);
}

char ** fileman_completion(char* line, int start, int end){
	char **matches;

	matches = (char **)NULL;

	/* If this word is at the start of the line, then it is a command
	     to complete.  Otherwise it is the name of a file in the current
	     directory. */
	if (start == 0)
		matches = completion_matches(line, command_generator);

	return (matches);
}

void cpfrom_wrapper(char **args){
	cpfrom(args[0], args[1], *args[2]);
}

void cpto_wrapper(char **args){
	cpto(args[0], args[1]);
}

void rename_wrapper(char **args){
	fs_rename(args[0], args[1]);
}

void mkdir_wrapper(char **args){
	fs_make_dir(args[0]);
}

void mv_wrapper(char **args){
	move(args[0], args[1]);
}









//
// TODO: ¿Que sucede al desconectar un nodo?
//
// Implementación:
//
// Si el FS que se levanta, corresponde a un estado anterior, se debe verificar que
// 	- El nombre de los archivos bitmap sean igual al configurado como nombre del nodo en Data-Node
//  - El archivo de directorios contenga el dato index


//
// http://www.chuidiang.org/clinux/sockets/socketselect.php
// http://www.delorie.com/djgpp/doc/libc/libc_646.html
// https://www.lemoda.net/c/recursive-directory/
