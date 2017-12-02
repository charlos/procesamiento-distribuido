#include <commons/bitarray.h>
#include <commons/config.h>
#include <commons/log.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <pthread.h>
#include <readline/history.h>
#include <readline/readline.h>
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
#include "file-system.h"

#define	SOCKET_BACKLOG 			100
#define	DIRECTORIES_AMOUNT  	100
#define	LOCK_READ 				0
#define	LOCK_WRITE 				1
#define	UNLOCK 					2
#define ROOT					0
#define STEADY 					's'
#define UNSTEADY				'u'

const char * CLEAN_FLAG = "-clean";

int listenning_socket;
t_fs_conf * fs_conf;
t_log * logger;

char status = UNSTEADY; 	// FS state
void * directories_mf_ptr; 	// directories mapped file ptr
t_config * nodes_table; 	// nodes table
t_list * nodes_list;		// nodes list

pthread_rwlock_t * directories_locks;
pthread_mutex_t nodes_table_m_lock;

int node_index = 0;
int pre_ass_node_index = 0;

void write_file(void *, t_config *, int, t_list *);
void upload_file(int *);
void set_file_block(t_config *, int, void *);
void rm_file_block(char *, int, int);
void rm_file(char *);
void rm_dir(char *);
void process_request(int *);
void move(char *, char *);
void load_fs_properties(void);
void init_locks(void);
void init(bool);
void get_metadata_file(int *);
void fs_rename(char *, char *);
void fs_make_dir(char *);
void fs_console(void *);
void free_node_block(char *, int);
void create_logger(void);
void create_bitmap_for_node(char *, int);
void cpto(char *, char *);
void cpfrom(char *, char *, char);
void cpblock(char *, int, char *);
void closure_trs(t_fs_to_release *);
void closure(void *);
void clean_dir(char *);
void check_fs_status(void);
void add_to_release_list(t_list *, char *);
void add_node(char *, int);
t_list * calc_required_blocks_for_txt_file(char *, int);
t_list * calc_required_blocks_for_binary_file(int file_size);
int uploading_file(t_fs_upload_file_req * req);
int rw_lock_unlock(pthread_rwlock_t *, int, int);
int rename_file(int, char *, char *);
int rename_dir(int, char *);
int make_dir(int, char *);
void load_bitmap_node(int *, char *, char *, int);
int get_released_size(t_list *, char *);
int get_line_length(char *, int, int);
int get_dir_index_from_table(char *, int, int, t_list *);
int get_dir_index(char *, int, t_list *);
int get_datanode_fd(char *);
int cpy_to_local_dir(char *, char *, char *);
int connect_node(int *, char *, char *, int);
int check_fs_space(t_list *);
int assign_node_block(char *);
int assign_blocks_to_file(t_config **, int, char *, char, int, t_list *);
char * pre_assign_node(char *);
char * pre_assign_node_s(char *);
char * get_datanode_ip_port(char * node_name);
char * assign_node(char *);
bool is_dir(char *, int, t_fs_directory *);
bool is_number(char *);
bool is_disconnected(t_fs_node *);
bool in_locked_index_list(int, t_list *);
bool dir_exists(int, int, char *);

int main(int argc, char * argv[]) {

	bool clean_fs = ((argc > 1) &&  (strcmp(argv[1], CLEAN_FLAG) == 0));

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
				hs_result = connect_node(new_client, hs_req->node_name, hs_req->node_ip_port, hs_req->blocks);
				fs_handshake_send_resp(new_client, hs_result);
				if (hs_result == SUCCESS) {
					if (status == UNSTEADY) {
						pthread_mutex_lock(&nodes_table_m_lock);
						check_fs_status();
						pthread_mutex_unlock(&nodes_table_m_lock);
					}
				} else {
					close_client(* new_client);
					free(new_client);
				}
				free(hs_req->node_name);
				free(hs_req);
				continue;
			} else {
				if (status == STEADY) {
					hs_result = SUCCESS;
				} else {
					hs_result = UNSTEADY_FS;
				}
				fs_handshake_send_resp(new_client, hs_result);
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
	t_config * conf = config_create("./file-system.cfg");
	fs_conf = malloc(sizeof(t_fs_conf));
	fs_conf->port = config_get_int_value(conf, "PUERTO");
	fs_conf->mount_point = string_duplicate(config_get_string_value(conf, "MONTAJE"));
	fs_conf->logfile = string_duplicate(config_get_string_value(conf, "LOGFILE"));
	config_destroy(conf);
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

	// temp directory
	char * temp_path = string_from_format("%s/temp", (fs_conf->mount_point));
	if ((stat(temp_path, &sb) < 0) || (stat(temp_path, &sb) == 0 && !(S_ISDIR(sb.st_mode))))
		mkdir(temp_path, S_IRWXU | S_IRWXG | S_IRWXO);

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

	char ** nodes = config_get_array_value(nodes_table, "NODOS");
	int pos = 0;
	while (nodes[pos] != NULL) {

		char * key = string_from_format("%sTotal", nodes[pos]);
		int blocks = config_get_int_value(nodes_table, key);
		free(key);

		char * bitmap_file_path = string_from_format("%s/metadata/bitmaps/%s.bin", (fs_conf->mount_point), nodes[pos]);
		t_bitarray * bitmap = bitarray_create_with_mode(map_file(bitmap_file_path, O_RDWR), blocks, MSB_FIRST);

		t_fs_node * node = (t_fs_node *) malloc(sizeof(t_fs_node));
		node->node_name = string_duplicate(nodes[pos]);
		node->ip_port = string_new();
		node->bitmap = bitmap;
		node->size = blocks;
		node->fd = -1;
		list_add(nodes_list, node);
		free(bitmap_file_path);

		pos++;
	}
	
	init_locks();

	free(nodes_bitmap_path);
	free(nodes_table_file_path);
	free(directories_file_path);
	free(root_files_path);
	free(files_path);
	free(temp_path);
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
int connect_node(int * node_fd, char * node_name, char * node_ip_port, int blocks) {

	pthread_mutex_lock(&nodes_table_m_lock);
	t_fs_node * node;
	bool node_exists = false;
	bool already_connected = false;
	int index = 0;
	while (index < (nodes_list->elements_count)) {
		node = (t_fs_node *) list_get(nodes_list, index);
		if (strcmp(node_name, (node->node_name)) == 0) {
			node_exists = true;
			if (!is_disconnected(node)) {
				already_connected = true;
			}
			break;
		}
		index++;
	}

	int exec;
	if (!node_exists) {
		add_node(node_name, blocks);
		create_bitmap_for_node(node_name, blocks);
		load_bitmap_node(node_fd, node_name, node_ip_port, blocks);
		exec = SUCCESS;
	} else {
		if (already_connected) {
			exec = ALREADY_CONNECTED;
		} else {
			node = (t_fs_node *) list_get(nodes_list, index);
			node->fd = * node_fd;
			free(node->ip_port);
			node->ip_port = string_duplicate(node_ip_port);
			exec = SUCCESS;
		}
	}
	pthread_mutex_unlock(&nodes_table_m_lock);
	return exec;
}

/**
 * @NAME add_node
 */
void add_node(char * new_node_name, int blocks) {

	char * data_bin_path = string_from_format("%s/metadata/nodos.bin", (fs_conf->mount_point));
	char * temp_data_bin_path = string_from_format("%s/metadata/temp.bin", (fs_conf->mount_point));

	FILE * temp = fopen(temp_data_bin_path, "w");

	fprintf(temp,"TAMANIO=%d\n", ((config_get_int_value(nodes_table, "TAMANIO")) + blocks));
	fprintf(temp,"LIBRE=%d\n", ((config_get_int_value(nodes_table, "LIBRE")) + blocks));

	int index;
	t_fs_node * node;
	char * node_list_str = string_new();
	if (nodes_list->elements_count > 0) {
		node = (t_fs_node *) list_get(nodes_list, 0);
		string_append_with_format(&node_list_str, "[%s", (node->node_name));
		index = 1;
		while (index < (nodes_list->elements_count)) {
			node = (t_fs_node *) list_get(nodes_list, index);
			string_append_with_format(&node_list_str, ",%s", (node->node_name));
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
	while (index < (nodes_list->elements_count)) {
		node = (t_fs_node *) list_get(nodes_list, index);
		key = string_from_format("%sTotal", (node->node_name));
		fprintf(temp,"%s=%d\n", key, (config_get_int_value(nodes_table, key)));
		free(key);
		key = string_from_format("%sLibre", (node->node_name));
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
void load_bitmap_node(int * node_fd, char * node_name, char * node_ip_port, int blocks) {

	char * bitmap_file_path = string_from_format("%s/metadata/bitmaps/%s.bin", (fs_conf->mount_point), node_name);
	t_bitarray * bitmap = bitarray_create_with_mode(map_file(bitmap_file_path, O_RDWR), blocks, MSB_FIRST);

	int pos = 0;
	while (pos < blocks) {
		bitarray_clean_bit(bitmap, pos);
		pos++;
	}

	t_fs_node * node = (t_fs_node *) malloc(sizeof(t_fs_node));
	node->node_name = string_duplicate(node_name);
	node->ip_port = string_duplicate(node_ip_port);
	node->bitmap = bitmap;
	node->size = blocks;
	node->fd = * node_fd;
	list_add(nodes_list, node);
	free(bitmap_file_path);
}

/**
 * @NAME check_fs_status
 */
void check_fs_status(void) {

	bool empty_dir;
	bool all_nodes_connected;
	bool at_least_one_copy;
	char * dir_path;
	char * md_file_path;
	char * bytes_key;
	char * cpy_key;
	char ** data;

	int keys_amount;
	int block;
	int cpy;

	t_config * md_file;
	DIR * dir;
	struct dirent * ent;

	t_fs_directory * fs_dir = (t_fs_directory *) directories_mf_ptr;
	int index = 0;
	while (index < DIRECTORIES_AMOUNT) {
		rw_lock_unlock(directories_locks, LOCK_READ, index);
		empty_dir = true;
		if (index == 0 || fs_dir->parent_dir >= 0) {
			dir_path = string_from_format("%s/metadata/archivos/%d", (fs_conf->mount_point), index);
			dir = opendir(dir_path);
			while ((ent = readdir(dir)) != NULL) {
				if ((strcmp(ent->d_name, ".") != 0) && (strcmp(ent->d_name, "..") != 0)) {
					empty_dir = false;
					all_nodes_connected = true;
					md_file_path = string_from_format("%s/%s", dir_path, (ent->d_name));
					md_file = config_create(md_file_path);
					free(md_file_path);
					keys_amount = config_keys_amount(md_file);
					block = 0;
					bytes_key = string_from_format("BLOQUE%dBYTES", block);
					while (config_has_property(md_file, bytes_key)) {
						at_least_one_copy = false;
						cpy = 0;
						while (!at_least_one_copy && cpy < keys_amount) {
							cpy_key = string_from_format("BLOQUE%dCOPIA%d", block, cpy);
							if (config_has_property(md_file, cpy_key)) {
								data = config_get_array_value(md_file, cpy_key);
								at_least_one_copy = (get_datanode_fd(data[0]) != DISCONNECTED_NODE);
								free(data[0]);
								free(data[1]);
								free(data);
							}
							free(cpy_key);
							cpy++;
						}
						if (!at_least_one_copy) {
							all_nodes_connected = false;
							break;
						}
						block++;
						free(bytes_key);
						bytes_key = string_from_format("BLOQUE%dBYTES", block);
					}
					free(bytes_key);
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

	t_fs_file_block_metadata * block_md;
	t_fs_copy_block * copy_md;
	int block = 0;
	int cpy;
	int keys_amount = config_keys_amount(md_file_cfg);

	char * bytes_key = string_from_format("BLOQUE%dBYTES", block);
	char * cpy_key;
	char ** data;

	pthread_mutex_lock(&nodes_table_m_lock);

	bool corrupted_block;
	while (config_has_property(md_file_cfg, bytes_key)) {

		corrupted_block = true;

		block_md = (t_fs_file_block_metadata *) malloc(sizeof(t_fs_file_block_metadata));
		block_md->file_block = block;
		block_md->size = config_get_int_value(md_file_cfg, bytes_key);
		block_md->copies_list = list_create();

		cpy = 0;
		while (cpy < keys_amount) {
			cpy_key = string_from_format("BLOQUE%dCOPIA%d", block, cpy);
			if (config_has_property(md_file_cfg, cpy_key)) {
				data = config_get_array_value(md_file_cfg, cpy_key);
				if (get_datanode_fd(data[0]) != DISCONNECTED_NODE) {
					corrupted_block = false;
					copy_md = (t_fs_copy_block *) malloc(sizeof(t_fs_copy_block));
					strcpy(&(copy_md->node), data[0]);
					strcpy(&(copy_md->ip_port), get_datanode_ip_port(data[0]));
					copy_md->node_block = atoi(data[1]);
					list_add((block_md->copies_list), copy_md);
				}
				free(data[0]);
				free(data[1]);
				free(data);
			}
			free(cpy_key);
			cpy++;
		}
		list_add((md_file->block_list), block_md);
		free(bytes_key);

		block++;
		bytes_key = string_from_format("BLOQUE%dBYTES", block);

		if (corrupted_block)
			break;
	}
	free(bytes_key);
	pthread_mutex_unlock(&nodes_table_m_lock);

	if (corrupted_block) {
		fs_get_metadata_file_send_resp(client_socket, CORRUPTED_FILE, NULL);
	} else {
		fs_get_metadata_file_send_resp(client_socket, SUCCESS, md_file);
	}

	int index = 0;
	while (index < (md_file->block_list->elements_count)) {
		block_md = (t_fs_file_block_metadata *) list_get((md_file->block_list), index);
		list_destroy_and_destroy_elements(block_md->copies_list, &closure);
		index++;
	}
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

/**
 * @NAME get_datanode_ip_port
 */
char * get_datanode_ip_port(char * node_name) {
	t_fs_node * node;
	int index = 0;
	while (index < (nodes_list->elements_count)) {
		node = (t_fs_node *) list_get(nodes_list, index);
		if (strcmp(node_name, (node->node_name)) == 0) {
			return node->ip_port;
		}
		index++;
	}
	return DISCONNECTED_NODE;
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
	if (strcmp(yamafs_dir, "/") != 0)
		string_append(&(req->path), "/");
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
	case UNSUPPORTED_FILE_TYPE:
		printf("error: unsupported file type.\nplease try again...\n");
		break;
	case EEXIST:
		printf("error: file already exists in yamafs.\nplease try again...\n");
		break;
	case ENOSPC:
		printf("error: no space left on device.\nplease try again...\n");
		break;
	default:;
	}

	unmap_file((req->buffer), (req->file_size));
	free(req->path);
	free(req);
	free(file_path_c);
}

/**
 * @NAME uploading_file
 */
int uploading_file(t_fs_upload_file_req * req) {

	if (((req->type) != BINARY) && ((req->type) != TEXT)) {
		return UNSUPPORTED_FILE_TYPE;
	}

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
int get_dir_index(char * path, int lock_type, t_list * locked_index_list) {
	if (strcmp(path, "/") == 0) {
		if (locked_index_list && in_locked_index_list(ROOT, locked_index_list)) {
			return ROOT;
		}
		rw_lock_unlock(directories_locks, lock_type, ROOT);
		return ROOT;
	}
	int dir_index;
	int pd_index = ROOT;
	char * path_c = string_duplicate(path);
	char * dir = strtok(path_c, "/");
	while (dir != NULL) {
		dir_index = get_dir_index_from_table(dir, pd_index, lock_type, locked_index_list);
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
int get_dir_index_from_table(char * dir, int parent_index, int lock_type, t_list * locked_index_list) {
	t_fs_directory * fs_dir = (t_fs_directory *) directories_mf_ptr;
	int index = 0;
	while (index < DIRECTORIES_AMOUNT) {

		if (index == parent_index) {
			index++;
			fs_dir++;
			continue;
		}

		if (locked_index_list && in_locked_index_list(index, locked_index_list)) {
			if (is_dir(dir, parent_index, fs_dir)) {
				return index;
			}
			index++;
			fs_dir++;
			continue;
		}

		rw_lock_unlock(directories_locks, lock_type, index);
		if (is_dir(dir, parent_index, fs_dir))
			return index;
		rw_lock_unlock(directories_locks, UNLOCK, index);

		index++;
		fs_dir++;
	}
	return ENOTDIR;
}

/**
 * @NAME is_dir
 */
bool is_dir(char * dir, int parent_index, t_fs_directory * fs_dir) {
	return ((fs_dir->parent_dir) >= 0) && (((fs_dir->parent_dir) == parent_index) && (strcmp((char *)(fs_dir->name), dir) == 0));
}

/**
 * @NAME in_locked_index_list
 */
bool in_locked_index_list(int index, t_list * locked_index_list) {
	int i = 0;
	while (i < (locked_index_list->elements_count)) {
		if (((int) list_get(locked_index_list, i)) == index) {
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

	if (check_fs_space(required_blocks) == ENOSPC) {
		return ENOSPC;
	}

	char * md_file_path = string_from_format("%s/metadata/archivos/%d/%s", (fs_conf->mount_point), dir_index, file_name);
	FILE * md_file = fopen(md_file_path, "w");
	fprintf(md_file,"TAMANIO=%d\n", file_size);

	char * cpy_01_node;
	char * cpy_02_node;
	char * key;
	char * value_str;

	int assigned_block;
	int block = 0;

	t_fs_required_block * required_block;
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
		fprintf(md_file, "BLOQUE%dCOPIA0=[%s,%d]\n", (required_block->block), cpy_01_node, assigned_block);
		assigned_block = assign_node_block(cpy_02_node);
		fprintf(md_file, "BLOQUE%dCOPIA1=[%s,%d]\n", (required_block->block), cpy_02_node, assigned_block);
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
 * @NAME is_disconnected
 */
bool is_disconnected(t_fs_node * node) {
	return (dn_ping((node->fd), logger) == DISCONNECTED_SERVER);
}

/**
 * @NAME check_fs_space
 */
int check_fs_space(t_list * required_blocks) {

	char * key;
	int index = 0;
	t_fs_node * node;
	while (index < (nodes_list->elements_count)) {
		node = (t_fs_node *) list_get(nodes_list, index);
		key = string_from_format("%sLibre", (node->node_name));
		node->free_blocks = config_get_int_value(nodes_table, key);
		free(key);
		index++;
	}

	int block = 0;
	char * cpy_01_node;
	char * cpy_02_node;
	pre_ass_node_index = node_index;
	while (block < (required_blocks->elements_count)) {
		cpy_01_node = pre_assign_node_s(NULL);
		if (!cpy_01_node) {
			return ENOSPC;
		}
		cpy_02_node = pre_assign_node_s(cpy_01_node);
		if (!cpy_02_node) {
			free(cpy_01_node);
			return ENOSPC;
		}
		free(cpy_01_node);
		free(cpy_02_node);
		block++;
	}
	return SUCCESS;
}

/**
 * @NAME pre_assign_node
 */
char * pre_assign_node(char * unwanted_node) {

	t_fs_node * selected_node = NULL;
	t_fs_node * node;

	int index = 0;
	while (index < (nodes_list->elements_count)) {
		node = (t_fs_node *) list_get(nodes_list, index);
		if (unwanted_node && (strcmp((node->node_name), unwanted_node) == 0)) {
			index++;
		} else {
			if (((node->free_blocks) > 0) && (get_datanode_fd(node->node_name) != DISCONNECTED_NODE)) {
				selected_node = node;
				break;
			}
			index++;
		}
	}

	if (!selected_node) {
		return NULL;
	} else {
		(selected_node->free_blocks)--;
		return string_duplicate(selected_node->node_name);
	}
}

/**
 * @NAME pre_assign_node_s
 */
char * pre_assign_node_s(char * unwanted_node) {

	t_fs_node * selected_node = NULL;
	t_fs_node * node;

	int index = pre_ass_node_index;
	while (index < (nodes_list->elements_count)) {
		node = (t_fs_node *) list_get(nodes_list, index);
		if (unwanted_node && (strcmp((node->node_name), unwanted_node) == 0)) {
			index++;
			if (index >= (nodes_list->elements_count))
				index = 0;
		} else {
			if (((node->free_blocks) > 0) && (get_datanode_fd(node->node_name) != DISCONNECTED_NODE)) {
				selected_node = node;
				index++;
				if (index >= (nodes_list->elements_count))
					index = 0;
				break;
			}
			index++;
			if (index >= (nodes_list->elements_count))
				index = 0;
		}
		if (index == pre_ass_node_index) {
			break;
		}
	}

	pre_ass_node_index = index;

	if (!selected_node) {
		return NULL;
	} else {
		(selected_node->free_blocks)--;
		return string_duplicate(selected_node->node_name);
	}
}


/**
 * @NAME assign_node
 */
char * assign_node(char * unwanted_node) {
	char * selected_node = NULL;
	char * key;
	int max = -1;
	int free_blocks;

	t_fs_node * node;
	int index = node_index;
	while (index < (nodes_list->elements_count)) {
		node = (t_fs_node *) list_get(nodes_list, index);
		if (unwanted_node && (strcmp((node->node_name), unwanted_node) == 0)) {
			index++;
			if (index >= (nodes_list->elements_count))
				index = 0;
		} else {
			if ((node->fd) >= 0) {
				// connected_node
				key = string_from_format("%sLibre", (node->node_name));
				free_blocks = config_get_int_value(nodes_table, key);
				free(key);
				if (free_blocks > 0) {
					selected_node = string_duplicate(node->node_name);
					index++;
					if (index >= (nodes_list->elements_count))
						index = 0;
					break;
				}
			}
			index++;
			if (index >= (nodes_list->elements_count))
				index = 0;
		}
	}

	node_index = index;
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
				its_busy = bitarray_test_bit((node->bitmap), pos);
				if (!its_busy) {
					bitarray_set_bit((node->bitmap), pos);
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
	while ((writed_bytes < file_size) && (index < (required_blocks->elements_count))) {  // TODO: check condition
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
			if ((node->fd) < 0) {
				return DISCONNECTED_NODE;
			} else if (is_disconnected(node)) {
				node->fd = -1;
				return DISCONNECTED_NODE;
			} else {
				return node->fd;
			}
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

	int readed_bytes = 0;
	int bytes;
	int block = 0;
	int cpy;
	int datanode_fd;
	int keys_amount = config_keys_amount(md_file);
	int node_block;

	char * bytes_key = string_from_format("BLOQUE%dBYTES", block);
	char * cpy_key;
	char ** data;

	while (config_has_property(md_file, bytes_key)) {
		datanode_fd = DISCONNECTED_NODE;
		cpy = 0;
		while (datanode_fd == DISCONNECTED_NODE && cpy < keys_amount) {
			cpy_key = string_from_format("BLOQUE%dCOPIA%d", block, cpy);
			if (config_has_property(md_file, cpy_key)) {
				data = config_get_array_value(md_file, cpy_key);
				datanode_fd = get_datanode_fd(data[0]);
				node_block = atoi(data[1]);
				free(data[0]);
				free(data[1]);
				free(data);
			}
			free(cpy_key);
			cpy++;
		}

		if (datanode_fd == DISCONNECTED_NODE) {
			break;
		} else {
			bytes = config_get_int_value(md_file, bytes_key);
			dn_block = dn_get_block(datanode_fd, node_block, logger);
			fwrite((dn_block->buffer), bytes, 1, fs_file);
			readed_bytes += bytes;

			free(dn_block->buffer);
			free(dn_block);
		}

		free(bytes_key);
		block++;
		bytes_key = string_from_format("BLOQUE%dBYTES", block);

	}
	free(bytes_key);
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
	t_fs_directory * dir = (t_fs_directory *) directories_mf_ptr;
	dir += dir_index;
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
	fs_dir++;

	int index = 1;
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
	t_list * locked_index_list = list_create();
	list_add(locked_index_list, dest_dir_index);

	int dir_index = get_dir_index(path, LOCK_WRITE, locked_index_list);
	if (dir_index == ENOTDIR) {
		// is a file
		char * dir_c = string_duplicate(path);
		char * base_c = string_duplicate(path);
		char * yamafs_dir = dirname(dir_c);
		char * yamafs_file = basename(base_c);

		dir_index = get_dir_index(yamafs_dir, LOCK_WRITE, locked_index_list);
		if (dir_index == ENOTDIR) {
			rw_lock_unlock(directories_locks, UNLOCK, dest_dir_index);
			list_destroy(locked_index_list);
			free(base_c);
			free(dir_c);
			printf("error: file doesn't exist.\nplease try again...\n");
			return;
		}

		struct stat sb;
		char * old_md_file_path = string_from_format("%s/metadata/archivos/%d/%s", (fs_conf->mount_point), dir_index, yamafs_file);
		if ((stat(old_md_file_path, &sb) < 0) || (stat(old_md_file_path, &sb) == 0 && !(S_ISREG(sb.st_mode)))) {
			rw_lock_unlock(directories_locks, UNLOCK, dest_dir_index);
			rw_lock_unlock(directories_locks, UNLOCK, dir_index);
			list_destroy(locked_index_list);
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
			list_destroy(locked_index_list);
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
		list_destroy(locked_index_list);
		free(new_md_file_path);
		free(old_md_file_path);
		free(base_c);
		free(dir_c);
	} else {
		// is a directory
		t_fs_directory * dir = (t_fs_directory *) directories_mf_ptr;
		dir += dir_index;

		if (!dir_exists(dir_index, dest_dir_index, (dir->name))) {
			dir->parent_dir = dest_dir_index;
			printf("the directory was moved successfully!\n");
		} else {
			printf("error: directory already exists on target directory.\nplease try again...\n");
		}
		rw_lock_unlock(directories_locks, UNLOCK, dir_index);
		rw_lock_unlock(directories_locks, UNLOCK, dest_dir_index);
		list_destroy(locked_index_list);
	}
}




//	╔══════════════════════════════════════════════════════════════╗
//	║ COMMAND: LS                                                  ║
//	╚══════════════════════════════════════════════════════════════╝

/**
 * @NAME ls
 */
void ls(char * dir_path) {
	int dir_index = get_dir_index(dir_path, LOCK_READ, NULL);
	if (dir_index == ENOTDIR) {
		printf("error: directory not exists.\nplease try again...\n");
		return;
	}

	printf("drwxrwxr-x .\n");
	printf("drwxrwxr-x ..\n");

	char * yamafs_dir_path = string_from_format("%s/metadata/archivos/%d", (fs_conf->mount_point), dir_index);
	struct dirent * ent;
	DIR * yamafs_dir = opendir(yamafs_dir_path);
	while ((ent = readdir(yamafs_dir)) != NULL) {
		if ((strcmp(ent->d_name, ".") != 0) && (strcmp(ent->d_name, "..") != 0) && ((ent->d_type) != DT_DIR))
			printf("-rw-rw-r-- %s\n", (ent->d_name));
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
			printf("drwxrwxr-x %s\n", (fs_dir->name));
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
	fprintf(temp,"NODOS=%s\n", node_list_str);
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

	t_fs_node * loaded_node;
	index = 0;
	while (index < (nodes_list->elements_count)) {
		loaded_node = (t_fs_node *) list_get(nodes_list, index);
		pos = 0;
		while (pos < (loaded_node->size)) {
			bitarray_clean_bit((loaded_node->bitmap), pos);
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
		printf("error: file doesn't exist.\nplease try again...\n");
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

	int keys_amount = config_keys_amount(md_file_cfg);
	int cpy;
	int block = 0;

	char * bytes_key = string_from_format("BLOQUE%dBYTES", block);
	char * cpy_key;
	char ** data;

	t_list * release_list = list_create();

	pthread_mutex_lock(&nodes_table_m_lock);
	//
	// bitmap node update
	//
	while (config_has_property(md_file_cfg, bytes_key)) {
		cpy = 0;
		while (cpy < keys_amount) {
			cpy_key = string_from_format("BLOQUE%dCOPIA%d", block, cpy);
			if (config_has_property(md_file_cfg, cpy_key)) {
				data = config_get_array_value(md_file_cfg, cpy_key);
				add_to_release_list(release_list, data[0]);
				free_node_block(data[0], atoi(data[1]));
				free(data[0]);
				free(data[1]);
				free(data);
			}
			free(cpy_key);
			cpy++;
		}
		block++;
		bytes_key = string_from_format("BLOQUE%dBYTES", block);
	}
	free(bytes_key);
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

	int fs_free_size = 0;
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
	printf("the file was deleted successfully!\n");
}

/**
 * @NAME add_to_release_list
 */
void add_to_release_list(t_list * release_list, char * node) {
	t_fs_to_release * to_release;
	int index = 0;
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

/**
 * @NAME free_node_block
 */
void free_node_block(char * node, int block) {
	t_fs_node * loaded_node;
	int index = 0;
	while (index < (nodes_list->elements_count)) {
		loaded_node = (t_fs_node *) list_get(nodes_list, index);
		if (strcmp(node, (loaded_node->node_name)) == 0) {
			bitarray_clean_bit((loaded_node->bitmap), block);
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




//	╔══════════════════════════════════════════════════════════════╗
//	║ COMMAND: RM FILE BLOCK                                       ║
//	╚══════════════════════════════════════════════════════════════╝

/**
 * @NAME rm_file_block
 */
void rm_file_block(char * file_path, int file_block, int cpy_n) {

	char * dir_c = string_duplicate(file_path);
	char * base_c = string_duplicate(file_path);
	char * yamafs_dir = dirname(dir_c);
	char * yamafs_file = basename(base_c);

	int dir_index = get_dir_index(yamafs_dir, LOCK_WRITE, NULL);
	if (dir_index == ENOTDIR) {
		free(base_c);
		free(dir_c);
		printf("error: file doesn't exist.\nplease try again...\n");
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

	t_config * md_file = config_create(md_file_path);

	char * cpy_key = string_from_format("BLOQUE%dCOPIA%d", file_block, cpy_n);
	if (!config_has_property(md_file, cpy_key)) {
		config_destroy(md_file);
		rw_lock_unlock(directories_locks, UNLOCK, dir_index);
		free(cpy_key);
		free(md_file_path);
		free(base_c);
		free(dir_c);
		printf("error: file copy doesn't exist.\nplease try again...\n");
		return;
	}

	int cpy = 0;
	int keys_amount = config_keys_amount(md_file);
	free(cpy_key);
	cpy_key = string_from_format("BLOQUE%dCOPIA%d", file_block, cpy);
	int copies = 0;
	while (cpy < keys_amount) {
		if (config_has_property(md_file, cpy_key))
			copies++;
		cpy++;
		free(cpy_key);
		cpy_key = string_from_format("BLOQUE%dCOPIA%d", file_block, cpy);
	}
	free(cpy_key);

	if (copies == 1) {
		config_destroy(md_file);
		rw_lock_unlock(directories_locks, UNLOCK, dir_index);
		free(md_file_path);
		free(base_c);
		free(dir_c);
		printf("error: the selected block is the last copy.\nplease try again...\n");
		return;
	}

	//
	// metadata file update
	//
	char * md_temp_file_path = string_from_format("%s/metadata/archivos/%d/%s.temp", (fs_conf->mount_point), dir_index, yamafs_file);
	FILE * md_temp_file = fopen(md_temp_file_path, "w");

	int block = 0;
	char * bytes_key = string_from_format("BLOQUE%dBYTES", block);
	while (config_has_property(md_file, bytes_key)) {
		cpy = 0;
		while (cpy < keys_amount) {
			if (block == file_block && cpy == cpy_n) {
				cpy++;
				continue;
			}
			cpy_key = string_from_format("BLOQUE%dCOPIA%d", block, cpy);
			if (config_has_property(md_file, cpy_key)) {
				fprintf(md_temp_file,"%s=%s\n", cpy_key, config_get_string_value(md_file, cpy_key));
			}
			free(cpy_key);
			cpy++;
		}
		fprintf(md_temp_file,"%s=%d\n", bytes_key, config_get_int_value(md_file, bytes_key));
		free(bytes_key);
		block++;
		bytes_key = string_from_format("BLOQUE%dBYTES", block);
	}
	free(bytes_key);

	fprintf(md_temp_file,"TAMANIO=%d\n", config_get_int_value(md_file, "TAMANIO"));
	fprintf(md_temp_file,"TIPO=%s", config_get_string_value(md_file, "TIPO"));
	fclose(md_temp_file);

	char * to_remove_key = string_from_format("BLOQUE%dCOPIA%d", file_block, cpy_n);
	char ** data = config_get_array_value(md_file, to_remove_key);
	char * node = string_duplicate(data[0]);
	int node_block = atoi(data[1]);
	free(data[0]);
	free(data[1]);
	free(data);
	free(to_remove_key);

	config_destroy(md_file);
	remove(md_file_path);
	rename(md_temp_file_path, md_file_path);

	rw_lock_unlock(directories_locks, UNLOCK, dir_index);
	free(md_temp_file_path);
	free(md_file_path);
	free(base_c);
	free(dir_c);

	//
	// node table update
	//
	pthread_mutex_lock(&nodes_table_m_lock);
	free_node_block(node, node_block); // bitmap node update

	char * data_bin_path = string_from_format("%s/metadata/nodos.bin", (fs_conf->mount_point));
	char * temp_data_bin_path = string_from_format("%s/metadata/temp.bin", (fs_conf->mount_point));
	FILE * temp_data_bin = fopen(temp_data_bin_path, "w");

	char ** nodes = config_get_array_value(nodes_table, "NODOS");
	char * node_list_str = string_new();
	string_append_with_format(&node_list_str, "[%s", nodes[0]);
	int index = 1;
	while (nodes[index] != NULL) {
		string_append_with_format(&node_list_str, ",%s", nodes[index]);
		index++;
	}
	string_append(&node_list_str, "]");
	fprintf(temp_data_bin,"NODOS=%s\n", node_list_str);
	free(node_list_str);

	char * key;
	index = 0;
	while (nodes[index] != NULL) {
		key = string_from_format("%sTotal", nodes[index]);
		fprintf(temp_data_bin,"%s=%d\n", key, config_get_int_value(nodes_table, key));
		free(key);
		key = string_from_format("%sLibre", nodes[index]);
		fprintf(temp_data_bin,"%s=%d\n", key, (config_get_int_value(nodes_table, key) + ((strcmp(nodes[index], node) == 0) ? 1 : 0)));
		free(key);
		index++;
	}
	fprintf(temp_data_bin,"TAMANIO=%d\n", config_get_int_value(nodes_table, "TAMANIO"));
	fprintf(temp_data_bin,"LIBRE=%d", (config_get_int_value(nodes_table, "LIBRE") + 1));
	fclose(temp_data_bin);

	config_destroy(nodes_table);
	remove(data_bin_path);
	rename(temp_data_bin_path, data_bin_path);
	nodes_table = config_create(data_bin_path);

	pthread_mutex_unlock(&nodes_table_m_lock);

	index = 0;
	while (nodes[index] != NULL) {
		free(nodes[index]);
		index++;
	}
	free(nodes);
	free(node);
	free(temp_data_bin_path);
	free(data_bin_path);
	printf("the file block was deleted successfully!\n");
}




//	╔══════════════════════════════════════════════════════════════╗
//	║ COMMAND: RM DIR                                              ║
//	╚══════════════════════════════════════════════════════════════╝

/**
 * @NAME rm_dir
 */
void rm_dir(char * dir_path) {
	if (strcmp(dir_path, "/") == 0) {
		printf("error: root directory cannot be deleted.\nplease try again...\n");
		return;
	}
	int dir_index = get_dir_index(dir_path, LOCK_WRITE, NULL);
	if (dir_index == ENOTDIR) {
		printf("error: directory not exists.\nplease try again...\n");
		return;
	}

	char * yamafs_dir_path = string_from_format("%s/metadata/archivos/%d", (fs_conf->mount_point), dir_index);
	struct dirent * ent;
	DIR * yamafs_dir = opendir(yamafs_dir_path);
	while ((ent = readdir(yamafs_dir)) != NULL) {
		if ((strcmp(ent->d_name, ".") != 0) && (strcmp(ent->d_name, "..") != 0) && ((ent->d_type) == DT_REG)) {
			rw_lock_unlock(directories_locks, UNLOCK, dir_index);
			closedir(yamafs_dir);
			free(yamafs_dir_path);
			printf("error: non-empty directory.\nplease try again...\n");
			return;
		}
	}
	closedir(yamafs_dir);
	free(yamafs_dir_path);

	t_fs_directory * fs_dir = (t_fs_directory *) directories_mf_ptr;
	fs_dir++;
	int index = 1;
	while (index < DIRECTORIES_AMOUNT) {
		if (index == dir_index) {
			index++;
			fs_dir++;
			continue;
		}
		rw_lock_unlock(directories_locks, LOCK_READ, index);
		if ((fs_dir->parent_dir > 0) && (fs_dir->parent_dir == dir_index)) {
			rw_lock_unlock(directories_locks, UNLOCK, dir_index);
			rw_lock_unlock(directories_locks, UNLOCK, index);
			printf("error: non-empty directory.\nplease try again...\n");
			return;
		}
		rw_lock_unlock(directories_locks, UNLOCK, index);
		index++;
		fs_dir++;
	}

	fs_dir = (t_fs_directory *) directories_mf_ptr;
	fs_dir += dir_index;
	fs_dir->parent_dir = -1;
	strcpy(&(fs_dir->name),"\0");
	rw_lock_unlock(directories_locks, UNLOCK, dir_index);
	printf("the directory was deleted successfully!\n");
}




//	╔══════════════════════════════════════════════════════════════╗
//	║ COMMAND: CPY BLOCK                                           ║
//	╚══════════════════════════════════════════════════════════════╝

/**
 * @NAME cpblock
 */
void cpblock(char * file_path, int file_block, char * node) {

	pthread_mutex_lock(&nodes_table_m_lock);

	int dest_dn_fd = get_datanode_fd(node);
	if (dest_dn_fd == DISCONNECTED_NODE) {
		pthread_mutex_unlock(&nodes_table_m_lock);
		printf("error: disconnected destination node.\nplease try again...\n");
		return;
	}

	char * dir_c = string_duplicate(file_path);
	char * base_c = string_duplicate(file_path);
	char * yamafs_dir = dirname(dir_c);
	char * yamafs_file = basename(base_c);

	int dir_index = get_dir_index(yamafs_dir, LOCK_WRITE, NULL);
	if (dir_index == ENOTDIR) {
		pthread_mutex_unlock(&nodes_table_m_lock);
		free(base_c);
		free(dir_c);
		printf("error: file doesn't exist.\nplease try again...\n");
		return;
	}

	struct stat sb;
	char * md_file_path = string_from_format("%s/metadata/archivos/%d/%s", (fs_conf->mount_point), dir_index, yamafs_file);
	if ((stat(md_file_path, &sb) < 0) || (stat(md_file_path, &sb) == 0 && !(S_ISREG(sb.st_mode)))) {
		pthread_mutex_unlock(&nodes_table_m_lock);
		rw_lock_unlock(directories_locks, UNLOCK, dir_index);
		free(md_file_path);
		free(base_c);
		free(dir_c);
		printf("error: file doesn't exist.\nplease try again...\n");
		return;
	}

	t_config * md_file = config_create(md_file_path);

	int keys_amount = config_keys_amount(md_file);
	int cpy = 0;
	int last_cpy = -1;

	char * cpy_key;
	char ** data;
	int source_dn_fd = -1;
	int source_dn_block;


	while (cpy < keys_amount) {
		cpy_key = string_from_format("BLOQUE%dCOPIA%d", file_block, cpy);
		if (config_has_property(md_file, cpy_key)) {
			last_cpy = cpy;
			data = config_get_array_value(md_file, cpy_key);
			if (source_dn_fd < 0 || source_dn_fd == DISCONNECTED_NODE) {
				source_dn_fd = get_datanode_fd(data[0]);
				source_dn_block = atoi(data[1]);
			}
			free(data[0]);
			free(data[1]);
			free(data);
		}
		free(cpy_key);
		cpy++;
		cpy_key = string_from_format("BLOQUE%dCOPIA%d", file_block, cpy);
	}
	free(cpy_key);

	if (last_cpy < 0) {
		pthread_mutex_unlock(&nodes_table_m_lock);
		config_destroy(md_file);
		rw_lock_unlock(directories_locks, UNLOCK, dir_index);
		free(md_file_path);
		free(base_c);
		free(dir_c);
		printf("error: the selected block doesn't exist.\nplease try again...\n");
		return;
	}

	//
	// node table update
	//
	char * key = string_from_format("%sLibre", node);
	int free_blocks = config_get_int_value(nodes_table, key);

	if (free_blocks <= 0) {
		pthread_mutex_unlock(&nodes_table_m_lock);
		config_destroy(md_file);
		rw_lock_unlock(directories_locks, UNLOCK, dir_index);
		free(key);
		free(md_file_path);
		free(base_c);
		free(dir_c);
		printf("error: the selected node is full.\nplease try again...\n");
		return;
	}

	if (source_dn_fd == DISCONNECTED_NODE) {
		pthread_mutex_unlock(&nodes_table_m_lock);
		config_destroy(md_file);
		rw_lock_unlock(directories_locks, UNLOCK, dir_index);
		free(key);
		free(md_file_path);
		free(base_c);
		free(dir_c);
		printf("error: all source nodes are disconnected.\nplease try again...\n");
		return;
	}

	int assigned_block = assign_node_block(node);

	char * value_str = string_itoa((config_get_int_value(nodes_table, key)) - 1);
	config_set_value(nodes_table, key, value_str);
	free(value_str);
	free(key);
	config_save(nodes_table);

	value_str = string_itoa((config_get_int_value(nodes_table, "LIBRE")) - 1);
	config_set_value(nodes_table, "LIBRE", value_str);
	free(value_str);
	config_save(nodes_table);

	t_dn_get_block_resp * dn_block = dn_get_block(source_dn_fd, source_dn_block, logger);
	dn_set_block(dest_dn_fd, assigned_block, (dn_block->buffer), logger);
	free(dn_block->buffer);
	free(dn_block);

	pthread_mutex_unlock(&nodes_table_m_lock);


	//
	// metadata file update
	//
	char * md_temp_file_path = string_from_format("%s/metadata/archivos/%d/%s.temp", (fs_conf->mount_point), dir_index, yamafs_file);
	FILE * md_temp_file = fopen(md_temp_file_path, "w");

	int block = 0;
	char * bytes_key = string_from_format("BLOQUE%dBYTES", block);
	while (config_has_property(md_file, bytes_key)) {
		cpy = 0;
		while (cpy < keys_amount) {
			cpy_key = string_from_format("BLOQUE%dCOPIA%d", block, cpy);
			if (config_has_property(md_file, cpy_key)) {
				fprintf(md_temp_file,"%s=%s\n", cpy_key, config_get_string_value(md_file, cpy_key));
			}
			free(cpy_key);
			cpy++;
		}
		fprintf(md_temp_file,"%s=%d\n", bytes_key, config_get_int_value(md_file, bytes_key));
		block++;
		free(bytes_key);
		bytes_key = string_from_format("BLOQUE%dBYTES", block);
	}
	free(bytes_key);

	fprintf(md_temp_file,"BLOQUE%dCOPIA%d=[%s,%d]\n", file_block, (last_cpy + 1), node, assigned_block);
	fprintf(md_temp_file,"TAMANIO=%d\n", config_get_int_value(md_file, "TAMANIO"));
	fprintf(md_temp_file,"TIPO=%s", config_get_string_value(md_file, "TIPO"));
	fclose(md_temp_file);

	config_destroy(md_file);
	remove(md_file_path);
	rename(md_temp_file_path, md_file_path);

	rw_lock_unlock(directories_locks, UNLOCK, dir_index);
	free(md_file_path);
	free(md_temp_file_path);
	free(base_c);
	free(dir_c);
	printf("the block was copied successfully!\n");
}




//	╔══════════════════════════════════════════════════════════════╗
//	║ COMMAND: INFO                                                ║
//	╚══════════════════════════════════════════════════════════════╝

/**
 * @NAME info
 */
void info(char * file_path) {

	char * dir_c = string_duplicate(file_path);
	char * base_c = string_duplicate(file_path);
	char * yamafs_dir = dirname(dir_c);
	char * yamafs_file = basename(base_c);

	int dir_index = get_dir_index(yamafs_dir, LOCK_READ, NULL);
	if (dir_index == ENOTDIR) {
		free(base_c);
		free(dir_c);
		printf("error: file doesn't exist.\nplease try again...\n");
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

	t_config * md_file = config_create(md_file_path);
	free(md_file_path);

	printf("  file: %s\n", file_path);
	printf("  size: %d bytes\n", config_get_int_value(md_file, "TAMANIO"));
	printf("  type: %s\n", config_get_string_value(md_file, "TIPO"));

	int block = 0;
	int cpy;
	int keys_amount = config_keys_amount(md_file);

	char * bytes_key = string_from_format("BLOQUE%dBYTES", block);
	char * cpy_key;

	while (config_has_property(md_file, bytes_key)) {
		cpy = 0;
		while (cpy < keys_amount) {
			cpy_key = string_from_format("BLOQUE%dCOPIA%d", block, cpy);
			if (config_has_property(md_file, cpy_key)) {
				printf("  %s=%s\n", cpy_key, config_get_string_value(md_file, cpy_key));
			}
			free(cpy_key);
			cpy++;
		}
		free(bytes_key);
		block++;
		bytes_key = string_from_format("BLOQUE%dBYTES", block);
	}
	free(bytes_key);

	config_destroy(md_file);
	rw_lock_unlock(directories_locks, UNLOCK, dir_index);
	free(base_c);
	free(dir_c);
}




//	╔══════════════════════════════════════════════════════════════╗
//	║ COMMAND: CAT                                                 ║
//	╚══════════════════════════════════════════════════════════════╝

/**
 * @NAME cat
 */
void cat(char * file_path) {

	char * dir_c = string_duplicate(file_path);
	char * base_c = string_duplicate(file_path);
	char * yamafs_dir = dirname(dir_c);
	char * yamafs_file = basename(base_c);

	int dir_index = get_dir_index(yamafs_dir, LOCK_READ, NULL);
	if (dir_index == ENOTDIR) {
		free(base_c);
		free(dir_c);
		printf("error: file doesn't exist.\nplease try again...\n");
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

	t_config * md_file = config_create(md_file_path);
	free(md_file_path);


	pthread_mutex_lock(&nodes_table_m_lock);

	t_dn_get_block_resp * dn_block;

	int bytes;
	int readed_bytes = 0;
	int block = 0;
	int cpy;
	int datanode_fd;
	int keys_amount = config_keys_amount(md_file);
	int file_size = config_get_int_value(md_file, "TAMANIO");
	int node_block;
	int i;

	char * bytes_key = string_from_format("BLOQUE%dBYTES", block);
	char * cpy_key;
	char ** data;

	while (config_has_property(md_file, bytes_key)) {
		datanode_fd = DISCONNECTED_NODE;
		cpy = 0;
		while (datanode_fd == DISCONNECTED_NODE && cpy < keys_amount) {
			cpy_key = string_from_format("BLOQUE%dCOPIA%d", block, cpy);
			if (config_has_property(md_file, cpy_key)) {
				data = config_get_array_value(md_file, cpy_key);
				datanode_fd = get_datanode_fd(data[0]);
				node_block = atoi(data[1]);
				free(data[0]);
				free(data[1]);
				free(data);
			}
			free(cpy_key);
			cpy++;
		}

		if (datanode_fd == DISCONNECTED_NODE) {
			break;
		} else {
			bytes = config_get_int_value(md_file, bytes_key);
			dn_block = dn_get_block(datanode_fd, node_block, logger);
			readed_bytes += bytes;

			for (i = 0; i < bytes; i++) {
				printf("%c", *((char *) (dn_block->buffer + i)));
			}

			free(dn_block->buffer);
			free(dn_block);
		}

		free(bytes_key);
		block++;
		bytes_key = string_from_format("BLOQUE%dBYTES", block);
	}
	free(bytes_key);
	pthread_mutex_unlock(&nodes_table_m_lock);

	config_destroy(md_file);
	rw_lock_unlock(directories_locks, UNLOCK, dir_index);
	free(base_c);
	free(dir_c);

	if (readed_bytes != file_size) {
		printf("error: corrupted yamafs file.\n");
	}

}




//	╔══════════════════════════════════════════════════════════════╗
//	║ COMMAND: MD5                                                 ║
//	╚══════════════════════════════════════════════════════════════╝

/**
 * @NAME md5
 */
void md5(char * file_path) {

	char * dir_c = string_duplicate(file_path);
	char * base_c = string_duplicate(file_path);
	char * yamafs_dir = dirname(dir_c);
	char * yamafs_file = basename(base_c);

	int dir_index = get_dir_index(yamafs_dir, LOCK_WRITE, NULL);
	if (dir_index == ENOTDIR) {
		free(base_c);
		free(dir_c);
		printf("error: file doesn't exist.\nplease try again...\n");
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
	rw_lock_unlock(directories_locks, UNLOCK, dir_index);

	char * temp_path = string_from_format("%s/temp", (fs_conf->mount_point));
	char * temp_file_path = string_from_format("%s/%s", temp_path, yamafs_file);
	char * command = string_from_format("md5sum %s", temp_file_path);

	cpto(file_path, temp_path);
	system(command);
	remove(temp_file_path);

	free(command);
	free(temp_file_path);
	free(temp_path);
	free(md_file_path);
	free(base_c);
	free(dir_c);
}
















//	╔═════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
//	║                                                     CONSOLE                                                     ║
//	╚═════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝

void rm_wrapper(char ** args);
void rename_wrapper(char ** args);
void mv_wrapper(char ** args);
void mkdir_wrapper(char ** args);
void md5_wrapper(char ** args);
void ls_wrapper(char ** args);
void info_wrapper(char ** args);
void format_wrapper(char ** args);
void cpto_wrapper(char ** args);
void cpfrom_wrapper(char ** args);
void cpblock_wrapper(char ** args);
void cat_wrapper(char ** args);
t_command * find_command(char * line);
int execute_line(char * line);
char ** fileman_completion(char* line, int start, int end);

t_command comandos[] = {
		{"format", format_wrapper},
		{"rm", rm_wrapper},
		{"rename", rename_wrapper},
		{"mv", mv_wrapper},
		{"cat", cat_wrapper},
		{"mkdir", mkdir_wrapper},
		{"cpfrom", cpfrom_wrapper},
		{"cpto", cpto_wrapper},
		{"cpblock", cpblock_wrapper},
		{"md5", md5_wrapper},
		{"ls", ls_wrapper},
		{"info", info_wrapper},
		{(char *) NULL, (Function *) NULL}
};

/**
 * @NAME fs_console
 */
void fs_console(void * unused) {

	rl_attempted_completion_function = (CPPFunction *) fileman_completion;

	while (1) {
		char * line = readline("[user@yamafs ~]$:"); //TODO Ver como solucionar lo del EoF
		if (strcmp(line, "exit") == 0){
			free(line);
			break;
		}
		int a = execute_line(line);
		if (line) add_history(line);
		free(line);
	}
}

/**
 * @NAME execute_line
 */
int execute_line(char * line) {

	char * line_aux = string_duplicate(line);
	int i = 0;
	char * word;
	if(string_contains(line, " ")){
		while (line_aux[i] != ' ') i++;
		word = malloc(sizeof(char) * i);
		strncpy(word, line_aux, i);
		word[i] = '\0';
	} else word = string_duplicate(line_aux);


	t_command * command = find_command (word);

	i++;
	char ** args = string_split(line_aux + i, " ");
	if (!command) {
		fprintf (stderr, "%s: No such command for YAMA FileSystem Console.\n", word);
		return (-1);
	}
	free(word);
	free(line_aux);
	/* Call the function. */
	(*(command->funcion)) (args);

	return 1;
}

/**
 * @NAME find_command
 */
t_command * find_command(char * line) {
	register int i;

	for (i = 0; comandos[i].name; i++)
		if (strcmp (line, comandos[i].name) == 0)
			return (&comandos[i]);

	return ((t_command *) NULL);
}

/**
 * @NAME command_generator
 */
char * command_generator(char * text, int state) {
	static int list_index, len;
	char * name;

	/* If this is a new word to complete, initialize now.  This includes
	   saving the length of TEXT for efficiency, and initializing the index
	   variable to 0. */
	if (!state) {
		list_index = 0;
		len = strlen (text);
	}

	/* Return the next name which partially matches from the command list. */
	while (name = comandos[list_index].name) {
		list_index++;
		if (strncmp (name, text, len) == 0)
			return (string_duplicate(name));
	}

	/* If no names matched, then return NULL. */
	return ((char *) NULL);
}

/**
 * @NAME fileman_completion
 */
char ** fileman_completion(char * line, int start, int end) {
	char ** matches;

	matches = (char **) NULL;

	/* If this word is at the start of the line, then it is a command
	   to complete.  Otherwise it is the name of a file in the current
	   directory. */
	if (start == 0)
		matches = completion_matches(line, command_generator);

	return (matches);
}

/**
 * @NAME format_wrapper
 */
void format_wrapper(char ** args) {
	format();
}

/**
 * @NAME rm_wrapper
 */
void rm_wrapper(char ** args){
	if (args[0] == NULL) {
		printf("error: illegal arguments.\nplease try again...\n");
		return;
	}
	if (strcmp(args[0], "-d") == 0) {
		if (args[1] == NULL) {
			printf("error: illegal arguments.\nplease try again...\n");
			return;
		}
		rm_dir(args[1]);
	} else if (strcmp(args[0], "-b") == 0) {
		if (args[1] == NULL || args[2] == NULL || !is_number(args[2]) || args[3] == NULL || !is_number(args[3])) {
			printf("error: illegal arguments.\nplease try again...\n");
			return;
		}
		rm_file_block(args[1], atoi(args[2]), atoi(args[3]));
	} else {
		rm_file(args[0]);
	}
}

/**
 * @NAME rename_wrapper
 */
void rename_wrapper(char ** args) {
	if (args[0] == NULL || args[1] == NULL) {
		printf("error: illegal arguments.\nplease try again...\n");
		return;
	}
	fs_rename(args[0], args[1]);
}

/**
 * @NAME mv_wrapper
 */
void mv_wrapper(char ** args) {
	if (args[0] == NULL || args[1] == NULL) {
		printf("error: illegal arguments.\nplease try again...\n");
		return;
	}
	move(args[0], args[1]);
}

/**
 * @NAME cat_wrapper
 */
void cat_wrapper(char ** args) {
	if (args[0] == NULL) {
		printf("error: illegal arguments.\nplease try again...\n");
		return;
	}
	cat(args[0]);
}

/**
 * @NAME mkdir_wrapper
 */
void mkdir_wrapper(char ** args) {
	if (args[0] == NULL) {
		printf("error: illegal arguments.\nplease try again...\n");
		return;
	}
	fs_make_dir(args[0]);
}

/**
 * @NAME cpfrom_wrapper
 */
void cpfrom_wrapper(char ** args) {
	if (args[0] == NULL || args[1] == NULL || args[2] == NULL) {
		printf("error: illegal arguments.\nplease try again...\n");
		return;
	}
	if ((strcmp(args[2], "t") == 0) || (strcmp(args[2], "b") == 0)) {
		cpfrom(args[0], args[1], ((strcmp(args[2], "t") == 0) ? TEXT : BINARY));
	} else {
		printf("error: unsupported file type.\nplease try again...\n");
	}
}

/**
 * @NAME cpto_wrapper
 */
void cpto_wrapper(char ** args) {
	if (args[0] == NULL || args[1] == NULL) {
		printf("error: illegal arguments.\nplease try again...\n");
		return;
	}
	cpto(args[0], args[1]);
}

/**
 * @NAME cpblock_wrapper
 */
void cpblock_wrapper(char ** args) {
	if (args[0] == NULL || args[1] == NULL || !is_number(args[1]) || args[2] == NULL) {
		printf("error: illegal arguments.\nplease try again...\n");
		return;
	}
	cpblock(args[0], atoi(args[1]), args[2]);
}

/**
 * @NAME ls_wrapper
 */
void ls_wrapper(char ** args) {
	if (args[0] == NULL) {
		printf("error: illegal arguments.\nplease try again...\n");
		return;
	}
	ls(args[0]);
}

/**
 * @NAME info_wrapper
 */
void info_wrapper(char ** args) {
	if (args[0] == NULL) {
		printf("error: illegal arguments.\nplease try again...\n");
		return;
	}
	info(args[0]);
}

/**
 * @NAME info_wrapper
 */
void md5_wrapper(char ** args) {
	if (args[0] == NULL) {
		printf("error: illegal arguments.\nplease try again...\n");
		return;
	}
	md5(args[0]);
}

/**
 * @NAME is_number
 */
bool is_number(char * str) {
	int j = 0;
	while (j < strlen(str)) {
		if(str[j] > '9' || str[j] < '0') {
			return false;
		}
		j++;
	}
	return true;
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
