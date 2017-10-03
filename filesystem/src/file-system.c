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
void load_fs_properties(void);
void init_locks(void);
void init(bool);
void get_metadata_file(int *);
void fs_console(void *);
void create_logger(void);
void create_bitmap_for_node(char *, int);
void closure(void *);
void clean_dir(char *);
void check_fs_status(void);
void add_node(t_list *, char *, int);
int uploading_file(t_fs_upload_file_req * req);
int rw_lock_unlock(pthread_rwlock_t *, int, int);
int load_bitmap_node(int *, bool, char *, int);
int get_line_length(char *, int, int);
int connect_node(int *, char *, int);
t_list * calc_required_blocks_for_txt_file(char *, int);
t_list * calc_required_blocks_for_binary_file(int file_size);
int assign_node_block(char *);
int assign_blocks_to_file(t_config **, int, char *, char, int, t_list *);
int get_datanode_fd(char *);
char * assign_node(char *);
void cpto(char *, char *);
void cpfrom(char *, char *, char);
int get_dir_index(char *, int);
int get_dir_index_from_table(char *, int, int);
int cpy_to_local_dir(char *, char *, char *);

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
		if (strcmp(node_name, nodes[pos]) == 0) {
			exits = true;
			break;
		}
		pos++;
	}
	if (!exits) {
		add_node(nodes_table_list, node_name, blocks);
		create_bitmap_for_node(node_name, blocks);
	}
	int exec = load_bitmap_node(client_socket, !exits, node_name, blocks);
	if (exec == SUCCESS)
		check_fs_status();
	pthread_mutex_unlock(&nodes_table_m_lock);

	list_destroy_and_destroy_elements(nodes_table_list, &closure);
	pos = 0;
	while (nodes[pos] != NULL) {
		free(nodes[pos]);
		pos++;
	}
	free(nodes);
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

	bool empty_dir;
	bool all_nodes_connected;
	bool located_node;

	int file_size;
	int file_blocks;
	int block;
	int pos;

	char * dir_path;
	char * md_file_path;
	char * key;
	char ** data;

	t_config * md_file;
	t_fs_node * node;
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
					file_size = config_get_int_value(md_file, "TAMANIO");
					file_blocks = (file_size / BLOCK_SIZE) + (((file_size % BLOCK_SIZE) > 0) ? 1 : 0);
					block = 0;
					while (block < file_blocks) {
						key = string_from_format("BLOQUE%dCOPIA0", block);
						data = config_get_array_value(md_file, key);
						located_node = false;
						pos = 0;
						while (pos < (nodes_list->elements_count)) {
							node = (t_fs_node *) list_get(nodes_list, pos);
							if (strcmp(data[0], (node->node_name)) == 0) {
								located_node = true;
								break;
							};
							pos++;
						}
						free(key);
						free(data[0]);
						free(data[1]);
						free(data);
						if (!located_node) {
							all_nodes_connected = false;
							break;
						}
						block++;
					}
					config_destroy(md_file);
					free(md_file_path);
				}
				if (!empty_dir && !all_nodes_connected) break;
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
	if (nodes_list->elements_count > 1)
		status = STEADY; // at least two connected nodes
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
 * @NAME get_metadata_file
 */
void get_metadata_file(int * client_socket) {
	t_fs_get_md_file_req * req = fs_get_metadata_file_recv_req(client_socket, logger);
	//
	// TODO
	//

	// MOCK
	t_fs_metadata_file * md_file = (t_fs_metadata_file *) malloc(sizeof(t_fs_metadata_file));
	md_file->path = (req->path);
	md_file->file_size = 6144;
	md_file->type = 'b';
	md_file->block_list = list_create();

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
		list_add((md_file->block_list), block_md);
	}

	fs_get_metadata_file_send_resp(client_socket, SUCCESS, md_file);

	list_destroy_and_destroy_elements(md_file->block_list, &closure);
	free(md_file);
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
 * @NAME fs_console
 */
void fs_console(void * unused) { // TODO
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
//			char * token = strtok(input, " ");
//			if (token != NULL) command = token;
//			token = strtok(NULL, " ");
//			if (token != NULL) param01 = token;
//			token = strtok(NULL, " ");
//			if (token != NULL) param02 = token;
//			token = strtok(NULL, " ");
//			if (token != NULL) param03 = token;

			cpfrom("/home/utnso/MOCK_DATA.csv", "/", TEXT);
			cpto("/MOCK_DATA.csv", "/home/utnso/cpto");

		}
	}
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
		printf("file wasn't uploaded successfully: file doesn't exist\nplease try again...\n");
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
		printf("file wasn't uploaded successfully: yamafs dir not exists\nplease try again...\n");
		break;
	case EEXIST:
		printf("file wasn't uploaded successfully: file already exists in yamafs\nplease try again...\n");
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

	int dir_index = get_dir_index(dir, LOCK_WRITE);
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

	t_list * required_blocks;
	if ((req->type) == TEXT) {
		required_blocks = calc_required_blocks_for_txt_file((req->buffer),(req->file_size));
	} else {
		required_blocks = calc_required_blocks_for_binary_file((req->file_size));
	}

	t_config * file_md;
	pthread_mutex_lock(&nodes_table_m_lock);
	int exec = assign_blocks_to_file(&file_md, dir_index, file, (req->type), (req->file_size), required_blocks);
	if (exec == SUCCESS) {
		write_file((req->buffer), file_md, (req->file_size), required_blocks);
	} else {
		char * file_md_path = string_duplicate(file_md->path);
		config_destroy(file_md);
		remove(file_md_path); // deleting metadata file
	}
	pthread_mutex_unlock(&nodes_table_m_lock);

	list_destroy_and_destroy_elements(required_blocks, &closure);
	rw_lock_unlock(directories_locks, UNLOCK, dir_index);
	free(md_file_path);
	free(base_c);
	free(dir_c);
	return exec;
}

/**
 * @NAME get_dir_index
 */
int get_dir_index(char * path, int lock_type) {
	if (strcmp(path, "/") == 0) {
		rw_lock_unlock(directories_locks, lock_type, ROOT);
		return ROOT;
	}
	int dir_index;
	int pd_index = ROOT;
	char * path_c = string_duplicate(path);
	char * dir = strtok(path_c, "/");
	while (dir != NULL) {
		dir_index = get_dir_index_from_table(dir, pd_index, lock_type);
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
int get_dir_index_from_table(char * dir, int parent_index, int lock_type) {
	t_fs_directory * fs_dir = (t_fs_directory *) directories_mf_ptr;
	int index = 0;
	while (index < DIRECTORIES_AMOUNT) {
		if (index == parent_index)
			continue;
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
	while ((writed_bytes <= file_size) && (index < (required_blocks->elements_count))) {
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
	char ** values = config_get_array_value(file, key);
	dn_set_block(get_datanode_fd(values[0]), atoi(values[1]), block, logger);
	free(values[0]);
	free(values[1]);
	free(values);
	free(key);

	key = string_from_format("BLOQUE%dCOPIA1", block_number);
	values = config_get_array_value(file, key);
	dn_set_block(get_datanode_fd(values[0]), atoi(values[1]), block, logger);
	free(values[0]);
	free(values[1]);
	free(values);
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
	return ERROR;
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
    printf("file wasn't copied successfully: local directory doesn't exist\nplease try again...\n");
    return;
  }

  char * dir_c = string_duplicate(yamafs_file_path);
  char * base_c = string_duplicate(yamafs_file_path);
  char * yamafs_dir = dirname(dir_c);
  char * yamafs_file = basename(base_c);

  int dir_index = get_dir_index(yamafs_dir, LOCK_READ);
  if (dir_index == ENOTDIR) {
    free(dir_c);
    free(base_c);
    printf("file wasn't copied successfully: file doesn't exist\nplease try again...\n");
    return;
  }

  char * md_file_path = string_from_format("%s/metadata/archivos/%d/%s", (fs_conf->mount_point), dir_index, yamafs_file);
  if ((stat(md_file_path, &sb) < 0) || (stat(md_file_path, &sb) == 0 && !(S_ISREG(sb.st_mode)))) {
    rw_lock_unlock(directories_locks, UNLOCK, dir_index);
    free(md_file_path);
    free(dir_c);
    free(base_c);
    printf("file wasn't copied successfully: file doesn't exist\nplease try again...\n");
    return;
  }

  switch (cpy_to_local_dir(md_file_path, local_dir, yamafs_file)) {
	case SUCCESS:
		printf("file copied successfully!\n");
		break;
	case CORRUPTED_FILE:
		printf("corrupted file\n");
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

  char * cpy0_key;
  char * cpy1_key;
  char * bytes_key;
  char ** values;
  int datanode_fd;
  int bytes;
  t_dn_get_block_resp * dn_block;

  pthread_mutex_lock(&nodes_table_m_lock);
  int readed_bytes = 0;
  int block = 0;
  while (readed_bytes < file_size) {

    cpy0_key = string_from_format("BLOQUE%dCOPIA0", block);
    cpy1_key = string_from_format("BLOQUE%dCOPIA1", block);
    bytes_key = string_from_format("BLOQUE%dBYTES", block);

    if (config_has_property(md_file, cpy0_key)) {
      values = config_get_array_value(md_file, cpy0_key);
    } else if (config_has_property(md_file, cpy1_key)) {
      values = config_get_array_value(md_file, cpy1_key);
    } else {
      free(bytes_key);
      free(cpy1_key);
      free(cpy0_key);
      break;
    }

    bytes = config_get_int_value(md_file, bytes_key);
    datanode_fd = get_datanode_fd(values[0]);
    dn_block = dn_get_block(datanode_fd, atoi(values[1]), logger);
    fwrite((dn_block->buffer), bytes, 1, fs_file);
    readed_bytes += bytes;

    free(dn_block->buffer);
    free(dn_block);
    free(values[0]);
    free(values[1]);
    free(values);
    free(bytes_key);
    free(cpy1_key);
    free(cpy0_key);
    block++;
  }
  pthread_mutex_unlock(&nodes_table_m_lock);

  fclose(fs_file);
  config_destroy(md_file);
  if (readed_bytes < file_size) {
    remove(local_file_path);
    free(local_file_path);
    return CORRUPTED_FILE;
  }
  free(local_file_path);
  return SUCCESS;
}
















//
// TODO: ¿Que sucede al desconectar un nodo?
//

//
// TODO: al crear un directorio se debe crear el directorio para los archivos del directorio (ejemplo: metadata/archivos/5/ejemplo.csv)
//

//
// Implementación:
//
// Si el FS que se levanta, corresponde a un estado anterior, se debe verificar que el nombre de los archivos bitmap
// sean igual al configurado como nombre del nodo en Data-Node


//
// http://www.chuidiang.org/clinux/sockets/socketselect.php
// http://www.delorie.com/djgpp/doc/libc/libc_646.html
// https://www.lemoda.net/c/recursive-directory/

void fs_read(char *);
int get_md_file(t_config *, char *);

void fs_read(char * file_path) {
	t_config * md;
	int exec = get_md_file(&md, file_path);



	config_destroy(md);
}

int get_md_file(t_config * md, char * file_path) {
	struct stat sb;

	char * dir_c = string_duplicate(file_path);
	char * base_c = string_duplicate(file_path);

	char * dir = dirname(dir_c);
	char * file = basename(base_c);

	int dir_index = get_dir_index(dir, LOCK_WRITE);
	if (dir_index == ENOTDIR) {
		free(base_c);
		free(dir_c);
		return ENOTDIR;
	}

	char * md_file_path = string_from_format("%s/metadata/archivos/%d/%s", (fs_conf->mount_point), dir_index, file);
	if ((stat(md_file_path, &sb) < 0) || (stat(md_file_path, &sb) == 0 && !(S_ISREG(sb.st_mode)))) {
		rw_lock_unlock(directories_locks, UNLOCK, dir_index);
		free(md_file_path);
		free(base_c);
		free(dir_c);
		return ENOENT;
	}

	md = config_create(md_file_path);

	free(md_file_path);
	free(dir_c);
	free(base_c);
	return SUCCESS;
}


/**
 * @NAME free_assigned_blocks
 */
void free_assigned_blocks(t_config * file, int used_blocks) {
	pthread_mutex_lock(&nodes_table_m_lock);
	char * key;
	char * value_str;
	char ** data;
	char * node;
	int used_b;

	int block = 0;
	while (block < used_blocks) {

		key = string_from_format("BLOQUE%dCOPIA0", block);
		data = config_get_array_value(file, key);
		free(key);
		node = data[0];
		used_b = atoi(data[1]);
		free_node_block(node, used_b);
		key = string_from_format("%sLibre", node);
		value_str = string_itoa(((config_get_int_value(nodes_table, key)) + 1));
		config_set_value(nodes_table, key, value_str);
		free(value_str);
		free(key);
		free(data[0]);
		free(data[1]);
		free(data);

		key = string_from_format("BLOQUE%dCOPIA1", block);
		data = config_get_array_value(file, key);
		free(key);
		node = data[0];
		used_b = atoi(data[1]);
		free_node_block(node, used_b);
		key = string_from_format("%sLibre", node);
		value_str = string_itoa(((config_get_int_value(nodes_table, key)) + 1));
		config_set_value(nodes_table, key, value_str);
		free(value_str);
		free(key);
		free(data[0]);
		free(data[1]);
		free(data);

		value_str = string_itoa((config_get_int_value(nodes_table, "LIBRE")) + 2);
		config_set_value(nodes_table, "LIBRE", value_str);
		free(value_str);

		block++;
	}

	config_save(nodes_table);
	pthread_mutex_unlock(&nodes_table_m_lock);
}

/**
 * @NAME free_node_block
 */
void free_node_block(char * node_name, int block) {
	t_fs_node * node;
	int index = 0;
	while (index < (nodes_list->elements_count)) {
		node = (t_fs_node *) list_get(nodes_list, index);
		if (strcmp((node->node_name), node_name) == 0) {
			bitarray_clean_bit(node->bitmap, block);
			return;
		}
		index++;
	}
}
