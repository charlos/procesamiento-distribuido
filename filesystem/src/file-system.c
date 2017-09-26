#include <commons/log.h>
#include <commons/config.h>
#include <pthread.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <stdarg.h>
#include <string.h>
#include <commons/config.h>
#include <commons/bitarray.h>
#include <fcntl.h>
#include <pthread.h>
#include <errno.h>
#include <shared-library/file-system-prot.h>
#include <shared-library/data-node-prot.h>
#include <shared-library/socket.h>
#include <dirent.h>
#include "file-system.h"

#define	SOCKET_BACKLOG 			100
#define	DIRECTORIES_AMOUNT  	100
#define	LOCK_READ 				0
#define	LOCK_WRITE 				1
#define	UNLOCK 					2
#define ROOT					0
#define STEADY 					's'
#define UNSTEADY				'u'

int listenning_socket;
t_fs_conf * fs_conf;
t_log * logger;

char status = UNSTEADY; 	// FS state
void * directories_mf_ptr; 	// directories mapped file ptr
t_config * nodes_table; 	// nodes table
t_list * nodes_list;		// nodes list

pthread_mutex_t nodes_table_m_lock;
pthread_mutex_t nodes_list_m_lock;
pthread_mutex_t state_m_lock;
pthread_rwlock_t * directories_locks;

void fs_console(void *);
void process_request(int *);
void handshake(int *);
void load_file(int *);
void read_file(int *);
void get_file_metadata(int *);
void load_fs_properties(void);
void create_logger(void);
void init(void);
void init_locks(void);
void closure(void *);
void connect_node(int *, char *, int);
void add_node(t_list *, char *, int);
void create_bitmap_for_node(char *, int);
void load_bitmap_node(int *, bool, char *, int);
void * map_file(char *);
void check_fs_status(void);

void create_metadata_file(char *, int, int, char);
int assign_blocks_to_file(t_config *, int);
char * assign_node(char *);
int assign_node_block(char *);
int rw_lock_unlock(pthread_rwlock_t *, int, int);

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
void init(void) {

	struct stat sb;

	// metadata directory
	char * metadata_path = string_from_format("%s/metadata", (fs_conf->mount_point));
	if ((stat(metadata_path, &sb) < 0) || (stat(metadata_path, &sb) == 0 && !(S_ISDIR(sb.st_mode))))
		mkdir(metadata_path, S_IRWXU | S_IRWXG | S_IRWXO);

	// files directory
	char * files_path = string_from_format("%s/archivos", metadata_path);
	if ((stat(files_path, &sb) < 0) || (stat(files_path, &sb) == 0 && !(S_ISDIR(sb.st_mode))))
		mkdir(files_path, S_IRWXU | S_IRWXG | S_IRWXO);

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
		int i = 1;
		while (i <= 99) {
			dir->index = i;
			fwrite(dir, sizeof(t_fs_directory), 1, dir_file);
			i++;
		}
		free(dir);
		fclose(dir_file);
	}

	directories_mf_ptr = map_file(directories_file_path);

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
	free(files_path);
	free(metadata_path);
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
	pthread_mutex_init(&nodes_list_m_lock, NULL);
	pthread_mutex_init(&state_m_lock, NULL);
}

/**
 * @NAME map_file
 */
void * map_file(char * file_path) {
	struct stat sb;
	size_t size;
	int fd; // file descriptor
	int status;

	fd = open(file_path, O_RDWR);
	check(fd < 0, "open %s failed: %s", file_path, strerror(errno));

	status = fstat(fd, &sb);
	check(status < 0, "stat %s failed: %s", file_path, strerror (errno));
	size = sb.st_size;

	void * mapped_file_ptr = mmap((caddr_t) 0, size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
	check((mapped_file_ptr == MAP_FAILED), "mmap %s failed: %s", file_path, strerror (errno));

	return mapped_file_ptr;
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
	switch (req->type) {
	case 'd':
		// data-node
		connect_node(client_socket, req->node_name, req->blocks);
		free(req->node_name);
		break;
	case 'y':
		// yama
		//
		// TODO
		//
		fs_handshake_send_resp(client_socket, SUCCESS);
		break;
	case 'w':
		//worker
		//
		// TODO
		//
		fs_handshake_send_resp(client_socket, SUCCESS);
		break;
	default:;
	}
	free(req);
}

/**
 * @NAME connect_node
 */
void connect_node(int * client_socket, char * node_name, int blocks) {
	pthread_mutex_lock(&nodes_table_m_lock);
	char ** nodes = config_get_array_value(nodes_table, "NODOS");
	t_list * nodes_table_list = list_create();
	bool exits = false;
	int pos = 0;
	char * node;
	while (nodes[pos] != NULL) {
		node = nodes[pos];
		list_add(nodes_table_list, node);
		if (strcmp(node_name, node) == 0) {
			exits = true;
			break;
		}
		pos++;
	}
	if (!exits) {
		add_node(nodes_table_list, node_name, blocks);
		create_bitmap_for_node(node_name, blocks);
	}
	pthread_mutex_unlock(&nodes_table_m_lock);

	load_bitmap_node(client_socket, !exits, node_name, blocks);
	check_fs_status();

	list_destroy_and_destroy_elements(nodes_table_list, &closure);
	pos = 0;
	while (nodes[pos] != NULL) {
		free(nodes[pos]);
		pos++;
	}
	free(nodes);
	fs_handshake_send_resp(client_socket, SUCCESS);
}

/**
 * @NAME add_node
 */
void add_node(t_list * nodes_table_list, char * new_node_name, int blocks) {

	int new_value = (config_get_int_value(nodes_table, "TAMANIO")) + blocks;
	char * new_value_str = string_itoa(new_value);
	config_set_value(nodes_table, "TAMANIO", new_value_str);
	free(new_value_str);

	new_value = (config_get_int_value(nodes_table, "LIBRE")) + blocks;
	new_value_str = string_itoa(new_value);
	config_set_value(nodes_table, "LIBRE", new_value_str);
	free(new_value_str);

	char * new_node_blocks = string_from_format("%sTotal", new_node_name);
	char * new_node_free_blocks = string_from_format("%sLibre", new_node_name);
	char * node_list_str;

	if (nodes_table_list->elements_count > 0) {
		node_list_str = string_from_format("[%s", (char *) (list_get(nodes_table_list, 0)));
		char * aux;
		int pos = 1;
		while (pos < (nodes_table_list->elements_count)) {
			aux = string_from_format("%s,%s", node_list_str, (char *) (list_get(nodes_table_list, pos)));
			free(node_list_str);
			node_list_str = aux;
			pos++;
		}
		aux = string_from_format("%s,%s]", node_list_str, new_node_name);
		free(node_list_str);
		node_list_str = aux;
		config_set_value(nodes_table, "NODOS", node_list_str);
		free(node_list_str);
	} else {
		node_list_str = string_from_format("[%s]", new_node_name);
		config_set_value(nodes_table, "NODOS", node_list_str);
		free(node_list_str);
	}

	new_value_str = string_itoa(blocks);
	dictionary_put(nodes_table->properties, new_node_blocks, (void *) new_value_str);
	dictionary_put(nodes_table->properties, new_node_free_blocks, (void *) new_value_str);
	config_save(nodes_table);
	free(new_value_str);
	free(new_node_free_blocks);
	free(new_node_blocks);
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
void load_bitmap_node(int * node_fd, bool clean_bitmap, char * node_name, int blocks) {
	pthread_mutex_lock(&nodes_list_m_lock);
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
	if (!loaded) {
		char * bitmap_file_path = string_from_format("%s/metadata/bitmaps/%s.bin", (fs_conf->mount_point), node_name);
		t_bitarray * bitmap = bitarray_create_with_mode(map_file(bitmap_file_path), blocks, MSB_FIRST);
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
		node->fd = node_fd;
		list_add(nodes_list, node);
		free(bitmap_file_path);
	}
	pthread_mutex_unlock(&nodes_list_m_lock);
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
	char * key;
	char ** data;
	t_config * metadata_file;
	t_fs_node * node;
	DIR * dir;
	struct dirent * ent;

	t_fs_directory * fs_dir = (t_fs_directory *) directories_mf_ptr;
	fs_dir++;
	int index = 1;
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
					metadata_file = config_create(ent->d_name);
					file_size = config_get_int_value(metadata_file, "TAMANIO");
					file_blocks = (file_size / BLOCK_SIZE) + (((file_size % BLOCK_SIZE) > 0) ? 1 : 0);
					block = 0;
					while (block < file_blocks) {
						key = string_from_format("BLOQUE%dCOPIA0", block);
						data = config_get_array_value(metadata_file, key);
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
					free(metadata_file);
				}
				if (!empty_dir && !all_nodes_connected) break;
			}
			closedir(dir);
			free(dir_path);
		}
		rw_lock_unlock(directories_locks, UNLOCK, index);

		if (!empty_dir && !all_nodes_connected) {
			pthread_mutex_lock(&state_m_lock);
			status = UNSTEADY;
			pthread_mutex_unlock(&state_m_lock);
			return;
		}

		index++;
		fs_dir++;

	}
	pthread_mutex_lock(&state_m_lock);
	status = STEADY;
	pthread_mutex_unlock(&state_m_lock);
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

void closure(void * node) {
	free(node);
}

























/**
 * @NAME create_metadata_file
 */
void create_metadata_file(char * file_name, int file_size, int index, char type) {
	// TODO: check if direct exists

	struct stat sb;
	// index file directory
	char * index_path = string_from_format("%s/archivos/%d", (fs_conf->mount_point), index);
	if ((stat(index_path, &sb) < 0) || (stat(index_path, &sb) == 0 && !(S_ISDIR(sb.st_mode))))
		mkdir(index_path, S_IRWXU | S_IRWXG | S_IRWXO);

	// file
	char * file_path = string_from_format("%s/%s", index_path, file_name);
	FILE * nodes_file = fopen(file_path, "w");
	fprintf(nodes_file,"TAMANIO=%d\n", file_size);
	fprintf(nodes_file,"TIPO=%s\n", ((type == 'b') ? "BINARIO" : "TEXTO"));
	fclose(nodes_file);

	t_config * file = config_create(file_path);
	int exec = assign_blocks_to_file(file, file_size);

	free(file);
	free(file_path);
	free(index_path);
}

/**
 * @NAME assign_blocks_to_file
 */
int assign_blocks_to_file(t_config * file, int file_size) {
	pthread_mutex_lock(&nodes_table_m_lock);
	int bytes_to_assign = file_size;
	char * orig;
	char * cpy;
	while (bytes_to_assign > 0) {
		orig = assign_node(NULL);
		if (!orig) {
			pthread_mutex_unlock(&nodes_table_m_lock);
			return ENOSPC;
		}
		cpy = assign_node(orig);
		if (!cpy) {
			free(orig);
			pthread_mutex_unlock(&nodes_table_m_lock);
			return ENOSPC;
		}
		free(orig);
		free(cpy);
		bytes_to_assign -= BLOCK_SIZE;
	}
	pthread_mutex_unlock(&nodes_table_m_lock);

	bytes_to_assign = file_size;
	char * key;
	char * value_str;
	int assigned_block;
	int block = 0;

	while (bytes_to_assign > 0) {

		pthread_mutex_lock(&nodes_table_m_lock);
		orig = assign_node(NULL);
		cpy = assign_node(orig);

		key = string_from_format("%sLibre", orig);
		value_str = string_itoa((config_get_int_value(nodes_table, key)) - 1);
		config_set_value(nodes_table, key, value_str);
		free(value_str);
		free(key);

		key = string_from_format("%sLibre", cpy);
		value_str = string_itoa((config_get_int_value(nodes_table, key)) - 1);
		config_set_value(nodes_table, key, value_str);
		free(value_str);
		free(key);

		value_str = string_itoa((config_get_int_value(nodes_table, "LIBRE")) - 2);
		config_set_value(nodes_table, "LIBRE", value_str);
		free(value_str);

		config_save(nodes_table);
		pthread_mutex_unlock(&nodes_table_m_lock);

		assigned_block = assign_node_block(orig);
		key = string_from_format("BLOQUE%dCOPIA0", block);
		value_str = string_from_format("[%s, %d]\n", cpy, assigned_block);
		dictionary_put(file->properties, key, (void *) value_str);
		free(value_str);
		free(key);

		assigned_block = assign_node_block(cpy);
		free(key);
		key = string_from_format("BLOQUE%dCOPIA1", block);
		value_str = string_from_format("[%s, %d]\n", cpy, assigned_block);
		dictionary_put(file->properties, key, (void *) value_str);
		free(value_str);
		free(key);

		config_save(file);

		free(cpy);
		free(orig);
		bytes_to_assign -= BLOCK_SIZE;
		block++;
	}
	return SUCCESS;
}

/**
 * @NAME assign_node
 */
char * assign_node(char * unwanted_node) {
	pthread_mutex_lock(&nodes_list_m_lock);
	char * selected_node;
	char * key;
	int max = -1;
	int free_blocks;
	int index = 0;
	t_fs_node * node;
	while (index < (nodes_list->elements_count)) {
		node = (t_fs_node *) list_get(nodes_list, index);
		if (unwanted_node && (strcmp((node->node_name), unwanted_node) == 0)) {
			continue;
		} else {
			key = string_from_format("%sLibre", (node->node_name));
			free_blocks = config_get_int_value(nodes_table, key);
			if ((free_blocks > 0) && ((max < 0) || (free_blocks > max))) {
				if (selected_node) free(selected_node);
				max = free_blocks;
				selected_node = string_duplicate(node->node_name);
			}
			free(key);
		}
		index++;
	}
	pthread_mutex_unlock(&nodes_list_m_lock);
	return selected_node;
}

/**
 * @NAME assign_node_block
 */
int assign_node_block(char * node_name) {
	pthread_mutex_lock(&nodes_list_m_lock);
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
	pthread_mutex_unlock(&nodes_list_m_lock);
	return block;
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

//
// TODO: al crear un directoio se debe crear el directorio para los archivos del directorio (ejemplo: metadata/archivos/5/ejemplo.csv)
//

//
// Para verificar:
//

// Si el FS que se levanta, corresponde a un estado anterior,
// se debe verificar que el nombre de los archivos bitmap sean igual al configurado como nombre del nodo en Data-Node

// http://www.chuidiang.org/clinux/sockets/socketselect.php

















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
	pthread_mutex_lock(&nodes_list_m_lock);
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
	pthread_mutex_unlock(&nodes_list_m_lock);
}







t_config * create_file_metadata(int ,char *);
t_config * file_metadata(char * path) {
	t_config * f_metadata;
	int dir_index;
	int pd_index = ROOT;
	char * path_c = string_duplicate(path);
	char * node = strtok(path_c, "/");
	while (node != NULL) {
		dir_index = get_dir_index(node, pd_index);


		if (dir_index == ENOTDIR) {
			char * file_name = node;
			node = strtok(NULL, "/");
			if (node == NULL) {
				// final node, the regular file
				f_metadata = create_file_metadata(dir_index, file_name);
				if (pd_index != ROOT) rw_lock_unlock(directories_locks, UNLOCK, pd_index);
				if (!f_metadata) return ENOENT;
			} else {
				if (pd_index != ROOT) rw_lock_unlock(directories_locks, UNLOCK, pd_index);
				return ENOTDIR;
			}
		}




		if (pd_index != ROOT) rw_lock_unlock(directories_locks, UNLOCK, pd_index);
		pd_index = dir_index;
		node = strtok(NULL, "/");
	}
	free(path_c);
}

int get_dir_index(char * dir, int parent_index) {
	t_fs_directory * fs_dir = (t_fs_directory *) directories_mf_ptr;
	int index = 1;
	while (index < DIRECTORIES_AMOUNT) {
		if (index == parent_index) continue;
		rw_lock_unlock(directories_locks, LOCK_READ, index);
		if (fs_dir->parent_dir >= 0 && (fs_dir->parent_dir == parent_index && strcmp((char *)(fs_dir->name), dir) == 0)) {
				return index;
		}
		rw_lock_unlock(directories_locks, UNLOCK, index);
		index++;
		fs_dir++;
	}
	return ENOTDIR;
}

t_config * create_file_metadata(int dir_index, char * file_name) {
	return NULL;
}
