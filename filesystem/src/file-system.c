
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
#include <errno.h>
#include <shared-library/file-system-prot.h>
#include <shared-library/data-node-prot.h>
#include <shared-library/socket.h>
#include "file-system.h"

#define	SOCKET_BACKLOG 			100

int listenning_socket;
t_fs_conf * fs_conf;
t_log * logger;

void * directories_mf_ptr; // directories mapped file ptr
t_config * nodes_table; // nodes table
t_list * bitmap_node_list; // bitmap nodes list

void fs_console(void *);
void process_request(int *);
void handshake(int *);
void load_file(int *);
void read_file(int *);
void get_file_metadata(int *);
void load_fs_properties(void);
void create_logger(void);
void init(void);
void closure(void *);
void connect_node(int *, char *, int);
void add_node(t_list *, char *, int);
void create_bitmap_for_node(char *, int);
void load_bitmap_node(bool, char *, int);
void * map_file(char *);

void create_metadata_file(char *, int, int, char);
int assign_blocks_to_file(t_config *, int);
char * assign_node(char *);
int assign_node_block(char *);
void free_assigned_blocks(t_config *, int);
void free_node_block(char *, int);

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
	// creating bitmap node list
	bitmap_node_list = list_create();

	free(nodes_bitmap_path);
	free(nodes_table_file_path);
	free(directories_file_path);
	free(files_path);
	free(metadata_path);
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
	char ** nodes = config_get_array_value(nodes_table, "NODOS");
	t_list * node_list = list_create();
	bool exits = false;
	int pos = 0;
	char * node;
	while (nodes[pos] != NULL) {
		node = nodes[pos];
		list_add(node_list, node);
		if (strcmp(node_name, node) == 0) {
			exits = true;
			break;
		}
		pos++;
	}

	if (!exits) {
		add_node(node_list, node_name, blocks);
		create_bitmap_for_node(node_name, blocks);
	}

	load_bitmap_node(!exits, node_name, blocks);

	//
	// TODO: ¿Debe existir una tabla de nodo - file descriptor para saber que socket corresponde a cada nodo?
	//

	list_destroy_and_destroy_elements(node_list, &closure);
	pos = 0;
	while (nodes[pos] != NULL) {
		free(nodes[pos]);
	}
	free(nodes);
	fs_handshake_send_resp(client_socket, SUCCESS);
}

/**
 * @NAME add_node
 */
void add_node(t_list * node_list, char * new_node_name, int blocks) {
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

	if (node_list->elements_count > 0) {
		node_list_str = string_from_format("[%s", (char *) (list_get(node_list, 0)));
		char * aux;
		int pos = 1;
		while (pos < (node_list->elements_count)) {
			aux = string_from_format("%s,%s", node_list_str, (char *) (list_get(node_list, pos)));
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
void load_bitmap_node(bool clean, char * node_name, int blocks) {

	t_fs_bitmap_node * bitmap_node;
	bool loaded = false;
	int index = 0;
	while (index < (bitmap_node_list->elements_count)) {
		bitmap_node = (t_fs_bitmap_node *) list_get(bitmap_node_list, index);
		if (strcmp(node_name, (bitmap_node->node_name)) == 0) {
			loaded = true;
			break;
		}
		index++;
	}

	if (!loaded) {
		char * bitmap_file_path = string_from_format("%s/metadata/bitmaps/%s.bin", (fs_conf->mount_point), node_name);
		t_bitarray * bitmap = bitarray_create_with_mode(map_file(bitmap_file_path), blocks, MSB_FIRST);
		if (clean) {
			int pos = 0;
			while (pos < blocks) {
				bitarray_clean_bit(bitmap, pos);
				pos++;
			}
		}
		int node_name_size = strlen(node_name) + 1;
		bitmap_node = (t_fs_bitmap_node *) malloc(sizeof(t_fs_bitmap_node));
		bitmap_node->node_name = malloc(sizeof(char) * node_name_size);
		memcpy((bitmap_node->node_name), node_name, node_name_size);
		bitmap_node->bitmap = bitmap;
		bitmap_node->size = blocks;
		list_add(bitmap_node_list, bitmap_node);
		free(bitmap_file_path);
	}
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

	int bytes_to_assign = file_size;

	char * key;
	char * value_str;
	char * orig;
	char * cpy;
	int assigned_block;
	int block = 0;
	while (bytes_to_assign > 0) {

		orig = assign_node(NULL);
		if (!orig) break;
		cpy = assign_node(orig);
		if (!cpy) {
			free(orig);
			break;
		}

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
		config_save(nodes_table);

		free(cpy);
		free(orig);
		bytes_to_assign -= BLOCK_SIZE;
		block++;
	}

	if (bytes_to_assign > 0) {
		free_assigned_blocks(file, (block + 1));
		return ENOSPC;
	}

	return SUCCESS;
}

/**
 * @NAME assign_node
 */
char * assign_node(char * unwanted_node) {
	char * selected_node;
	int max = -1;

	t_fs_bitmap_node * bitmap_node;
	char * key;
	int free_blocks;
	int index = 0;
	while (index < (bitmap_node_list->elements_count)) {
		bitmap_node = (t_fs_bitmap_node *) list_get(bitmap_node_list, index);
		if (unwanted_node && (strcmp((bitmap_node->node_name), unwanted_node) == 0)) {
			continue;
		} else {
			key = string_from_format("%sLibre", (bitmap_node->node_name));
			free_blocks = config_get_int_value(nodes_table, key);
			if ((free_blocks > 0) && ((max < 0) || (free_blocks > max))) {
				if (selected_node) free(selected_node);
				max = free_blocks;
				selected_node = string_duplicate(bitmap_node->node_name);
			}
			free(key);
		}
		index++;
	}
	return selected_node;
}

/**
 * @NAME assign_node_block
 */
int assign_node_block(char * node) {
	int block;
	t_fs_bitmap_node * bitmap_node;
	int index = 0;
	while (index < (bitmap_node_list->elements_count)) {
		bitmap_node = (t_fs_bitmap_node *) list_get(bitmap_node_list, index);
		if (strcmp(node, (bitmap_node->node_name)) == 0) {
			bool its_busy;
			int pos = 0;
			while (pos < (bitmap_node->size)) {
				its_busy = bitarray_test_bit(bitmap_node->bitmap, pos);
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
 * @NAME free_assigned_blocks
 */
void free_assigned_blocks(t_config * file, int used_blocks) {

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

}

/**
 * @NAME free_node_block
 */
void free_node_block(char * node, int block) {
	t_fs_bitmap_node * bitmap_node;
	int index = 0;
	while (index < (bitmap_node_list->elements_count)) {
		bitmap_node = (t_fs_bitmap_node *) list_get(bitmap_node_list, index);
		if (strcmp((bitmap_node->node_name), node) == 0) {
			bitarray_clean_bit(bitmap_node->bitmap, block);
			return;
		}
		index++;
	}
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
// Para verificar:
//

// Si el FS que se levanta, corresponde a un estado anterior,
// se debe verificar que el nombre de los archivos bitmap sean igual al configurado como nombre del nodo en Data-Node

// http://www.chuidiang.org/clinux/sockets/socketselect.php
