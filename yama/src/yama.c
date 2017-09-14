/*
 ============================================================================
 Name        : yama.c
 Author      : Carlos Flores
 Version     :
 Copyright   : GitHub @Charlos
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include "yama.h"

void closure(t_fs_file_block_metadata *);

int main(void) {

	char * fs_ip = "127.0.0.1";
	char * fs_port = "5003";
	int file_system_socket = connect_to_socket(fs_ip, fs_port);

	char * path = "/user/juan/datos/datos_personales.csv";
	t_fs_get_file_md_resp * resp = fs_get_file_metadata(file_system_socket, path, NULL);
	t_fs_file_metadata * file_md = resp->file_metadata;

	printf(">> exec code: %d \n", resp->exec_code);
	printf(">> path: %s \n", path);
	printf(">> file size: %d bytes\n", file_md->file_size);
	printf(">> type: %c \n", file_md->type);
	printf(">> blocks: %d \n", file_md->block_list->elements_count);

	t_fs_file_block_metadata * block_md;
	int index = 0;
	while (index < (file_md->block_list->elements_count)) {
		block_md = (t_fs_file_block_metadata *) list_get(file_md->block_list, index);
		printf("----------------------------------------------------\n");
		printf(">> >> file block n°: %d \n", block_md->file_block);
		printf(">> >> node: %d \n", block_md->node);
		printf(">> >> node block: %d \n", block_md->node_block);
		printf(">> >> copy node: %d \n", block_md->copy_node);
		printf(">> >> copy node block: %d \n", block_md->copy_node_block);
		printf(">> >> size: %d bytes\n", block_md->size);
		printf("----------------------------------------------------\n");
		index++;
	}

	list_destroy_and_destroy_elements(resp->file_metadata->block_list, &closure);
	free(resp->file_metadata);
	free(resp);

	return EXIT_SUCCESS;
}

void closure(t_fs_file_block_metadata * block_md) {
	free(block_md);
}
