/*
 ============================================================================
 Name        : master.c
 Author      : Carlos Flores
 Version     :
 Copyright   : GitHub @Charlos
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include "master.h"
#include <commons/log.h>
#include <commons/config.h>
#include <shared-library/worker-prot.h>

int main(int argc, char ** argv) {

	if (argc != 5) {
		printf(
				"ERROR: Cantidad de parametros invalida. Deben ser 4: transformador/ruta \nreductor/ruta \nArchivo de origen/ruta \narchivo resultado/ruta");
		exit(0);
	}

	master_config = crear_config();
	pedido_master * pedido = crear_pedido_yama(argv);
	transformador_file = read_file(pedido->ruta_trans);

	int yama_socket = connect_to_socket(master_config->ip_yama,
			master_config->port_yama);

	// Enviar Pedido a YAMA

	// RECV LOOP
	int * operation_code;
	respuesta_yama * buffer;
	int status = recv(yama_socket, operation_code, sizeof(int), 0);

	while (status != -1 && operation_code == 1) {
		status = recv(yama_socket, buffer, sizeof(respuesta_yama), 0);

		if (status != -1) {

			// Genero un hilo que atienda la respuesta yama
			int s;
			pthread_attr_t attr;
			s = pthread_attr_init(&attr);
			respuesta_yama * respuesta = malloc(sizeof(respuesta_yama));
			void* res;

			s = pthread_create(&respuesta->thread_id, &attr, &atender_respuesta,
					buffer);
			if (s != 0) {

			}
			s = pthread_attr_destroy(&attr);
			if (s != 0) {

			}
			s = pthread_join(respuesta->thread_id, res);
			if (s != 0) {

			}

		} else {
			// atiendo error ?
		}
	}

	// REDUCCION LOCAL


	char* hora = temporal_get_string_time();
	printf("Hora actual: %s\n", hora); /* prints !!!Hello World!!! */
	//connect_send(hora);
	return EXIT_SUCCESS;
}
master_cfg * crear_config() {
	t_config * conf = malloc(sizeof(t_config));
	conf = config_create("./master.cfg");

	master_cfg * mcfg = malloc(sizeof(master_cfg));
	mcfg->ip_yama = config_get_string_value(conf, "IP_YAMA");
	mcfg->port_yama = config_get_string_value(conf, "PORT_YAMA");

	return mcfg;
}
pedido_master * crear_pedido_yama(char ** argv) {
	pedido_master * pedido = malloc(sizeof(pedido_master));
	pedido->ruta_trans = argv[1];
	pedido->ruta_reduc = argv[2];
	pedido->ruta_orige = argv[3];
	pedido->ruta_resul = argv[4];

	return pedido;
}
int atender_respuesta(void * args) {
	respuesta_yama * respuesta = (respuesta_yama *) args;

	char ** ip_port = string_split(respuesta->ip_port, ":");
	char * ip = ip_port[0];
	char * port = ip_port[1];
	int socket_worker = connect_to_socket(ip, port);
	transform_req_send(socket_worker, respuesta->bloque,
			respuesta->bytes_ocupados, respuesta->archivo_temporal,
			transformador_file->filesize,transformador_file->file, logger);

	t_request_transformation * worker_response = transform_req_recv(socket_worker, logger);

	// NOTIFICA A YAMA
	if(worker_response.exec_code == SUCCESS) {

	} else if (worker_response->exec_code == ERROR) {

	}
}

struct_file * read_file(char * path) {
	FILE * file;
	void *buffer = malloc(255);
	struct stat st;
	char * dir = string_trim(&path);
	file = fopen(dir, "r");

	if (file) {
		char * string = string_new();

		fstat(file, &st);
//		size_t size = st.st_size;
		struct_file * file_struct = malloc(sizeof(struct_file));
		file_struct->filesize = st.st_size;
		file_struct->file = string_new();

		/* POR AHORA LO HAGO POR STRING PORQUE NO SE SACAR EL PAGE OFFSET PARA USAR MMAP
		 *
		 fstat(file, &st);
		 size_t size = st.st_size;
		 void* script;

		 script = mmap(NULL, size, PROT_READ, MAP_PRIVATE, file,)
		 */

		while (fgets(buffer, 255, (FILE*) file)) {
			string_append(&string, buffer);

		}
		if (feof(file)) {
			fclose(file);
			free(buffer);
			string_append(&(file_struct->file), string);
			return file_struct;
		}
		return NULL;
	}
	return NULL;
}
