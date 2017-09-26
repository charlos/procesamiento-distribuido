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

	ip_port_combo * combo = split_ipport(respuesta->ip_port);

	int socket_worker = connect_to_socket(combo->ip, combo->port);
	transform_req_send(socket_worker, respuesta->bloque,
			respuesta->bytes_ocupados, respuesta->archivo_temporal,
			transformador_file->filesize, transformador_file->file, logger);

	t_request_transformation * worker_response = transform_req_recv(
			socket_worker, logger);

	// NOTIFICA A YAMA

}
struct_file * read_file(char * path) {
	FILE * file;
	void *buffer = malloc(255);
	struct stat st;
	char * dir = string_trim(&path);
	file = fopen(dir, "r");

	if (file) {
		fstat(file, &st);
		size_t size = st.st_size;
		struct_file * file_struct = malloc(sizeof(struct_file));
		file_struct->filesize = st.st_size;
		file_struct->file = malloc(file_struct->filesize);

		file_struct->file = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED,
				file, 0);

	}
	return NULL;
}
