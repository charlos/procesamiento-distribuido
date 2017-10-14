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

	yama_socket = connect_to_socket(master_config->ip_yama,
			master_config->port_yama);

	// Enviar Pedido a YAMA

	// RECV LOOP
	int * operation_code;
	respuesta_yama_transform * buffer;
	int status = 1;

	while (status != -1) {
		status = yama_response_recv(&yama_socket, buffer);

		if (status != -1) {

			// Genero un hilo que atienda la respuesta yama
			int s;
			pthread_attr_t attr;
			s = pthread_attr_init(&attr);
			void* res;

			s = pthread_create(&buffer->thread_id, &attr, &atender_respuesta_transform,
					buffer);
			if (s != 0) {

			}
			s = pthread_attr_destroy(&attr);
			if (s != 0) {

			}
			s = pthread_join(buffer->thread_id, res);
			if (s != 0) {

			}

		} else {
			// atiendo error ?
		}
	}

	// REDUCCION LOCAL
	int status_reduccion;
	respuesta_yama_reduccion * paquete_reduccion = malloc(
			sizeof(respuesta_yama_reduccion));
	do {
		status_reduccion = reduccion_local_res_recv(&yama_socket,
				paquete_reduccion);

		// Genero un hilo que atienda la respuesta yama
		int s;
		pthread_attr_t attr;
		s = pthread_attr_init(&attr);
		void* res;

		s = pthread_create(&buffer->thread_id, &attr, &atender_respuesta_reduccion,
				buffer);
		if (s != 0) {

		}
		s = pthread_attr_destroy(&attr);
		if (s != 0) {

		}
		s = pthread_join(buffer->thread_id, res);
		if (s != 0) {

		}

	} while (status_reduccion != -1);

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
int atender_respuesta_transform(void * args) {
	respuesta_yama_transform * respuesta = (respuesta_yama_transform *) args;

	ip_port_combo * combo = split_ipport(respuesta->ip_port);

	int socket_worker = connect_to_socket(combo->ip, combo->port);
	transform_req_send(socket_worker, respuesta->bloque,
			respuesta->bytes_ocupados, respuesta->archivo_temporal,
			transformador_file->filesize, transformador_file->file, logger);

	int * result = malloc(sizeof(int));
	int status = transform_res_recv(&socket_worker, result);
	if (status != -1) {
		// NOTIFICA A YAMA
		status = yama_transform_res_send(&yama_socket, result);
	}
	free(respuesta);
	free(combo);
	free(result);
	return status;
}
int atender_respuesta_reduccion(void * resp) {

}
struct_file * read_file(char * path) {
	FILE * file;
	struct stat st;
	string_trim(&path);
	file = fopen(path, "r");

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
