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

int main(int argc, char ** argv) {

	if(argc != 5) {
		printf("ERROR: Cantidad de parametros invalida. Deben ser 4: transformador/ruta \nreductor/ruta \nArchivo de origen/ruta \narchivo resultado/ruta");
		exit(0);
	}
	master_config = crear_config();

	int yama_socket = connect_to_socket(master_config->ip_yama, master_config->port_yama);

	pedido_master * pedido = crear_pedido_yama(argv);

	// Enviar Pedido a YAMA

	// RECV LOOP
	int * operation_code;
	respuesta_yama * buffer;
	int status = recv(yama_socket, operation_code, sizeof(int), 0);

	while(status != -1 && operation_code == 1) {
		status = recv(yama_socket, buffer, sizeof(respuesta_yama), 0);

		if(status != -1) {

			// Genero un hilo que atienda la respuesta yama
			int s; pthread_attr_t attr;
			s = pthread_attr_init(&attr);
			pthread_t hilo;
			void* res;

			pthread_create(&hilo, &attr, &atender_respuesta, buffer);

			s = pthread_attr_destroy(&attr);
			s = pthread_join(hilo, res);
		} else {
			// atiendo error ?
		}
	}



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
