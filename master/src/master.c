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
	t_yama_planificacion_resp * respuesta_transformacion = yama_nueva_solicitud(yama_socket, pedido->ruta_orige, logger);

	// RECV LOOP
	while(respuesta_transformacion->exec_code == EXITO && (respuesta_transformacion->etapa == TRANSFORMACION || respuesta_transformacion->etapa == REPLANIFICACION)) {
		int i;
		for(i = 0; i < list_size(respuesta_transformacion->planificados); i++) {

			t_transformacion * transformacion = (t_transformacion *) list_get(respuesta_transformacion->planificados, i);
			crear_hilo_transformador(transformacion, respuesta_transformacion->job_id);
		}
		free(respuesta_transformacion);
		respuesta_transformacion = yama_resp_planificacion(yama_socket, logger);
	}
	if(respuesta_transformacion->exec_code == ERROR) { //TODO: MANEJO DE ERRORES
		printf("Hubo un error");
		exit(0);
	} else if(respuesta_transformacion->etapa == REDUCCION_LOCAL) {

		t_yama_planificacion_resp * respuesta_reduccion = respuesta_transformacion;
		while(respuesta_reduccion->exec_code == EXITO && respuesta_reduccion->etapa == REDUCCION_LOCAL) {
			int i;
			for(i = 0; i < list_size(respuesta_reduccion->planificados); i++) {
				// obtener cada t_reduccion_local
				// crear hilo de reduccion local
			}
			// recibir resultado
		}
	}

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
void atender_respuesta_transform(respuesta_yama_transform * respuesta) {

	ip_port_combo * combo = split_ipport(respuesta->ip_port);

	int socket_worker = connect_to_socket(combo->ip, combo->port);
	transform_req_send(socket_worker, respuesta->bloque,
			respuesta->bytes_ocupados, respuesta->archivo_temporal,
			transformador_file->filesize, transformador_file->file, logger);

	resultado_transformacion * result = malloc(sizeof(resultado_transformacion));
	int status = transform_res_recv(&socket_worker, result);


	yama_resultado_transf_bloque(yama_socket, respuesta->job, &respuesta->nodo, respuesta->bloque, result->resultado, logger);
//	status = yama_transform_res_send(&yama_socket, resultado);
	liberar_respuesta_transformacion(respuesta);
	liberar_combo_ip(combo);
	free(result);
}
int atender_respuesta_reduccion(void * resp) {

}
struct_file * read_file(char * path) {
	FILE * file;
	struct stat st;
	// este trim nose porque rompe
//	string_trim(&path);
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

void liberar_respuesta_transformacion(respuesta_yama_transform *respuesta){
	free(respuesta->archivo_temporal);
	free(respuesta->ip_port);
	free(respuesta->nodo);
	free(respuesta);
}

void liberar_combo_ip(ip_port_combo *combo){
	free(combo->ip);
	free(combo->port);
	free(combo);
}

void crear_hilo_transformador(t_transformacion *transformacion, int job_id){
	pthread_t hilo_solicitud;
	pthread_attr_t attr;
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

	respuesta_yama_transform *transformacion_master = crear_transformacion_master(transformacion);
	transformacion_master->job = job_id;
	free(transformacion);
	pthread_create(&hilo_solicitud, &attr, &atender_respuesta_transform, transformacion_master);
	pthread_attr_destroy(&attr);
}

respuesta_yama_transform *crear_transformacion_master(t_transformacion *transformacion_yama){
	respuesta_yama_transform *transformacion_master = malloc(sizeof(respuesta_yama_transform));
	transformacion_master->archivo_temporal = transformacion_yama->archivo_temporal;
	transformacion_master->bloque = transformacion_yama->bloque;
	transformacion_master->bytes_ocupados = transformacion_yama->bytes_ocupados;
	transformacion_master->ip_port = transformacion_yama->ip_port;
	transformacion_master->nodo = transformacion_yama->nodo;

	return transformacion_master;
}
