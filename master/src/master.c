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
#include <fcntl.h>
pedido_master * pedido;
int job_id;
int main(int argc, char ** argv) {

	if (argc != 5) {
		printf(
				"ERROR: Cantidad de parametros invalida. Deben ser 4: transformador/ruta \nreductor/ruta \nArchivo de origen/ruta \narchivo resultado/ruta");
		exit(0);
	}
	crear_logger(argv[0], &logger, false, LOG_LEVEL_TRACE);
	struct timeval tiempo_inicio, tiempo_finalizacion;
	uint32_t elapsedTime;
	lista_estadisticas = inicializar_estadisticas();
	gettimeofday(&tiempo_inicio, NULL);

	master_config = crear_config();
	pedido = crear_pedido_yama(argv);
	transformador_file = read_file(pedido->ruta_trans);

	yama_socket = connect_to_socket(master_config->ip_yama,
			master_config->port_yama);

	// Enviar Pedido a YAMA
	t_yama_planificacion_resp * respuesta_solicitud = yama_nueva_solicitud(yama_socket, pedido->ruta_orige, logger);
	job_id = respuesta_solicitud->job_id;
	// RECV LOOP
	while(respuesta_solicitud->exec_code != SERVIDOR_DESCONECTADO && respuesta_solicitud->etapa != ALMACENAMIENTO_OK) {
		atender_solicitud(respuesta_solicitud);

		respuesta_solicitud = yama_resp_planificacion(yama_socket, logger);
	}

	gettimeofday(&tiempo_finalizacion, NULL);
	elapsedTime = ((tiempo_finalizacion.tv_sec*1e6 + tiempo_finalizacion.tv_usec) - (tiempo_inicio.tv_sec*1e6 + tiempo_inicio.tv_usec)) / 1000.0;

	imprimir_estadisticas(elapsedTime);
	log_trace(logger, "Termino Ejecucion");
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

	struct timeval tiempo_inicio, tiempo_fin;
	uint32_t dif_tiempo;
	gettimeofday(&tiempo_inicio, NULL);

	log_trace(logger, "Job: %d - Se creo hilo para transformacion", job_id);
	ip_port_combo * combo = split_ipport(respuesta->ip_port);

	int socket_worker = connect_to_socket(combo->ip, combo->port);
	transform_req_send(socket_worker, respuesta->bloque, respuesta->bytes_ocupados, respuesta->archivo_temporal, transformador_file->filesize, transformador_file->file, logger);
	// TODO Manejar si el send salio mal


	t_response_task * response_task = task_response_recv(socket_worker, logger);
	send_recv_status(socket_worker, response_task->exec_code);

	int result;
	if(response_task->exec_code == DISCONNECTED_CLIENT) {
		result = TRANSF_ERROR;
	} else {
		result = traducir_respuesta(response_task->result_code, TRANSFORMACION);
	}

	yama_registrar_resultado_transf_bloque(yama_socket, job_id, respuesta->nodo, respuesta->bloque, result, logger);
//	status = yama_transform_res_send(&yama_socket, resultado);
	liberar_respuesta_transformacion(respuesta);
	liberar_combo_ip(combo);

	log_trace(logger, "Job: %d - Termina hilo para transformacion", job_id);
	gettimeofday(&tiempo_fin, NULL);
	dif_tiempo = ((tiempo_fin.tv_sec*1e6 + tiempo_fin.tv_usec) - (tiempo_inicio.tv_sec*1e6 + tiempo_inicio.tv_usec)) / 1000.0;
	t_estadisticas * est_transformacion = list_get(lista_estadisticas, 0);
	list_add(est_transformacion->tiempo_ejecucion_hilos, dif_tiempo);
}
void atender_respuesta_reduccion(t_red_local * respuesta) {

	struct timeval tiempo_inicio, tiempo_fin;
	uint32_t dif_tiempo;
	gettimeofday(&tiempo_inicio, NULL);

	log_trace(logger, "Job: %d - Se creo hilo para reduccion local", job_id);
	ip_port_combo * combo = split_ipport(respuesta->ip_puerto);
	int socket_worker = connect_to_socket(combo->ip, combo->port);

	struct_file *script_reduccion = read_file(pedido->ruta_reduc);
	local_reduction_req_send(socket_worker, respuesta->archivos_temp, respuesta->archivo_rl_temp, script_reduccion->filesize, script_reduccion->file, logger);
	// TODO Manejar si el send salio mal
	t_response_task * response_task = task_response_recv(socket_worker, logger);
	send_recv_status(socket_worker, response_task->exec_code);

	int result;
	if(response_task->exec_code == DISCONNECTED_CLIENT) {
		result = REDUC_LOCAL_ERROR;
	} else {
		result = traducir_respuesta(response_task->result_code, REDUCCION_LOCAL);
	}

	yama_registrar_resultado(yama_socket, job_id, respuesta->nodo, RESP_REDUCCION_LOCAL, result, logger);
	closure_rl(respuesta);
	liberar_combo_ip(combo);


	unmap_file(script_reduccion->file, script_reduccion->filesize);
	free(script_reduccion);

	log_trace(logger, "Job: %d - Termino hilo para reduccion local", job_id);
	gettimeofday(&tiempo_fin, NULL);
	dif_tiempo = ((tiempo_fin.tv_sec*1e6 + tiempo_fin.tv_usec) - (tiempo_inicio.tv_sec*1e6 + tiempo_inicio.tv_usec)) / 1000.0;
	t_estadisticas * est_reduccion_local = list_get(lista_estadisticas, 1);
	list_add(est_reduccion_local->tiempo_ejecucion_hilos, dif_tiempo);

}
struct_file * read_file(char * path) {
	FILE * file;
	struct stat st;
	// este trim nose porque rompe
//	string_trim(&path);
	file = fopen(path, "r");

	if (file) {
//		fstat(file, &st);
		fseek(file, 0L, SEEK_END);
		size_t size = ftell(file); // st.st_size;
		fseek(file, 0L, SEEK_SET);
		struct_file * file_struct = malloc(sizeof(struct_file));
		file_struct->filesize = size;

		file_struct->file = map_file(path, O_RDWR);
		fclose(file);
		return file_struct;
	}
	return NULL;
}

void liberar_respuesta_transformacion(respuesta_yama_transform *respuesta){
	free(respuesta->archivo_temporal);
	free(respuesta->ip_port);
	free(respuesta->nodo);
	free(respuesta);
}

t_estadisticas * inicializar_struct_estadisticas(int etapa) {
	t_estadisticas * nueva_estadistica = malloc(sizeof(t_estadisticas));
	nueva_estadistica->etapa = etapa;
	nueva_estadistica->tiempo_promedio_ejecucion = 0;
	nueva_estadistica->cant_max_tareas_simultaneas = 0;
	nueva_estadistica->cant_total_tareas = 0;
	nueva_estadistica->cant_fallos_job = 0;
	nueva_estadistica->tiempo_ejecucion_hilos = list_create();

	return nueva_estadistica;
}
t_list * inicializar_estadisticas() {
	t_estadisticas * est_transformacion = inicializar_struct_estadisticas(TRANSFORMACION);
	t_estadisticas * est_reduccion_local = inicializar_struct_estadisticas(REDUCCION_LOCAL);
	t_estadisticas * est_reduccion_global = inicializar_struct_estadisticas(REDUCCION_GLOBAL);
	t_list * lista_estadisticas = list_create();
	list_add(lista_estadisticas, est_transformacion);
	list_add(lista_estadisticas, est_reduccion_local);
	list_add(lista_estadisticas, est_reduccion_global);
	return lista_estadisticas;
}
int calcular_promedio(t_list * lista_promedios) {
	int i, total, cant;
	cant = list_size(lista_promedios);
	total = 0;

	for(i = 0; i < cant; i++) {
		int t = list_get(lista_promedios, i);
		total += t;
	}
	if(!cant) return 0;
	return total/cant;
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

void crear_hilo_reduccion_local(t_red_local *reduccion){
	pthread_t hilo_solicitud;
	pthread_attr_t attr;
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
	pthread_create(&hilo_solicitud, &attr, &atender_respuesta_reduccion, reduccion);
	pthread_attr_destroy(&attr);

}

void resolver_reduccion_global(t_yama_planificacion_resp *solicitud){
	int i, nodo_enc_socket;
	t_red_global * nodo_encargado;

	for(i = 0; i < list_size(solicitud->planificados); i++) {
		nodo_encargado = list_get(solicitud->planificados, i);
		if(nodo_encargado->designado)break;
	}
	ip_port_combo * ip_port = split_ipport(nodo_encargado->ip_puerto);
	nodo_enc_socket = connect_to_socket(ip_port->ip, ip_port->port);
	liberar_combo_ip(ip_port);
	struct_file * file = read_file(pedido->ruta_reduc);

	// Se envia script y lista de nodos a worker designado
	global_reduction_req_send(nodo_enc_socket, file->filesize, file->file,  solicitud->planificados, logger);

	// recibir respuesta de worker
	t_response_task * response_task = task_response_recv(nodo_enc_socket, logger);
	send_recv_status(nodo_enc_socket, response_task->exec_code);
	// Enviar notificacion a YAMA

	yama_registrar_resultado(yama_socket, job_id, nodo_encargado->nodo, RESP_REDUCCION_GLOBAL, response_task->result_code, logger);
	free(response_task);
	free(file->file);
	free(file);
}
respuesta_yama_transform *crear_transformacion_master(t_transformacion *transformacion_yama){
	respuesta_yama_transform *transformacion_master = malloc(sizeof(respuesta_yama_transform));
	transformacion_master->archivo_temporal = transformacion_yama->archivo_temporal;
	transformacion_master->bloque = transformacion_yama->bloque;
	transformacion_master->bytes_ocupados = transformacion_yama->bytes_ocupados;
	transformacion_master->ip_port = transformacion_yama->ip_puerto;
	transformacion_master->nodo = transformacion_yama->nodo;

	return transformacion_master;
}

void atender_solicitud(t_yama_planificacion_resp *solicitud){
	int i, nodo_enc_socket;
	t_red_global * nodo_encargado;
	switch(solicitud->etapa){
	case TRANSFORMACION:
		log_trace(logger, "Job: %d - Iniciando Transformacion", job_id);
		for(i = 0; i < list_size(solicitud->planificados); i++) {

			t_transformacion * transformacion = (t_transformacion *) list_get(solicitud->planificados, i);
			crear_hilo_transformador(transformacion, job_id);
		}
		t_estadisticas * est_transformacion = list_get(lista_estadisticas, 0);
		est_transformacion->cant_total_tareas *= list_size(solicitud->planificados);
		est_transformacion->cant_max_tareas_simultaneas = max(list_size(solicitud->planificados), est_transformacion->cant_max_tareas_simultaneas);
		break;

	case REDUCCION_LOCAL:
		log_trace(logger, "Job: %d - Iniciando Reduccion Local", job_id);
		for(i = 0; i < list_size(solicitud->planificados); i++) {
			t_red_local *reduccion = list_get(solicitud->planificados, i);
			crear_hilo_reduccion_local(reduccion);
		}
		t_estadisticas * est_reduccion_local = list_get(lista_estadisticas, 1);
		est_reduccion_local->cant_total_tareas += list_size(solicitud->planificados);
		est_reduccion_local->cant_max_tareas_simultaneas = max(list_size(solicitud->planificados), est_reduccion_local->cant_max_tareas_simultaneas);
		break;
	case REDUCCION_GLOBAL:
		log_trace(logger, "Job: %d - Iniciando Reduccion Global", job_id);
		resolver_reduccion_global(solicitud);
		list_iterate(solicitud->planificados, closure_rg);
		closure_rg(nodo_encargado);
		break;
	case ALMACENAMIENTO:
//		nodo_encargado = malloc(sizeof(t_red_global));
		nodo_encargado = list_get(solicitud->planificados, 0);
		ip_port_combo * ip_port_combo = split_ipport(nodo_encargado->ip_puerto);
		nodo_enc_socket = connect_to_socket(ip_port_combo->ip, ip_port_combo->port);
		// enviar solicitus a worker
		enviar_solicitud_almacenamiento_a_worker(nodo_enc_socket, nodo_encargado->archivo_rg);
		// recibir archivo y ruta

		// guardar?
		t_estadisticas * est_reduccion_global = list_get(lista_estadisticas, 2);
		est_reduccion_global->cant_total_tareas += list_size(solicitud->planificados);

		liberar_combo_ip(ip_port_combo);
		// TODO Terminar de liberar estructuras
		break;
	default:
		// Todavia nose
		printf("default");
	}
	// cada hilo tiene que liberar los atributos internos de su solicitud
	list_destroy(solicitud->planificados);
	free(solicitud);
}
int traducir_respuesta(int respuesta, int etapa) {
	if(respuesta == SUCCESS) {
		switch (etapa) {
		case TRANSFORMACION:
			return TRANSF_OK;
		case REDUCCION_LOCAL:
			return REDUC_LOCAL_OK;
		case REDUCCION_GLOBAL:
			return REDUC_GLOBAL_OK;
		default:
			break;
		}
	} else {
		switch (etapa) {
		case TRANSFORMACION:
			return TRANSF_ERROR;
		case REDUCCION_LOCAL:
			return REDUC_LOCAL_ERROR;
		case REDUCCION_GLOBAL:
			return REDUC_GLOBAL_ERROR;
		default:
			break;
		}
	}
}

void imprimir_estadisticas(int elapsedTime){
	printf("Tiempo de ejecucion total: %d ms  \n", elapsedTime);

	printf("ETAPA DE TRANSFORMACION\n");

	t_estadisticas * est_transformacion = list_get(lista_estadisticas, 0);
	int promedio_transformacion = calcular_promedio(est_transformacion->tiempo_ejecucion_hilos);
	printf("Tiempo promedio de ejecucion: %d ms\n", promedio_transformacion);

	printf("Cantidad total de tareas realizadas: %d\n", est_transformacion->cant_total_tareas);
	printf("Cantidad máxima de tareas simultaneas: %d\n", est_transformacion->cant_max_tareas_simultaneas);
	printf("Cantidad de fallos en la etapa: %d\n", est_transformacion->cant_fallos_job);

	printf("ETAPA DE REDUCCION LOCAL\n");

	t_estadisticas * est_reduccion_local = list_get(lista_estadisticas, 1);
	int promedio_reduccion_local = calcular_promedio(est_reduccion_local->tiempo_ejecucion_hilos);
	printf("Tiempo promedio de ejecucion: %d ms\n", promedio_reduccion_local);

	printf("Cantidad total de tareas realizadas: %d\n", est_reduccion_local->cant_total_tareas);
	printf("Cantidad máxima de tareas simultaneas: %d\n", est_reduccion_local->cant_max_tareas_simultaneas);
	printf("Cantidad de fallos en la etapa: %d\n", est_reduccion_local->cant_fallos_job);

	printf("ETAPA DE REDUCCION GLOBAL\n");

	t_estadisticas * est_reduccion_global = list_get(lista_estadisticas, 2);
	int promedio_reduccion_global = calcular_promedio(est_reduccion_global->tiempo_ejecucion_hilos);
	printf("Tiempo promedio de ejecucion: %d ms\n", promedio_reduccion_global);

	printf("Cantidad total de tareas realizadas: %d\n", est_reduccion_global->cant_total_tareas);
	printf("Cantidad de fallos en la etapa: %d\n", est_reduccion_global->cant_fallos_job);
}
