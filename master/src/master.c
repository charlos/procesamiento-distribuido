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

pedido_master * pedido;
int job_id;
int main(int argc, char ** argv) {

	if (argc != 5) {
		printf(
				"ERROR: Cantidad de parametros invalida. Deben ser 4: transformador/ruta \nreductor/ruta \nArchivo de origen/ruta \narchivo resultado/ruta");
		exit(0);
	}
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
	while(respuesta_solicitud->exec_code != ERROR && respuesta_solicitud->exec_code != SERVIDOR_DESCONECTADO && respuesta_solicitud->etapa != ALMACENAMIENTO_OK) {
		atender_solicitud(respuesta_solicitud);

		respuesta_solicitud = yama_resp_planificacion(yama_socket, logger);
	}

	gettimeofday(&tiempo_finalizacion, NULL);
	elapsedTime = ((tiempo_finalizacion.tv_sec*1e6 + tiempo_finalizacion.tv_usec) - (tiempo_inicio.tv_sec*1e6 + tiempo_inicio.tv_usec)) / 1000.0;

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
	int promedio_transformacion = calcular_promedio(est_reduccion_local->tiempo_ejecucion_hilos);
	printf("Tiempo promedio de ejecucion: %d ms\n", promedio_transformacion);

	printf("Cantidad total de tareas realizadas: %d\n", est_reduccion_local->cant_total_tareas);
	printf("Cantidad máxima de tareas simultaneas: %d\n", est_reduccion_local->cant_max_tareas_simultaneas);
	printf("Cantidad de fallos en la etapa: %d\n", est_reduccion_local->cant_fallos_job);

	printf("ETAPA DE REDUCCION GLOBAL\n");

	t_estadisticas * est_reduccion_global = list_get(lista_estadisticas, 2);
	int promedio_transformacion = calcular_promedio(est_reduccion_global->tiempo_ejecucion_hilos);
	printf("Tiempo promedio de ejecucion: %d ms\n", promedio_transformacion);

	printf("Cantidad total de tareas realizadas: %d\n", est_reduccion_global->cant_total_tareas);
	printf("Cantidad de fallos en la etapa: %d\n", est_reduccion_global->cant_fallos_job);


	/*
	 * 		|																				|
	 * 		|	Esto va a volar, lo dejo para reutilizar en atender solicitud	(mas abajo)	|
	 * 		v																				v
	 */
	if(respuesta_solicitud->exec_code == ERROR) { //TODO: MANEJO DE ERRORES
		printf("Hubo un error");
		exit(0);
	} else if(respuesta_solicitud->etapa == REDUCCION_LOCAL) {

		t_yama_planificacion_resp * respuesta_reduccion = respuesta_solicitud;
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

	struct timeval tiempo_inicio, tiempo_fin;
	uint32_t dif_tiempo;
	gettimeofday(&tiempo_inicio, NULL);

	ip_port_combo * combo = split_ipport(respuesta->ip_port);

	int socket_worker = connect_to_socket(combo->ip, combo->port);
	transform_req_send(socket_worker, respuesta->bloque,
			respuesta->bytes_ocupados, respuesta->archivo_temporal,
			transformador_file->filesize, transformador_file->file, logger);

	resultado_transformacion * result = malloc(sizeof(resultado_transformacion));
	int status = transform_res_recv(&socket_worker, result);


	yama_registrar_resultado_transf_bloque(yama_socket, job_id, respuesta->nodo, respuesta->bloque, result->resultado, logger);
//	status = yama_transform_res_send(&yama_socket, resultado);
	liberar_respuesta_transformacion(respuesta);
	liberar_combo_ip(combo);
	free(result);

	gettimeofday(&tiempo_fin, NULL);
	dif_tiempo = ((tiempo_fin.tv_sec*1e6 + tiempo_fin.tv_usec) - (tiempo_inicio.tv_sec*1e6 + tiempo_inicio.tv_usec)) / 1000.0;
	t_estadisticas * est_transformacion = list_get(lista_estadisticas, 0);
	list_add(est_transformacion->tiempo_ejecucion_hilos, dif_tiempo);
}
void atender_respuesta_reduccion(t_red_local * respuesta) {

	struct timeval tiempo_inicio, tiempo_fin;
	uint32_t dif_tiempo;
	gettimeofday(&tiempo_inicio, NULL);

	ip_port_combo * combo = split_ipport(respuesta->ip_puerto);
	int socket_worker = connect_to_socket(combo->ip, combo->port);

	struct_file *script_reduccion = read_file(pedido->ruta_reduc);
	local_reduction_req_send(socket_worker, respuesta->archivo_rl_temp, respuesta->archivos_temp, script_reduccion->filesize, script_reduccion->file, logger);
	liberar_respuesta_reduccion_local(respuesta);
	liberar_combo_ip(combo);

	free(script_reduccion->file);
	free(script_reduccion);

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
		file_struct->file = malloc(file_struct->filesize);

		file_struct->file = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED,
				file, 0);

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

void liberar_respuesta_reduccion_local(t_red_local *respuesta){
	free(respuesta->archivo_rl_temp);
	free(respuesta->archivos_temp);
	free(respuesta->nodo);
	free(respuesta->ip_puerto);
	free(respuesta);
}

void liberar_combo_ip(ip_port_combo *combo){
	free(combo->ip);
	free(combo->port);
	free(combo);
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
	list_add(est_transformacion);
	list_add(est_reduccion_local);
	list_add(est_reduccion_global);
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

		for(i = 0; i < list_size(solicitud->planificados); i++) {

			t_transformacion * transformacion = (t_transformacion *) list_get(solicitud->planificados, i);
			crear_hilo_transformador(transformacion, job_id);
		}
		t_estadisticas * est_transformacion = list_get(lista_estadisticas, 0);
		est_transformacion->cant_total_tareas *= list_size(solicitud->planificados);
		est_transformacion->cant_max_tareas_simultaneas = max(list_size(solicitud->planificados), est_transformacion->cant_max_tareas_simultaneas);
		break;

	case REDUCCION_LOCAL:
		for(i = 0; i < list_size(solicitud->planificados); i++) {
			t_red_local *reduccion = list_get(solicitud->planificados, i);
			crear_hilo_reduccion_local(reduccion);
		}
		t_estadisticas * est_reduccion_local = list_get(lista_estadisticas, 1);
		est_reduccion_local->cant_total_tareas += list_size(solicitud->planificados);
		est_reduccion_local->cant_max_tareas_simultaneas = max(list_size(solicitud->planificados), est_reduccion_local->cant_max_tareas_simultaneas);
		break;
	case REDUCCION_GLOBAL:
		for(i = 0; i < list_size(solicitud->planificados); i++) {
			t_red_global * nodo = list_get(solicitud->planificados, i);
			if(nodo->designado) {
				nodo_encargado = list_remove(solicitud->planificados, i);
				break;
			}
			//TODO: liberar memoria
		}
		ip_port_combo * ip_port = split_ipport(nodo_encargado->ip_puerto);
		nodo_enc_socket = connect_to_socket(ip_port->ip, ip_port->port);
		struct_file * file = read_file(pedido->ruta_reduc);
		// TODO: enviar script, lista de nodos, y lista de nombres de archivos

		// recibir respuesta de worker

		// Enviar notificacion a YAMA
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
		break;
	default:
		// Todavia nose
		printf("default");
	}
	// cada hilo tiene que liberar los atributos internos de su solicitud
	free(solicitud);
}
