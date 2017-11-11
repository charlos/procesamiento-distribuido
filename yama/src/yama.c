
#include <commons/collections/list.h>
#include <commons/config.h>
#include <commons/log.h>
#include <pthread.h>
#include <shared-library/file-system-prot.h>
#include <shared-library/socket.h>
#include <shared-library/yama-prot.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include "yama.h"

t_yama_conf * yama_conf;
t_log * logger;
int fs_socket;
int new_job_id = 0;
int algoritmo;
pthread_mutex_t tabla_estados;

int clock_;
t_list * carga_nodos;
t_list * jobs;

#define	SOCKET_BACKLOG 						20
#define	CLOCK 								1
#define	WCLOCK								2

void yama_console(void *);
void signal_handler(int);
void registrar_resultado_t_bloque(int *, int, char *, int, int);
void registrar_resultado_rl(int *, int, char *, int);
void registrar_resultado_rg(int *, int, int);
void registrar_resultado_a(int *, int, int);
void recibir_solicitudes_master(void);
void procesar_nueva_solicitud(int *, char *);
void print_etapa(int);
void print_estado(int);
void posicionar_clock(void);
void planificar_etapa_reduccion_local_nodo(int *, int, char *);
void planificar_etapa_reduccion_global(int *, int);
void planificar_almacenamiento(int *, int);
void inicio(void);
void info_job(int);
void info_job(int);
void incluir_nodos_para_balanceo(t_fs_metadata_file *);
void incluir_nodo_job(t_yama_job *, t_list *, char *, char *, int, int, int, t_list *, t_yama_carga_nodo *);
void crear_log(void);
void cierre_t(t_transformacion *);
void cierre_rl(t_red_local *);
void cierre_rg(t_red_global *);
void cierre_cn(t_yama_carga_nodo *);
void cierre_cmd(t_fs_copy_block *);
void cierre_bamd(t_fs_file_block_metadata *);
void cierre_a(t_almacenamiento *);
void chequear_inicio_reduccion_local(int *, int, char *);
void chequear_inicio_reduccion_global(int *, int);
void cargar_configuracion(void);
void calcular_disponibilidad_nodos(void);
void balanceo_de_carga(t_yama_job *, t_fs_file_block_metadata *, t_list *);
t_list * planificar_etapa_transformacion(t_fs_metadata_file *);
int registrar_resultado_transformacion_bloque(int *);
int registrar_resultado_reduccion_local(int *);
int registrar_resultado_reduccion_global(int *);
int registrar_resultado_almacenamiento(int *);
int pwl(char *);
int nueva_solicitud(int *);
int disponibilidad(char *);
int balanceo_de_carga_replanificacion(t_yama_job *, t_yama_nodo_job *, t_yama_transformacion *, t_list *);
int atender_solicitud_master(int *);
char * get_result_message(int);
char * get_fs_error_message(int16_t);
bool is_number(char *);

int main(void) {

	cargar_configuracion();
	crear_log();
	inicio();

	signal(SIGUSR1, signal_handler);

	// thread sonsola
	pthread_attr_t attr;
	pthread_t thread;
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
	pthread_create(&thread, &attr, &yama_console, NULL);
	pthread_attr_destroy(&attr);

	fs_socket = connect_to_socket((yama_conf->fs_ip), (yama_conf->fs_puerto));
	int resp = fs_handshake(fs_socket, YAMA, NULL, NULL, NULL, logger);
	if (resp != SUCCESS) {
		log_error(logger, "error al conectarse a yama-fs");
		if (resp == UNSTEADY_FS) {
			log_error(logger, "yama-fs no estable");
		} else {
			log_error(logger, "yama-fs cod. error: %d", resp);
		}
		exit(EXIT_FAILURE);
	}
	recibir_solicitudes_master();
	return EXIT_SUCCESS;
}

/**
 * @NAME cargar_configuracion
 */
void cargar_configuracion(void) {
	t_config * conf = config_create("/home/utnso/yama.cfg");
	yama_conf = malloc(sizeof(t_yama_conf));
	yama_conf->puerto = config_get_int_value(conf, "PUERTO");
	yama_conf->fs_ip = string_duplicate(config_get_string_value(conf, "FS_IP"));
	yama_conf->fs_puerto = string_duplicate(config_get_string_value(conf, "FS_PUERTO"));
	yama_conf->retardo_plan = config_get_int_value(conf, "RETARDO_PLANIFICACION");
	yama_conf->algoritmo = string_duplicate(config_get_string_value(conf, "ALGORITMO_BALANCEO"));
	yama_conf->disp_base = config_get_int_value(conf, "DISP_BASE");
	yama_conf->log = string_duplicate(config_get_string_value(conf, "LOG"));
	config_destroy(conf);
}

/**
 * @NAME crear_log
 */
void crear_log(void) {
	logger = log_create((yama_conf->log), "yama_log", false, LOG_LEVEL_TRACE);
}

/**
 * @NAME inicio
 */
void inicio(void) {
	carga_nodos = list_create();
	jobs = list_create();
	algoritmo = (strcmp((yama_conf->algoritmo), "CLOCK") == 0) ? CLOCK : WCLOCK;
	pthread_mutex_init(&tabla_estados, NULL);
}

/**
 * @NAME signal_handler
 */
void signal_handler(int signal) {
	if (signal == SIGUSR1) {
		free(yama_conf->fs_ip);
		free(yama_conf->fs_puerto);
		free(yama_conf->algoritmo);
		free(yama_conf->log);
		free(yama_conf);
		cargar_configuracion();
		algoritmo = (strcmp((yama_conf->algoritmo), "CLOCK") == 0) ? CLOCK : WCLOCK;
	}
}

/**
 * @NAME recibir_solicitudes_master
 */
void recibir_solicitudes_master(void) {
	int atsm;
	int nuevaConexion;
	int fd_seleccionado;
	int listening_socket = open_socket(SOCKET_BACKLOG, (yama_conf->puerto));
	int set_fd_max = listening_socket;

	fd_set lectura;
	fd_set master;
	FD_ZERO(&lectura);
	FD_ZERO(&master);
	FD_SET(listening_socket, &master);

	for (;;) {
		lectura = master;
		select(set_fd_max + 1, &lectura, NULL, NULL, NULL);
		for (fd_seleccionado = 0 ; fd_seleccionado <= set_fd_max; fd_seleccionado++) {
			if (FD_ISSET(fd_seleccionado, &lectura)) {
				if (fd_seleccionado == listening_socket) {
					if ((nuevaConexion = accept_connection(listening_socket)) == -1) {
						log_error(logger, "Error al aceptar conexion");
					} else {
						log_trace(logger, "Nueva conexion: socket %d", nuevaConexion);
						FD_SET(nuevaConexion, &master);
						if (nuevaConexion > set_fd_max) set_fd_max = nuevaConexion;
					}
				} else {
					atsm = atender_solicitud_master(&fd_seleccionado);
					if (atsm == CLIENTE_DESCONECTADO) {
						FD_CLR(fd_seleccionado, &master);
					}
				}
			}
		}
	}
}

/**
 * @NAME atender_solicitud_master
 */
int atender_solicitud_master(int * socket_cliente) {
	int cod_operacion = yama_recv_cod_operacion(socket_cliente, logger);
	if (cod_operacion == CLIENTE_DESCONECTADO)
		return CLIENTE_DESCONECTADO;
	int atsm;
	switch(cod_operacion){
	case NUEVA_SOLICITUD:
		atsm = nueva_solicitud(socket_cliente);
		break;
	case REGISTRAR_RES_TRANSF_BLOQUE:
		atsm = registrar_resultado_transformacion_bloque(socket_cliente);
		break;
	case REGISTRAR_RES_REDUCCION_LOCAL:
		atsm = registrar_resultado_reduccion_local(socket_cliente);
		break;
	case REGISTRAR_RES_REDUCCION_GLOBAL:
		atsm = registrar_resultado_reduccion_global(socket_cliente);
		break;
	case REGISTRAR_RES_ALMACENAMIENTO:
		atsm = registrar_resultado_almacenamiento(socket_cliente);
		break;
	default:;
	}
	return atsm;
}



/**	╔═════════════════════════════════════════════════════════════════════════════════════════════════╗
	║                                         NUEVA SOLICITUD                                         ║
	╚═════════════════════════════════════════════════════════════════════════════════════════════════╝ **/

/**
 * @NAME nueva_solicitud
 */
int nueva_solicitud(int * socket_cliente) {
	t_yama_nueva_solicitud_req * req = yama_nueva_solicitud_recv_req(socket_cliente, logger);
	if ((req->exec_code) == CLIENTE_DESCONECTADO) {
		free(req);
		return CLIENTE_DESCONECTADO;
	}
	pthread_mutex_lock(&tabla_estados);
	procesar_nueva_solicitud(socket_cliente, (req->archivo));
	pthread_mutex_unlock(&tabla_estados);
	free(req->archivo);
	free(req);
	return EXITO;
}

/**
 * @NAME procesar_nueva_solicitud
 */
void procesar_nueva_solicitud(int * socket_cliente, char * archivo) {

	t_fs_get_md_file_resp * resp = fs_get_metadata_file(fs_socket, archivo, logger);

	if ((resp->exec_code) == EXITO) {
		t_fs_metadata_file * metadata = (resp->metadata_file);

		new_job_id++;
		usleep(1000 * (yama_conf->retardo_plan)); // delay planificacion

		incluir_nodos_para_balanceo(metadata);
		calcular_disponibilidad_nodos();
		posicionar_clock();

		t_list * planificados = planificar_etapa_transformacion(metadata);
		printf("\n nuevo job procesado exitosamente [job id %d]", new_job_id);
		yama_planificacion_send_resp(socket_cliente, EXITO, TRANSFORMACION, new_job_id, planificados);

		list_destroy_and_destroy_elements(planificados, &cierre_t);
		list_destroy_and_destroy_elements((metadata->block_list), &cierre_bamd);
		free(metadata->path);
		free(metadata);
	} else {
		printf("\n error al procesar nuevo job: %s", get_fs_error_message(resp->exec_code));
		yama_planificacion_send_resp(socket_cliente, (resp->exec_code), FINALIZADO_ERROR, -1, NULL);
	}
	free(resp);
}

/**
 * @NAME incluir_nodos_para_balanceo
 */
void incluir_nodos_para_balanceo(t_fs_metadata_file * metadata) {

	t_list * bloques_archivo = (metadata->block_list);
	t_list * copias;
	t_fs_file_block_metadata * bloque_archivo_md;
	t_fs_copy_block * copia_md;
	t_yama_carga_nodo * carga_nodo;

	bool existe;
	int i = 0;
	int j;
	int k;
	while (i < (bloques_archivo->elements_count)) {
		bloque_archivo_md = (t_fs_file_block_metadata *) list_get(bloques_archivo, i);
		copias = (bloque_archivo_md->copies_list);
		j = 0;
		while (j < (copias->elements_count)) {
			copia_md = (t_fs_copy_block *) list_get(copias, j);
			existe = false;
			k = 0;
			while (k < carga_nodos->elements_count) {
				carga_nodo = (t_yama_carga_nodo *) list_get(carga_nodos, k);
				if (strcmp((carga_nodo->nodo), (copia_md->node)) == 0) {
					existe = true;
					break;
				}
				k++;
			}
			if (!existe) {
				carga_nodo = (t_yama_carga_nodo *) malloc(sizeof(t_yama_carga_nodo));
				carga_nodo->nodo = string_duplicate(copia_md->node);
				carga_nodo->disponibilidad = 0;
				carga_nodo->wl = 0;
				carga_nodo->wl_total = 0;
				list_add(carga_nodos, carga_nodo);
			}
			j++;
		}
		i++;
	}
}

/**
 * @NAME calcular_disponibilidad_nodos
 */
void calcular_disponibilidad_nodos(void) {
	t_yama_carga_nodo * carga_nodo;
	int index = 0;
	while (index < (carga_nodos->elements_count)) {
		carga_nodo = (t_yama_carga_nodo *) list_get(carga_nodos, index);
		carga_nodo->disponibilidad = disponibilidad(carga_nodo->nodo);
		index++;
	}
}

/**
 * @NAME disponibilidad
 */
int disponibilidad(char * nodo) {
	return ((yama_conf->disp_base) - pwl(nodo));
}

/**
 * @NAME pwl
 */
int pwl(char * nodo) {

	if (((carga_nodos->elements_count) == 0) || (algoritmo == CLOCK))
		return 0;

	t_yama_carga_nodo * carga_nodo;
	int wl_max = -1;
	int wl_nodo;
	int index = 0;
	while (index < carga_nodos->elements_count) {
		carga_nodo = (t_yama_carga_nodo *) list_get(carga_nodos, index);
		if ((wl_max < 0) || ((carga_nodo->wl) > wl_max))
			wl_max = (carga_nodo->wl);
		if (strcmp(carga_nodo->nodo, nodo) == 0)
			wl_nodo = (carga_nodo->wl);
		index++;
	}
	return wl_max - wl_nodo;
}

/**
 * @NAME posicionar_clock
 */
void posicionar_clock(void) {

	int disp_max = -1;
	int wl_total_min = -1;

	t_yama_carga_nodo * carga_nodo;
	int index = 0;
	while (index < carga_nodos->elements_count) {
		carga_nodo = (t_yama_carga_nodo *) list_get(carga_nodos, index);
		if ((disp_max < 0) || ((carga_nodo->disponibilidad) > disp_max)
				|| (((carga_nodo->disponibilidad) == disp_max) && ((carga_nodo->wl_total) < wl_total_min))) {
			disp_max = (carga_nodo->disponibilidad);
			wl_total_min = (carga_nodo->wl_total);
			clock_ = index;
		}
		index++;
	}

}

/**
 * @NAME planificar_etapa_transformacion
 */
t_list * planificar_etapa_transformacion(t_fs_metadata_file * metadata) {

	t_yama_job * job = (t_yama_job *) malloc (sizeof(t_yama_job));
	job->job_id = new_job_id;
	job->etapa = TRANSFORMACION;
	job->nodos = list_create();

	t_list * planificados = list_create();
	t_list * bloques_archivo = (metadata->block_list);
	t_fs_file_block_metadata * bloque_archivo_md;
	int i = 0;
	while (i < (bloques_archivo->elements_count)) {
		bloque_archivo_md = (t_fs_file_block_metadata *) list_get(bloques_archivo, i);
		balanceo_de_carga(job, bloque_archivo_md, planificados);
		i++;
	}
	list_add(jobs, job);
	return planificados;
}

/**
 * @NAME balanceo_de_carga
 */
void balanceo_de_carga(t_yama_job * job, t_fs_file_block_metadata * bloque_archivo_md, t_list * planificados) {

	t_list * copias_bloque_md = (bloque_archivo_md->copies_list);
	t_list * copias_bloque = list_create();
	t_fs_copy_block * copia_md;
	t_yama_copia_bloque * copia;

	int index = 0;
	while (index < (copias_bloque_md->elements_count)) {
		copia_md = (t_fs_copy_block *) list_get(copias_bloque_md, index);
		copia = (t_yama_copia_bloque *) malloc (sizeof(t_yama_copia_bloque));
		copia->nodo = string_duplicate(copia_md->node);
		copia->ip_puerto = string_duplicate(copia_md->ip_port);
		copia->bloque_nodo = (copia_md->node_block);
		copia->planificada = false;
		list_add(copias_bloque, copia);
		index++;
	}

	t_yama_carga_nodo * carga_nodo;
	int clock_aux = clock_;
	for (;;) {
		carga_nodo = (t_yama_carga_nodo *) list_get(carga_nodos, clock_aux);
		index = 0;
		while (index < (copias_bloque->elements_count)) {
			copia = (t_yama_copia_bloque *) list_get(copias_bloque, index);
			if ((strcmp((carga_nodo->nodo), (copia->nodo)) == 0) && ((carga_nodo->disponibilidad) > 0)) {

				copia->planificada = true;
				incluir_nodo_job(job, planificados, (copia->nodo), (copia->ip_puerto), (bloque_archivo_md->file_block), (copia->bloque_nodo), (bloque_archivo_md->size), copias_bloque, carga_nodo);
				(carga_nodo->disponibilidad)--;

				clock_aux++; // avanzo clock
				if (clock_aux >= (carga_nodos->elements_count))
					clock_aux = 0;

				carga_nodo = (t_yama_carga_nodo *) list_get(carga_nodos, clock_aux);
				if ((carga_nodo->disponibilidad) == 0) {
					carga_nodo->disponibilidad = (yama_conf->disp_base);
					clock_aux++; // avanzo clock
					if (clock_aux >= (carga_nodos->elements_count))
						clock_aux = 0;
				}

				clock_ = clock_aux;
				return;
			}
			index++;
		}

		clock_aux++; // avanzo clock
		if (clock_aux >= (carga_nodos->elements_count))
			clock_aux = 0;

		if (clock_aux == clock_) {
			index = 0;
			while (index < (carga_nodos->elements_count)) {
				carga_nodo = (t_yama_carga_nodo *) list_get(carga_nodos, index);
				carga_nodo->disponibilidad += (yama_conf->disp_base);
				index++;
			}
		}
	}
}

void incluir_nodo_job(t_yama_job * job, t_list * planificados, char * nodo, char * ip_puerto, int bloque_archivo, int bloque_nodo, int bytes_ocupados, t_list * copias, t_yama_carga_nodo * carga_nodo) {

	t_list * transformaciones;
	t_list * nodos = (job->nodos);
	t_yama_nodo_job * nodo_job;
	bool existe = false;
	int i = 0;
	while (i < (nodos->elements_count)) {
		nodo_job = (t_yama_nodo_job *) list_get(nodos, i);
		if (strcmp((nodo_job->nodo), nodo) == 0) {
			existe = true;
			transformaciones = (nodo_job->transformaciones);
			break;
		}
		i++;
	}

	if (!existe) {
		nodo_job =  (t_yama_nodo_job *) malloc (sizeof(t_yama_nodo_job));
		nodo_job->nodo = string_duplicate(nodo);
		nodo_job->ip_puerto = string_duplicate(ip_puerto);
		nodo_job->estado = TRANSF_EN_PROCESO;
		nodo_job->transformaciones = list_create();
		nodo_job->archivo_rl_temp = string_new();
		nodo_job->designado = false;
		nodo_job->archivo_rg = string_new();
		nodo_job->wl_total_nodo = 0;
		nodo_job->carga = carga_nodo;
		transformaciones = (nodo_job->transformaciones);
		list_add(nodos, nodo_job);
	}

	t_yama_transformacion * transf = (t_yama_transformacion *) malloc (sizeof(t_yama_transformacion));
	transf->bloque_archivo = bloque_archivo;
	transf->bloque_nodo = bloque_nodo;
	transf->bytes_ocupados = bytes_ocupados;
	transf->copias = copias;
	transf->archivo_temp = string_new();
	string_append_with_format(&(transf->archivo_temp), "/J%d-%s-B%dETF", (job->job_id), (nodo_job->nodo), bloque_nodo);
	transf->estado = TRANSF_EN_PROCESO;
	list_add(transformaciones, transf);

	t_transformacion * planificado = (t_transformacion *) malloc (sizeof(t_transformacion));
	planificado->nodo = string_duplicate(nodo_job->nodo);
	planificado->ip_puerto = string_duplicate(nodo_job->ip_puerto);
	planificado->bloque = (transf->bloque_nodo);
	planificado->bytes_ocupados = (transf->bytes_ocupados);
	planificado->archivo_temporal = string_duplicate(transf->archivo_temp);
	list_add(planificados, planificado);

	(nodo_job->wl_total_nodo)++;
	(nodo_job->carga->wl)++;
	(nodo_job->carga->wl_total)++;

}










/**	╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
	║                                         REGISTRAR RESULTADO TRANSFORMACION                                         ║
	╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝ **/

/**
 * @NAME registrar_resultado_transformacion_bloque
 */
int registrar_resultado_transformacion_bloque(int * socket_cliente) {
	t_yama_reg_resultado_t_req * req = yama_registrar_resultado_t_recv_req(socket_cliente, logger);
	if ((req->exec_code) == CLIENTE_DESCONECTADO) {
		free(req);
		return CLIENTE_DESCONECTADO;
	}
	pthread_mutex_lock(&tabla_estados);
	registrar_resultado_t_bloque(socket_cliente, (req->job_id), (req->nodo), (req->bloque), (req->resultado_t));
	if ((req->resultado_t) == TRANSF_OK)
		chequear_inicio_reduccion_local(socket_cliente, (req->job_id), (req->nodo));
	pthread_mutex_unlock(&tabla_estados);
	free(req->nodo);
	free(req);
	return EXITO;
}

/**
 * @NAME registrar_resultado_t_bloque
 */
void registrar_resultado_t_bloque(int * socket_cliente, int job_id, char * nodo, int bloque, int resultado) {

	printf("\n [job id %d] - ET - registrando resultado - nodo \"%s\", bloque %d, resultado: %s", job_id, nodo, bloque, get_result_message(resultado));
	
	t_yama_job * job;
	t_yama_nodo_job * nodo_j;
	t_yama_transformacion * transf;

	int i = 0;
	int j;
	int k;
	while (i < (jobs->elements_count)) {
		job = (t_yama_job *) list_get(jobs, i);
		if ((job->job_id) == job_id) {
			j = 0;
			while (j < (job->nodos->elements_count)) {
				nodo_j = (t_yama_nodo_job *) list_get((job->nodos), j);
				if (strcmp((nodo_j->nodo), nodo) == 0) {
					k = 0;
					while (k < (nodo_j->transformaciones->elements_count)) {

						transf = (t_yama_transformacion *) list_get((nodo_j->transformaciones), k);

						if ((transf->bloque_nodo) == bloque) {

							transf->estado = resultado; // registro resultado transformacion

							if ((job->etapa) == FINALIZADO_ERROR) {

								printf("\n [job id %d] - FINALIZADO POR ERROR PREVIO", job_id);

								// el job finalizo antes
								// error previo en otra transformacion perteneciente al mismo job
								(nodo_j->carga->wl)--; // reducir carga nodo

							} else if ((job->etapa) == TRANSFORMACION) {

								if (resultado == TRANSF_ERROR) {
									//
									// replanificacion transformacion
									//
									(nodo_j->carga->wl)--; // reducir carga nodo
									usleep(1000 * (yama_conf->retardo_plan)); // delay planificacion
									t_list * planificados = list_create();

									if (balanceo_de_carga_replanificacion(job, nodo_j, transf, planificados) == EXITO) {

										transf = list_remove((nodo_j->transformaciones), k); // se pudo replanificar, remuevo transformacion con error
										free(transf->archivo_temp);
										free(transf);

										yama_planificacion_send_resp(socket_cliente, EXITO, TRANSFORMACION, job_id, planificados);
										printf("\n [job id %d] - ET - replanificación exitosa", job_id);
									} else {
										yama_planificacion_send_resp(socket_cliente, ERROR, FINALIZADO_ERROR, job_id, NULL);
										printf("\n [job id %d] - FINALIZADO CON ERROR (no hay nodos disponibles para replanificación)", job_id);
										info_job(job_id);
									}
									list_destroy_and_destroy_elements(planificados, &cierre_t);
								}

							}
							return;
						}
						k++;
					}
				}
				j++;
			}
		}
		i++;
	}
}

/**
 * @NAME balanceo_de_carga_replanificacion
 */
int balanceo_de_carga_replanificacion(t_yama_job * job, t_yama_nodo_job * nodo_j, t_yama_transformacion * transf, t_list * planificados) {

	t_yama_copia_bloque * copia;

	bool sin_nodos_disponibles = true;
	int index = 0;
	while (index < (transf->copias->elements_count)) {
		copia = (t_yama_copia_bloque *) list_get((transf->copias), index);
		if (!(copia->planificada)) {
			sin_nodos_disponibles = false;
			break;
		}
	}

	if (sin_nodos_disponibles) {
		transf->estado = TRANSF_ERROR_SIN_NODOS_DISP;
		nodo_j->estado = TRANSF_ERROR;
		job->etapa = FINALIZADO_ERROR;
		return FINALIZADO_ERROR;
	}

	int clock_aux = clock_;
	t_yama_carga_nodo * carga_nodo;
	for (;;) {
		carga_nodo = (t_yama_carga_nodo *) list_get(carga_nodos, clock_aux);
		index = 0;
		while (index < (transf->copias->elements_count)) {

			copia = (t_fs_copy_block *) list_get((transf->copias), index);

			if (!(copia->planificada) && (strcmp((carga_nodo->nodo), (copia->nodo)) == 0) && ((carga_nodo->disponibilidad) > 0)) {

				copia->planificada = true;
				incluir_nodo_job(job, planificados, (copia->nodo), (copia->ip_puerto), (transf->bloque_archivo), (copia->bloque_nodo), (transf->bytes_ocupados), (transf->copias), carga_nodo);
				(carga_nodo->disponibilidad)--;

				clock_aux++; // avanzo clock
				if (clock_aux >= (carga_nodos->elements_count))
					clock_aux = 0;


				carga_nodo = (t_yama_carga_nodo *) list_get(carga_nodos, clock_aux);
				if ((carga_nodo->disponibilidad) == 0) {
					carga_nodo->disponibilidad = (yama_conf->disp_base);
					clock_aux++; // avanzo clock
					if (clock_aux >= (carga_nodos->elements_count))
						clock_aux = 0;
				}
				clock_ = clock_aux;
				return EXITO;
			}
			index++;
		}

		clock_aux++; // avanzo clock
		if (clock_aux >= (carga_nodos->elements_count))
			clock_aux = 0;

		if (clock_aux == clock_) {
			index = 0;
			while (index < (carga_nodos->elements_count)) {
				carga_nodo = (t_yama_carga_nodo *) list_get(carga_nodos, index);
				carga_nodo->disponibilidad += (yama_conf->disp_base);
				index++;
			}
		}
	}
}

/**
 * @NAME chequear_inicio_reduccion_local
 */
void chequear_inicio_reduccion_local(int * socket_cliente, int job_id, char * nodo) {

	bool planificar_rl = true;
	t_yama_job * job;
	t_yama_nodo_job * nodo_j;
	t_yama_transformacion * transf;

	int i = 0;
	int j;
	int k;
	while (i < (jobs->elements_count)) {
		job = (t_yama_job *) list_get(jobs, i);
		if ((job->job_id) == job_id) {
			j = 0;
			while (j < (job->nodos->elements_count)) {
				nodo_j = (t_yama_nodo_job *) list_get((job->nodos), j);
				if (strcmp((nodo_j->nodo), nodo) == 0) {
					if ((nodo_j->estado) == TRANSF_ERROR) {
						planificar_rl = false;
					} else {
						k = 0;
						while (k < (nodo_j->transformaciones->elements_count)) {
							transf = (t_yama_transformacion *) list_get((nodo_j->transformaciones), k);
							if (transf->estado != TRANSF_OK) {
								planificar_rl = false;
								break;
							}
							k++;
						}
					}
					break;
				}
				j++;
			}
			break;
		}
		i++;
	}

	if (planificar_rl) {
		usleep(1000 * (yama_conf->retardo_plan)); // delay planificacion
		planificar_etapa_reduccion_local_nodo(socket_cliente, job_id, nodo);
	}
}

/**
 * @NAME planificar_etapa_reduccion_local_nodo
 */
void planificar_etapa_reduccion_local_nodo(int * socket_cliente, int job_id, char * nodo) {

	t_list * planificados = list_create();
	t_red_local * red_local;
	t_yama_job * job;
	t_yama_nodo_job * nodo_j;
	t_yama_transformacion * transf;

	int i = 0;
	int j;
	int k;
	while (i < (jobs->elements_count)) {
		job = (t_yama_job *) list_get(jobs, i);
		if ((job->job_id) == job_id) {
			j = 0;
			while (j < (job->nodos->elements_count)) {
				nodo_j = (t_yama_nodo_job *) list_get((job->nodos), j);
				if (strcmp((nodo_j->nodo), nodo) == 0) {

					char * archivo_rl_temp = string_new();
					string_append_with_format(&archivo_rl_temp, "/J%d-%s-r-local", job_id, nodo);

					job->etapa = REDUCCION_LOCAL;
					nodo_j->estado = REDUC_LOCAL_EN_PROCESO;

					free(nodo_j->archivo_rl_temp);
					nodo_j->archivo_rl_temp = string_duplicate(archivo_rl_temp);

					red_local = (t_red_local *) malloc (sizeof(t_red_local));
					red_local->nodo = string_duplicate(nodo_j->nodo);
					red_local->ip_puerto = string_duplicate(nodo_j->ip_puerto);
					red_local->archivo_rl_temp = string_duplicate(archivo_rl_temp);
					red_local->archivos_temp = string_new();

					transf = (t_yama_transformacion *) list_get((nodo_j->transformaciones), 0);
					string_append(&(red_local->archivos_temp), (transf->archivo_temp));

					k = 1;
					while (k < (nodo_j->transformaciones->elements_count)) {
						transf = (t_yama_transformacion *) list_get((nodo_j->transformaciones), k);
						string_append_with_format(&(red_local->archivos_temp), ";%s", (transf->archivo_temp));
						k++;
					}

					free(archivo_rl_temp);
					break;
				}
				j++;
			}
			break;
		}
		i++;
	}

	list_add(planificados, red_local);
	yama_planificacion_send_resp(socket_cliente, EXITO, REDUCCION_LOCAL, job_id, planificados);
	printf("\n [job id %d] - ERL planificado - nodo \"%s\"", job_id, nodo);

	list_destroy_and_destroy_elements(planificados, &cierre_rl);
}










/**	╔═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
	║                                         REGISTRAR RESULTADO REDUCCION LOCAL                                         ║
	╚═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝ **/

/**
 * @NAME registrar_resultado_reduccion_local
 */
int registrar_resultado_reduccion_local(int * socket_cliente) {
	t_yama_reg_resultado_req * req = yama_registrar_resultado_recv_req(socket_cliente, logger);
	if ((req->exec_code) == CLIENTE_DESCONECTADO) {
		free(req);
		return CLIENTE_DESCONECTADO;
	}
	pthread_mutex_lock(&tabla_estados);
	registrar_resultado_rl(socket_cliente, (req->job_id), (req->nodo), (req->resultado));
	if ((req->resultado) == REDUC_LOCAL_OK)
		chequear_inicio_reduccion_global(socket_cliente, (req->job_id));
	pthread_mutex_unlock(&tabla_estados);
	free(req->nodo);
	free(req);
	return EXITO;
}

/**
 * @NAME registrar_resultado_rl
 */
void registrar_resultado_rl(int * socket_cliente, int job_id, char * nodo, int resultado) {

	printf("\n [job id %d] - ERL - registrando resultado - nodo \"%s\", resultado: %s", job_id, nodo, get_result_message(resultado));

	t_yama_job * job;
	t_yama_nodo_job * nodo_j;

	int i = 0;
	int j;
	while (i < (jobs->elements_count)) {
		job = (t_yama_job *) list_get(jobs, i);
		if ((job->job_id) == job_id) {
			j = 0;
			while (j < (job->nodos->elements_count)) {
				nodo_j = (t_yama_nodo_job *) list_get((job->nodos), j);
				if (strcmp((nodo_j->nodo), nodo) == 0) {

					nodo_j->estado = resultado;

					if ((job->etapa) == FINALIZADO_ERROR) {

						printf("\n [job id %d] - FINALIZADO POR ERROR PREVIO", job_id);

						// el job finalizo antes
						// error previo en reduccion local realizada en otro nodo para el mismo job
						(nodo_j->carga->wl) -= (nodo_j->wl_total_nodo);

					} else if ((job->etapa) == REDUCCION_LOCAL) {

						if (resultado == REDUC_LOCAL_ERROR) {
							//
							// error en reduccion local, no hay replanificacion
							//
							(nodo_j->carga->wl) -= (nodo_j->wl_total_nodo);
							job->etapa = FINALIZADO_ERROR;
							yama_planificacion_send_resp(socket_cliente, ERROR, FINALIZADO_ERROR, job_id, NULL);
							printf("\n [job id %d] - FINALIZADO CON ERROR", job_id);
							info_job(job_id);
						}

					}

					return;
				}
				j++;
			}
			break;
		}
		i++;
	}
}

/**
 * @NAME chequear_inicio_reduccion_global
 */
void chequear_inicio_reduccion_global(int * socket_cliente, int job_id) {

	bool planificar_rg = true;
	t_yama_job * job;
	t_yama_nodo_job * nodo_j;

	int i = 0;
	int j;
	while (i < (jobs->elements_count)) {
		job = (t_yama_job *) list_get(jobs, i);
		if ((job->job_id) == job_id) {
			j = 0;
			while (j < (job->nodos->elements_count)) {
				nodo_j = (t_yama_nodo_job *) list_get((job->nodos), j);
				if (nodo_j->estado != REDUC_LOCAL_OK) {
					planificar_rg = false;
					break;
				}
				j++;
			}
			break;
		}
		i++;
	}

	if (planificar_rg) {
		usleep(1000 * (yama_conf->retardo_plan)); // delay planificacion
		planificar_etapa_reduccion_global(socket_cliente, job_id);
	}

}

/**
 * @NAME planificar_etapa_reduccion_global
 */
void planificar_etapa_reduccion_global(int * socket_cliente, int job_id) {

	t_yama_job * job;
	t_yama_nodo_job * nodo_j;
	t_yama_nodo_job * nodo_designado;

	int cant_red_temp;
	int i = 0;
	int j;
	while (i < (jobs->elements_count)) {
		job = (t_yama_job *) list_get(jobs, i);
		if ((job->job_id) == job_id) {
			cant_red_temp = (job->nodos->elements_count);
			j = 0;
			while (j < (job->nodos->elements_count)) {
				nodo_j = (t_yama_nodo_job *) list_get((job->nodos), j);
				if ((!nodo_designado) || (nodo_j->wl_total_nodo < nodo_designado->wl_total_nodo)) {
					nodo_designado = nodo_j;
				}
				j++;
			}
			break;
		}
		i++;
	}

	int inc_carga = ((cant_red_temp / 2) + (((cant_red_temp % 2) > 0) ? 1 : 0));
	(nodo_designado->wl_total_nodo) += inc_carga;
	(nodo_designado->carga->wl) += inc_carga;
	(nodo_designado->carga->wl_total) += inc_carga;
	 nodo_designado->designado = true;

	t_list * planificados = list_create();
	t_red_global * red_global;

	i = 0;
	while (i < (jobs->elements_count)) {
		job = (t_yama_job *) list_get(jobs, i);
		if ((job->job_id) == job_id) {
			job->etapa = REDUCCION_GLOBAL;
			j = 0;
			while (j < (job->nodos->elements_count)) {
				nodo_j = (t_yama_nodo_job *) list_get((job->nodos), j);
				red_global = (t_red_global *) malloc(sizeof(t_red_global));
				red_global->nodo = string_duplicate(nodo_j->nodo);
				red_global->ip_puerto = string_duplicate(nodo_j->ip_puerto);
				red_global->archivo_rl_temp = string_duplicate(nodo_j->archivo_rl_temp);
				if (nodo_designado == nodo_j) {
					nodo_j->estado = REDUC_GLOBAL_EN_PROCESO;
					red_global->designado = true;
					char * archivo_rg = string_new();
					string_append_with_format(&archivo_rg, "/J%d-final", job_id);
					red_global->archivo_rg = string_duplicate(archivo_rg);
					free(nodo_j->archivo_rg);
					nodo_j->archivo_rg = string_duplicate(archivo_rg);
					free(archivo_rg);
				} else {
					red_global->designado = false;
					red_global->archivo_rg = string_new();
				}
				list_add(planificados, red_global);
				j++;
			}
			break;
		}
		i++;
	}

	yama_planificacion_send_resp(socket_cliente, EXITO, REDUCCION_GLOBAL, job_id, planificados);
	printf("\n [job id %d] - ERG planificado - nodo designado \"%s\"", job_id, (nodo_designado->nodo));

	list_destroy_and_destroy_elements(planificados, &cierre_rg);
}










/**	╔══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
	║                                         REGISTRAR RESULTADO REDUCCION GLOBAL                                         ║
	╚══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝ **/

/**
 * @NAME registrar_resultado_reduccion_global
 */
int registrar_resultado_reduccion_global(int * socket_cliente) {
	t_yama_reg_resultado_req * req = yama_registrar_resultado_recv_req(socket_cliente, logger);
	if ((req->exec_code) == CLIENTE_DESCONECTADO) {
		free(req);
		return CLIENTE_DESCONECTADO;
	}
	pthread_mutex_lock(&tabla_estados);
	registrar_resultado_rg(socket_cliente, (req->job_id), (req->resultado));
	if ((req->resultado) == REDUC_GLOBAL_OK) {
		usleep(1000 * (yama_conf->retardo_plan)); // delay planificacion
		planificar_almacenamiento(socket_cliente, (req->job_id));
	}
	pthread_mutex_unlock(&tabla_estados);
	free(req->nodo);
	free(req);
	return EXITO;
}

/**
 * @NAME registrar_resultado_rg
 */
void registrar_resultado_rg(int * socket_cliente, int job_id, int resultado) {

	printf("\n [job id %d] - ERG - registrando resultado - resultado: %s", job_id, get_result_message(resultado));

	t_yama_job * job;
	t_yama_nodo_job * nodo_j;

	int i = 0;
	int j;
	while (i < (jobs->elements_count)) {
		job = (t_yama_job *) list_get(jobs, i);
		if ((job->job_id) == job_id) {
			j = 0;
			while (j < (job->nodos->elements_count)) {
				nodo_j = (t_yama_nodo_job *) list_get((job->nodos), j);
				if (nodo_j->designado) {

					nodo_j->estado = resultado;

					if ((job->etapa) == REDUCCION_GLOBAL) {

						if (resultado == REDUC_GLOBAL_ERROR) {
							//
							// error en reduccion global, no hay replanificacion
							//
							// libero cargas de todos los nodos
							j = 0;
							while (j < (job->nodos->elements_count)) {
								nodo_j = (t_yama_nodo_job *) list_get((job->nodos), j);
								(nodo_j->carga->wl) -= (nodo_j->wl_total_nodo);
								j++;
							}
							job->etapa = FINALIZADO_ERROR;
							yama_planificacion_send_resp(socket_cliente, ERROR, FINALIZADO_ERROR, job_id, NULL);
							printf("\n [job id %d] - FINALIZADO CON ERROR", job_id);
							info_job(job_id);
						}

					}
					break;
				}
				j++;
			}
			break;
		}
		i++;
	}
}

/**
 * @NAME planificar_almacenamiento
 */
void planificar_almacenamiento(int * socket_cliente, int job_id) {

	t_list * planificados = list_create();
	t_almacenamiento * almacenamiento;

	t_yama_job * job;
	t_yama_nodo_job * nodo_j;

	int i = 0;
	int j;
	while (i < (jobs->elements_count)) {
		job = (t_yama_job *) list_get(jobs, i);
		if ((job->job_id) == job_id) {
			job->etapa = ALMACENAMIENTO;
			j = 0;
			while (j < (job->nodos->elements_count)) {
				nodo_j = (t_yama_nodo_job *) list_get((job->nodos), j);
				if (nodo_j->designado) {
					nodo_j->estado = ALMACENAMIENTO_EN_PROCESO;
					t_almacenamiento * almacenamiento = (t_almacenamiento *) malloc (sizeof(t_almacenamiento));
					almacenamiento->nodo = string_duplicate(nodo_j->nodo);
					almacenamiento->ip_puerto = string_duplicate(nodo_j->ip_puerto);
					almacenamiento->archivo_rg = string_duplicate(nodo_j->archivo_rg);
					list_add(planificados, almacenamiento);
					printf("\n [job id %d] - EA planificado - nodo designado \"%s\"", job_id, (almacenamiento->nodo));
					break;
				}
				j++;
			}
			break;
		}
		i++;
	}

	yama_planificacion_send_resp(socket_cliente, EXITO, ALMACENAMIENTO, job_id, planificados);
	list_destroy_and_destroy_elements(planificados, &cierre_a);
}










/**	╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
	║                                         REGISTRAR RESULTADO ALMACENAMIENTO                                         ║
	╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝ **/

/**
 * @NAME registrar_resultado_almacenamiento
 */
int registrar_resultado_almacenamiento(int * socket_cliente) {
	t_yama_reg_resultado_req * req = yama_registrar_resultado_recv_req(socket_cliente, logger);
	if ((req->exec_code) == CLIENTE_DESCONECTADO) {
		free(req);
		return CLIENTE_DESCONECTADO;
	}
	pthread_mutex_lock(&tabla_estados);
	registrar_resultado_a(socket_cliente, (req->job_id), (req->resultado));
	pthread_mutex_unlock(&tabla_estados);
	free(req->nodo);
	free(req);
	return EXITO;
}

/**
 * @NAME registrar_resultado_a
 */
void registrar_resultado_a(int * socket_cliente, int job_id, int resultado) {

	printf("\n [job id %d] - EA - registrando resultado - resultado: %s", job_id, get_result_message(resultado));

	t_yama_job * job;
	t_yama_nodo_job * nodo_j;

	int i = 0;
	int j;
	while (i < (jobs->elements_count)) {
		job = (t_yama_job *) list_get(jobs, i);
		if ((job->job_id) == job_id) {
			j = 0;
			while (j < (job->nodos->elements_count)) {
				nodo_j = (t_yama_nodo_job *) list_get((job->nodos), j);
				if (nodo_j->designado) {

					nodo_j->estado = resultado;

					// libero cargas de todos los nodos (finalizacion del job)
					j = 0;
					while (j < (job->nodos->elements_count)) {
						nodo_j = (t_yama_nodo_job *) list_get((job->nodos), j);
						(nodo_j->carga->wl) -= (nodo_j->wl_total_nodo);
						j++;
					}

					if ((job->etapa) == ALMACENAMIENTO) {

						if (resultado == ALMACENAMIENTO_ERROR) {
							//
							// error en reduccion global, no hay replanificacion
							//
							job->etapa = FINALIZADO_ERROR;
							yama_planificacion_send_resp(socket_cliente, ERROR, FINALIZADO_ERROR, job_id, NULL);
							printf("\n [job id %d] - FINALIZADO CON ERROR", job_id);
						} else {
							job->etapa = FINALIZADO_OK;
							yama_planificacion_send_resp(socket_cliente, EXITO, FINALIZADO_OK, job_id, NULL);
							printf("\n [job id %d] - EA - finalizado exitosamente", job_id);
							printf("\n [job id %d] - FINALIZADO EXITOSAMENTE", job_id);
						}
						info_job(job_id);

					}

					break;
				}
				j++;
			}
			break;
		}
		i++;
	}
}

/**
 * @NAME cierre_bamd
 */
void cierre_bamd(t_fs_file_block_metadata * bloque_achivo_md) {
	list_destroy_and_destroy_elements((bloque_achivo_md->copies_list), &cierre_cmd);
	free(bloque_achivo_md);
}

/**
 * @NAME cierre_cmd
 */
void cierre_cmd(t_fs_copy_block * copia_md) {
	free(copia_md);
}

/**
 * @NAME cierre_t
 */
void cierre_t(t_transformacion * transformacion) {
	free(transformacion->nodo);
	free(transformacion->ip_puerto);
	free(transformacion->archivo_temporal);
	free(transformacion);
}

/**
 * @NAME cierre_rl
 */
void cierre_rl(t_red_local * rl) {
	free(rl->nodo);
	free(rl->ip_puerto);
	free(rl->archivo_rl_temp);
	free(rl->archivos_temp);
	free(rl);
}

/**
 * @NAME cierre_rg
 */
void cierre_rg(t_red_global * rg) {
	free(rg->nodo);
	free(rg->ip_puerto);
	free(rg->archivo_rl_temp);
	free(rg->archivo_rg);
	free(rg);
}

/**
 * @NAME cierre_a
 */
void cierre_a(t_almacenamiento * a) {
	if (a->nodo) free(a->nodo);
	if (a->ip_puerto) free(a->ip_puerto);
	if (a->archivo_rg) free(a->archivo_rg);
	free(a);
}

/**
 * @NAME cierre_cn
 */
void cierre_cn(t_yama_carga_nodo * cn) {
	free(cn->nodo);
	free(cn);
}











/**	╔═════════════════════════════════════════════════════════════════════════════════════════╗
	║                                         CONSOLA                                         ║
	╚═════════════════════════════════════════════════════════════════════════════════════════╝ **/

const char * INFO_JOB_CMD = "info";

/**
 * @NAME yama_console
 */
void yama_console(void * unused) {
	char * input = NULL;
	char * input_c = NULL;
	char * command = NULL;
	char * param = NULL;
	size_t len = 0;
	ssize_t read;
	printf("\n[user@yama ~]$:");
	while ((read = getline(&input, &len, stdin)) != -1) {
		if (read > 0) {
			input[read-1] = '\0';
			input_c = string_duplicate(input);
			char * token = strtok(input_c, " ");
			if (token != NULL) command = token;
			token = strtok(NULL, " ");
			if (token != NULL) param = token;
			if (strcmp(command, INFO_JOB_CMD) == 0) {
				if (param == NULL || !is_number(param)) {
					printf("\nerror: illegal arguments");
					return;
				} else {
					pthread_mutex_lock(&tabla_estados);
					info_job(atoi(param));
					pthread_mutex_unlock(&tabla_estados);
				}
			} else {
				printf("\nerror: command not found");
			}
		}
		free(input_c);
		printf("\n[user@yama ~]$:");
	}
}

/**
 * @NAME info_job
 */
void info_job(int job_id) {

	bool existe = false;

	t_yama_job * job;
	t_yama_nodo_job * nodo_j;
	t_yama_transformacion * transf;

	int i = 0;
	int j;
	int k;
	while (i < (jobs->elements_count)) {
		job = (t_yama_job *) list_get(jobs, i);
		if ((job->job_id) == job_id) {
			existe = true;
			printf("\n    #job-id             #phase        #node                    #ip:port                          #status   #file-block   #node-block                        #t-status                                        #archivo-temp                                     #archivo-rl-temp   #designado                                          #archivo_rg");
			printf("\n   ________   ________________   __________   _________________________   ______________________________   ___________   ___________   ______________________________   __________________________________________________   __________________________________________________   __________   __________________________________________________");
			j = 0;
			while (j < (job->nodos->elements_count)) {
				nodo_j = (t_yama_nodo_job *) list_get((job->nodos), j);

				k = 0;
				while (k < (nodo_j->transformaciones->elements_count)) {
					transf = (t_yama_transformacion *) list_get((nodo_j->transformaciones), k);
					printf("\n   %8d", (job->job_id));
					print_etapa(job->etapa);
					printf(" | %10s", (nodo_j->nodo));
					printf(" | %25s", (nodo_j->ip_puerto));
					print_estado(nodo_j->estado);
					printf(" | %11d", (transf->bloque_archivo));
					printf(" | %11d", (transf->bloque_nodo));
					print_estado(transf->estado);
					printf(" | %50s", (transf->archivo_temp));
					printf(" | %50s", (nodo_j->archivo_rl_temp));
					printf(" | %10s", ((nodo_j->designado) ? "SI" : " "));
					printf(" | %50s", (nodo_j->archivo_rg));
					k++;
				}
				j++;
			}
		}
		i++;
	}

	if (!existe) {
		printf("\nerror: job id not exists.\nplease try again...");
	}

}

/**
 * @NAME print_etapa
 */
void print_etapa(int etapa) {
	switch(etapa){
	case TRANSFORMACION:
		printf(" | %16s", "TRANSFORMACION");
		break;
	case REDUCCION_LOCAL:
		printf(" | %16s", "REDUCCION LOCAL");
		break;
	case REDUCCION_GLOBAL:
		printf(" | %16s", "REDUCCION GLOBAL");
		break;
	case ALMACENAMIENTO:
		printf(" | %16s", "ALMACENAMIENTO");
		break;
	case FINALIZADO_OK:
		printf(" | %16s", "FINALIZADO OK");
		break;
	case FINALIZADO_ERROR:
		printf(" | %16s", "FINALIZADO ERROR");
		break;
	default:;
	}
}

/**
 * @NAME print_estado
 */
void print_estado(int estado) {
	switch(estado){
	case TRANSF_EN_PROCESO:
		printf(" | %30s", "TRANSF EN PROCESO");
		break;
	case TRANSF_OK:
		printf(" | %30s", "TRANSF OK");
		break;
	case TRANSF_ERROR:
		printf(" | %30s", "TRANSF ERROR");
		break;
	case TRANSF_ERROR_SIN_NODOS_DISP:
		printf(" | %30s", "TRANSF ERROR SIN NODOS DISP");
		break;
	case REDUC_LOCAL_EN_PROCESO:
		printf(" | %30s", "REDUC LOCAL EN PROCESO");
		break;
	case REDUC_LOCAL_OK:
		printf(" | %30s", "REDUC LOCAL OK");
		break;
	case REDUC_LOCAL_ERROR:
		printf(" | %30s", "REDUC LOCAL ERROR");
		break;
	case REDUC_GLOBAL_EN_PROCESO:
		printf(" | %30s", "REDUC GLOBAL EN PROCESO");
		break;
	case REDUC_GLOBAL_OK:
		printf(" | %30s", "REDUC GLOBAL OK");
		break;
	case REDUC_GLOBAL_ERROR:
		printf(" | %30s", "REDUC GLOBAL ERROR");
		break;
	case ALMACENAMIENTO_EN_PROCESO:
		printf(" | %30s", "ALMACENAMIENTO EN PROCESO");
		break;
	case ALMACENAMIENTO_OK:
		printf(" | %30s", "ALMACENAMIENTO OK");
		break;
	case ALMACENAMIENTO_ERROR:
		printf(" | %30s", "ALMACENAMIENTO ERROR");
		break;
	default:;
	}
}

/**
 * @NAME get_fs_error_message
 */
char * get_fs_error_message(int16_t error) {
	switch(error){
	case ENOTDIR:
		return "el directorio no existe en yamafs";
		break;
	case ENOENT:
		return "el directorio o archivo, no existe en yamafs";
		break;
	case CORRUPTED_FILE:
		return "archivo corrupto (¿nodos desconectados?)";
		break;
	case DISCONNECTED_SERVER:
		return "yamafs desconectado";
		break;
	default:;
	}
}

/**
 * @NAME get_result_message
 */
char * get_result_message(int result) {
	switch(result){
	case TRANSF_OK:
		return "transformación exitosa";
		break;
	case TRANSF_ERROR:
		return "error en transformación";
		break;
	case REDUC_LOCAL_OK:
		return "reducción local exitosa";
		break;
	case REDUC_LOCAL_ERROR:
		return "error en reducción local (sin replanificación)";
		break;
	case REDUC_GLOBAL_OK:
		return "reducción global exitosa";
		break;
	case REDUC_GLOBAL_ERROR:
		return "error en reducción global (sin replanificación)";
		break;
	case ALMACENAMIENTO_OK:
		return "almacenamiento exitoso";
		break;
	case ALMACENAMIENTO_ERROR:
		return "error en almacenamiento (sin replanificación)";
		break;
	default:;
	}
}

/**
 * @NAME is_number
 */
bool is_number(char * str) {
	int j = 0;
	while (j < strlen(str)) {
		if(str[j] > '9' || str[j] < '0') {
			return false;
		}
		j++;
	}
	return true;
}
