
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
t_list * carga_jobs;
t_list * tabla_de_estados;

#define	SOCKET_BACKLOG 						20
#define	CLOCK 								1
#define	WCLOCK								2

void yama_console(void *);
void signal_handler(int);
void registrar_resultado_t_bloque(int *, int, char *, int, int);
void registrar_resultado_rl(int *, int, char *, int);
void registrar_resultado_rg(int *, int, char *, int);
void registrar_resultado_a(int *, int, char *, int);
void registrar_error_job(int);
void reducir_carga_nodo(char *, int);
void reducir_carga_job(int, char *, int);
void recibir_solicitudes_master(void);
void procesar_nueva_solicitud(int *, char *);
void print_etapa(int);
void print_estado(int);
void posicionar_clock(void);
void planificar_etapa_reduccion_local_nodo(int *, int, char *);
void planificar_etapa_reduccion_global(int *, int);
void planificar_almacenamiento(int *, int, char *);
void liberar_cargas_job(int);
void liberar_carga_job(int, char *);
void inicio(void);
void info_job(int);
void info_job(int);
void incluir_nodos_para_balanceo(t_fs_metadata_file *);
void incluir_carga_job(int, char *);
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
void balanceo_de_carga(t_fs_file_block_metadata *, t_list *);
void aumentar_carga_nodo(char *, int);
void aumentar_carga_job(int, char *, int);
t_list * planificar_etapa_transformacion(t_fs_metadata_file *);
int registrar_resultado_transformacion_bloque(int *);
int registrar_resultado_reduccion_local(int *);
int registrar_resultado_reduccion_global(int *);
int registrar_resultado_almacenamiento(int *);
int pwl(char *);
int obtener_carga_job(int, char *);
int nueva_solicitud(int *);
int disponibilidad(char *);
int balanceo_de_carga_replanificacion(int, t_list *);
int atender_solicitud_master(int *);
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
	logger = log_create((yama_conf->log), "yama_log", true, LOG_LEVEL_TRACE);
}

/**
 * @NAME inicio
 */
void inicio(void) {
	tabla_de_estados = list_create();
	carga_nodos = list_create();
	carga_jobs = list_create();
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
	pthread_mutex_lock(&tabla_estados);
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
		atsm = registrar_resultado_reduccion_local(socket_cliente);
		break;
	case REGISTRAR_RES_ALMACENAMIENTO:
		atsm = registrar_resultado_almacenamiento(socket_cliente);
		break;
	default:;
	}
	pthread_mutex_unlock(&tabla_estados);
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
	procesar_nueva_solicitud(socket_cliente, (req->archivo));
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
		yama_planificacion_send_resp(socket_cliente, EXITO, TRANSFORMACION, new_job_id, planificados);

		list_destroy_and_destroy_elements(planificados, &cierre_t);
		list_destroy_and_destroy_elements((metadata->block_list), &cierre_bamd);
		free(metadata->path);
		free(metadata);
	} else {
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
	t_list * planificados = list_create();
	t_list * bloques_archivo = (metadata->block_list);
	t_fs_file_block_metadata * bloque_archivo_md;
	int i = 0;
	while (i < (bloques_archivo->elements_count)) {
		bloque_archivo_md = (t_fs_file_block_metadata *) list_get(bloques_archivo, i);
		balanceo_de_carga(bloque_archivo_md, planificados);
		i++;
	}
	return planificados;
}

/**
 * @NAME balanceo_de_carga
 */
void balanceo_de_carga(t_fs_file_block_metadata * bloque_archivo_md, t_list * planificados) {

	t_list * copias_bloque_md = (bloque_archivo_md->copies_list);
	t_fs_copy_block * copia_md;
	t_list * copias_bloque = list_create();
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
	t_transformacion * transformacion;
	t_yama_estado_bloque * estado_bloque;

	int clock_aux = clock_;
	for (;;) {

		carga_nodo = (t_yama_carga_nodo *) list_get(carga_nodos, clock_aux);
		index = 0;

		while (index < (copias_bloque->elements_count)) {

			copia = (t_yama_copia_bloque *) list_get(copias_bloque, index);

			if ((strcmp((carga_nodo->nodo), (copia->nodo)) == 0) && ((carga_nodo->disponibilidad) > 0)) {

				char * archivo_temp = string_new();
				string_append_with_format(&archivo_temp, "/tmp/J%d-%s-B%dETF", new_job_id, (copia->nodo), (copia->bloque_nodo));

				copia->planificada = true;

				transformacion = (t_transformacion *) malloc (sizeof(t_transformacion));
				transformacion->nodo = string_duplicate(copia->nodo);
				transformacion->ip_puerto = string_duplicate(copia->ip_puerto);
				transformacion->bloque = (copia->bloque_nodo);
				transformacion->bytes_ocupados = (bloque_archivo_md->size);
				transformacion->archivo_temporal = string_duplicate(archivo_temp);
				list_add(planificados, transformacion);

				estado_bloque = (t_yama_estado_bloque *) malloc (sizeof(t_yama_estado_bloque));
				estado_bloque->job_id = new_job_id;
				estado_bloque->etapa = TRANSFORMACION;
				estado_bloque->bloque_archivo = (bloque_archivo_md->file_block);
				estado_bloque->nodo = string_duplicate(copia->nodo);
				estado_bloque->ip_puerto = string_duplicate(copia->ip_puerto);
				estado_bloque->bloque_nodo = (copia->bloque_nodo);
				estado_bloque->bytes_ocupados = (bloque_archivo_md->size);
				estado_bloque->estado = TRANSF_EN_PROCESO;
				estado_bloque->archivo_temp = string_duplicate(archivo_temp);
				estado_bloque->copias = copias_bloque;
				list_add(estado_bloque, tabla_de_estados);

				free(archivo_temp);

				(carga_nodo->disponibilidad)--;
				incluir_carga_job(new_job_id, (copia->nodo));
				aumentar_carga_job(new_job_id,(copia->nodo), 1);

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
	registrar_resultado_t_bloque(socket_cliente, (req->job_id), (req->nodo), (req->bloque), (req->resultado_t));
	if ((req->resultado_t) == TRANSF_OK)
		chequear_inicio_reduccion_local(socket_cliente, (req->job_id), (req->nodo));
	free(req->nodo);
	free(req);
	return EXITO;
}

/**
 * @NAME registrar_resultado_t_bloque
 */
void registrar_resultado_t_bloque(int * socket_cliente, int job_id, char * nodo, int bloque, int resultado) {
	t_yama_estado_bloque * estado_bloque;
	int index = 0;
	while (index < (tabla_de_estados->elements_count)) {
		estado_bloque = (t_yama_estado_bloque *) list_get(tabla_de_estados, index);
		if (((estado_bloque->job_id) == job_id) && (strcmp((estado_bloque->nodo), nodo) == 0) && ((estado_bloque->bloque_nodo) == bloque)) {
			estado_bloque->estado = resultado;
			if ((estado_bloque->etapa) == FINALIZADO_ERROR)	 {
				// el job finalizo antes,
				// error previo en otra transformacion perteneciente al mismo job
				reducir_carga_job(job_id, nodo, 1);
			} else if ((estado_bloque->etapa) == TRANSFORMACION) {
				if (resultado == TRANSF_ERROR) {
					// replanificacion transformacion
					reducir_carga_job(job_id, nodo, 1);
					usleep(1000 * (yama_conf->retardo_plan)); // delay planificacion
					t_list * planificados = list_create();
					int bcr = balanceo_de_carga_replanificacion(index, planificados);
					if (bcr == EXITO) {
						yama_planificacion_send_resp(socket_cliente, EXITO, TRANSFORMACION, job_id, planificados);
					} else {
						registrar_error_job(job_id);
						yama_planificacion_send_resp(socket_cliente, ERROR, FINALIZADO_ERROR, job_id, NULL);
					}
					list_destroy_and_destroy_elements(planificados, &cierre_t);
				}
			}
			return;
		}
		index++;
	}
}

/**
 * @NAME balanceo_de_carga_replanificacion
 */
int balanceo_de_carga_replanificacion(int pos_tabla_estados, t_list * planificados) {

	t_yama_estado_bloque * estado_bloque = (t_yama_estado_bloque *) list_get(tabla_de_estados, pos_tabla_estados);

	t_list * copias_bloque = (estado_bloque->copias);
	t_yama_copia_bloque * copia;

	bool sin_nodos_disponibles = true;
	int index = 0;
	while (index < (copias_bloque->elements_count)) {
		copia = (t_yama_copia_bloque *) list_get(copias_bloque, index);
		if (!(copia->planificada)) {
			sin_nodos_disponibles = false;
			break;
		}
	}

	if (sin_nodos_disponibles) {
		estado_bloque->estado = TRANSF_ERROR_SIN_NODOS_DISP;
		return FINALIZADO_ERROR;
	}

	int clock_aux = clock_;

	t_yama_carga_nodo * carga_nodo;
	t_transformacion * transformacion;

	for (;;) {

		carga_nodo = (t_yama_carga_nodo *) list_get(carga_nodos, clock_aux);
		index = 0;

		while (index < (copias_bloque->elements_count)) {

			copia = (t_fs_copy_block *) list_get(copias_bloque, index);

			if (!(copia->planificada) && (strcmp((carga_nodo->nodo), (copia->nodo)) == 0) && ((carga_nodo->disponibilidad) > 0)) {

				copia->planificada = true;

				char * archivo_temp = string_new();
				string_append_with_format(&archivo_temp, "/tmp/J%d-%s-B%d-transf", (estado_bloque->job_id), (copia->nodo), (copia->bloque_nodo));

				transformacion = (t_transformacion *) malloc (sizeof(t_transformacion));
				transformacion->nodo = string_duplicate(copia->nodo);
				transformacion->ip_puerto = string_duplicate(copia->ip_puerto);
				transformacion->bloque = (copia->bloque_nodo);
				transformacion->bytes_ocupados = (estado_bloque->bytes_ocupados);
				transformacion->archivo_temporal = string_duplicate(archivo_temp);
				list_add(planificados, transformacion);

				estado_bloque->etapa = TRANSFORMACION;
				free(estado_bloque->nodo);
				estado_bloque->nodo = string_duplicate(copia->nodo);
				free(estado_bloque->ip_puerto);
				estado_bloque->ip_puerto= string_duplicate(copia->ip_puerto);
				estado_bloque->bloque_nodo = (copia->bloque_nodo);
				estado_bloque->estado = TRANSF_EN_PROCESO;
				free(estado_bloque->archivo_temp);
				estado_bloque->archivo_temp = string_duplicate(archivo_temp);

				free(archivo_temp);

				(carga_nodo->disponibilidad)--;
				incluir_carga_job((estado_bloque->job_id),(copia->nodo));
				aumentar_carga_job((estado_bloque->job_id),(copia->nodo), 1);

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
	t_yama_estado_bloque * estado_bloque;
	int index = 0;
	while (index < (tabla_de_estados->elements_count)) {
		estado_bloque = (t_yama_estado_bloque *) list_get(tabla_de_estados, index);
		if (((estado_bloque->job_id) == job_id) && (strcmp((estado_bloque->nodo), nodo) == 0)) {
			if (((estado_bloque->etapa) != TRANSFORMACION) && ((estado_bloque->estado) != TRANSF_OK)) {
				planificar_rl = false;
				break;
			}
		}
		index++;
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

	t_yama_estado_bloque * estado_bloque;
	t_list * planificados = list_create();
	t_red_local * red_local;

	char * archivo_rl_temp = string_new();
	string_append_with_format(&archivo_rl_temp, "/tmp/J%d-%s-r-local", job_id, nodo);

	int index = 0;
	while (index < (tabla_de_estados->elements_count)) {
		estado_bloque = (t_yama_estado_bloque *) list_get(tabla_de_estados, index);
		if (((estado_bloque->job_id) == job_id) && (strcmp((estado_bloque->nodo), nodo) == 0)) {

			if (!red_local) {
				red_local = (t_red_local *) malloc (sizeof(t_red_local));
				red_local->nodo = string_duplicate((estado_bloque->nodo));
				red_local->ip_puerto = string_duplicate((estado_bloque->ip_puerto));
				red_local->archivo_rl_temp = string_duplicate(archivo_rl_temp);
				red_local->archivos_temp = string_new();
				string_append(&(red_local->archivos_temp), (estado_bloque->archivo_temp));
				aumentar_carga_job(job_id, nodo, 1);
			} else {
				string_append_with_format(&(red_local->archivos_temp), ";%s", (estado_bloque->archivo_temp));
			}

			estado_bloque->etapa = REDUCCION_LOCAL;
			estado_bloque->estado = REDUC_LOCAL_EN_PROCESO;
			estado_bloque->archivo_rl_temp = string_duplicate(archivo_rl_temp);

		}
		index++;
	}
	free(archivo_rl_temp);

	list_add(planificados, red_local);
	yama_planificacion_send_resp(socket_cliente, EXITO, REDUCCION_LOCAL, job_id, planificados);

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
	registrar_resultado_rl(socket_cliente, (req->job_id), (req->nodo), (req->resultado));
	if ((req->resultado) == REDUC_LOCAL_OK)
		chequear_inicio_reduccion_global(socket_cliente, (req->job_id));
	free(req->nodo);
	free(req);
	return EXITO;
}

/**
 * @NAME registrar_resultado_rl
 */
void registrar_resultado_rl(int * socket_cliente, int job_id, char * nodo, int resultado) {
	bool carga_liberada = false;
	bool error_procesado = false;
	t_yama_estado_bloque * estado_bloque;
	int index = 0;
	while (index < (tabla_de_estados->elements_count)) {
		estado_bloque = (t_yama_estado_bloque *) list_get(tabla_de_estados, index);
		if (((estado_bloque->job_id) == job_id) && (strcmp((estado_bloque->nodo), nodo) == 0)) {
			estado_bloque->estado = resultado;
			if (((estado_bloque->etapa) == FINALIZADO_ERROR) && (!carga_liberada)) {
				// el job finalizo antes
				// error previo en reduccion local realizada en otro nodo para el mismo job
				// libero carga en el nodo, solo una vez
				liberar_carga_job(job_id, nodo);
				carga_liberada = true;
			} else if ((estado_bloque->etapa) == REDUCCION_LOCAL) {
				if (resultado == REDUC_LOCAL_ERROR && (!error_procesado)) {
					// reduccion local con error, no hay replanificacion
					liberar_carga_job(job_id, nodo);
					registrar_error_job(job_id);
					yama_planificacion_send_resp(socket_cliente, ERROR, FINALIZADO_ERROR, job_id, NULL);
					error_procesado = true;
				}
			}
		}
		index++;
	}
}

/**
 * @NAME chequear_inicio_reduccion_global
 */
void chequear_inicio_reduccion_global(int * socket_cliente, int job_id) {
	bool planificar_rg = true;
	t_yama_estado_bloque * estado_bloque;
	int index = 0;
	while (index < (tabla_de_estados->elements_count)) {
		estado_bloque = (t_yama_estado_bloque *) list_get(tabla_de_estados, index);
		if ((estado_bloque->job_id) == job_id) {
			if (((estado_bloque->etapa) != REDUCCION_LOCAL) && ((estado_bloque->estado) != REDUC_LOCAL_OK)) {
				planificar_rg = false;
				break;
			}
		}
		index++;
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

	t_yama_estado_bloque * estado_bloque;
	t_yama_estado_bloque * estado_nodo_designado;
	t_yama_carga_nodo * carga_nodo;
	t_yama_carga_nodo * carga_nodo_designado;

	int cant_red_temp = 0;
	int i = 0;
	int j;
	while (i < (carga_nodos->elements_count)) {
		carga_nodo = (t_yama_carga_nodo *) list_get(carga_nodos, i);
		j = 0;
		while (j < (tabla_de_estados->elements_count)) {
			estado_bloque = (t_yama_estado_bloque *) list_get(tabla_de_estados, j);
			if (((estado_bloque->job_id) == job_id) && (strcmp((estado_bloque->nodo), (carga_nodo->nodo)) == 0)) {
				cant_red_temp++;
				if ((!carga_nodo_designado) || ((carga_nodo->wl) < (carga_nodo_designado->wl))) {
					carga_nodo_designado = carga_nodo;
					estado_nodo_designado = estado_bloque;
				}
				break;
			}
			j++;
		}
		i++;
	}

	aumentar_carga_job(job_id, (carga_nodo_designado->nodo), ((cant_red_temp / 2) + (((cant_red_temp % 2) > 0) ? 1 : 0)));

	char * archivo_rg = string_new();
	string_append_with_format(&archivo_rg, "/tmp/J%d-final", job_id);

	t_list * planificados = list_create();
	t_red_global * red_global;

	i = 0;
	while (i < (carga_nodos->elements_count)) {
		carga_nodo = (t_yama_carga_nodo *) list_get(carga_nodos, i);
		j = 0;
		while (j < (tabla_de_estados->elements_count)) {
			estado_bloque = (t_yama_estado_bloque *) list_get(tabla_de_estados, j);
			if (((estado_bloque->job_id) == job_id) && (strcmp((estado_bloque->nodo), (carga_nodo->nodo)) == 0)) {

				red_global = (t_red_global *) malloc(sizeof(t_red_global));
				red_global->nodo = string_duplicate(estado_bloque->nodo);
				red_global->ip_puerto = string_duplicate(estado_bloque->ip_puerto);
				red_global->archivo_rl_temp = string_duplicate(estado_bloque->archivo_rl_temp);

				if (strcmp((estado_bloque->nodo), (estado_nodo_designado->nodo)) == 0) {
					red_global->designado = true;
					red_global->archivo_rg = string_duplicate(archivo_rg);
				} else {
					red_global->designado = false;
					red_global->archivo_rg = string_new();
				}

				list_add(planificados, red_global);
				break;
			}
			j++;
		}
		i++;
	}

	i = 0;
	while (i < (tabla_de_estados->elements_count)) {
		estado_bloque = (t_yama_estado_bloque *) list_get(tabla_de_estados, j);
		if ((estado_bloque->job_id) == job_id) {
			estado_bloque->etapa = REDUCCION_GLOBAL;
			estado_bloque->estado = REDUC_GLOBAL_EN_PROCESO;
			if (strcmp((estado_bloque->nodo), (estado_nodo_designado->nodo)) == 0) {
				estado_bloque->designado = true;
				estado_bloque->archivo_rg = string_duplicate(archivo_rg);
			} else {
				estado_bloque->designado = false;
				estado_bloque->archivo_rg = string_new();
			}
		}
		i++;
	}

	free(archivo_rg);
	yama_planificacion_send_resp(socket_cliente, EXITO, REDUCCION_GLOBAL, job_id, planificados);
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
	registrar_resultado_rg(socket_cliente, (req->job_id), (req->nodo), (req->resultado));
	if ((req->resultado) == REDUC_GLOBAL_OK) {
		usleep(1000 * (yama_conf->retardo_plan)); // delay planificacion
		planificar_almacenamiento(socket_cliente, (req->job_id), (req->nodo));
	}
	free(req->nodo);
	free(req);
	return EXITO;
}

/**
 * @NAME registrar_resultado_rg
 */
void registrar_resultado_rg(int * socket_cliente, int job_id, char * nodo, int resultado) {
	bool error_procesado = false;
	t_yama_estado_bloque * estado_bloque;
	int index = 0;
	while (index < (tabla_de_estados->elements_count)) {
		estado_bloque = (t_yama_estado_bloque *) list_get(tabla_de_estados, index);
		if (((estado_bloque->job_id) == job_id) && (strcmp((estado_bloque->nodo), nodo) == 0)) {
			estado_bloque->estado = resultado;
			if ((estado_bloque->etapa) == REDUCCION_GLOBAL) {
				if ((resultado == REDUC_GLOBAL_ERROR) && (!error_procesado)) {
					// reduccion global con error, no hay replanificacion
					liberar_cargas_job(job_id);
					registrar_error_job(job_id);
					yama_planificacion_send_resp(socket_cliente, ERROR, FINALIZADO_ERROR, job_id, NULL);
					error_procesado = true;
				}
			}
		}
		index++;
	}
}

/**
 * @NAME planificar_almacenamiento
 */
void planificar_almacenamiento(int * socket_cliente, int job_id, char * nodo) {

	t_yama_estado_bloque * estado_bloque;
	t_list * planificados = list_create();
	t_almacenamiento * almacenamiento;
	bool planificado = false;

	int index = 0;
	while (index < (tabla_de_estados->elements_count)) {
		estado_bloque = (t_yama_estado_bloque *) list_get(tabla_de_estados, index);
		if (((estado_bloque->job_id) == job_id) && (strcmp((estado_bloque->nodo), nodo) == 0)) {
			if (!planificado) {
				almacenamiento = (t_almacenamiento *) malloc (sizeof(t_almacenamiento));
				almacenamiento->nodo = string_duplicate(estado_bloque->nodo);
				almacenamiento->ip_puerto = string_duplicate(estado_bloque->ip_puerto);
				almacenamiento->archivo_rg = string_duplicate(estado_bloque->archivo_rg);
			}
			estado_bloque->etapa = ALMACENAMIENTO;
			estado_bloque->estado = ALMACENAMIENTO_EN_PROCESO;
		}
		index++;
	}

	list_add(planificados, almacenamiento);
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
	registrar_resultado_a(socket_cliente, (req->job_id), (req->nodo), (req->resultado));
	free(req->nodo);
	free(req);
	return EXITO;
}

/**
 * @NAME registrar_resultado_a
 */
void registrar_resultado_a(int * socket_cliente, int job_id, char * nodo, int resultado) {
	bool error_procesado = false;
	t_yama_estado_bloque * estado_bloque;
	int index = 0;
	while (index < (tabla_de_estados->elements_count)) {
		estado_bloque = (t_yama_estado_bloque *) list_get(tabla_de_estados, index);
		if (((estado_bloque->job_id) == job_id) && (strcmp((estado_bloque->nodo), nodo) == 0)) {
			estado_bloque->estado = resultado;
			if ((estado_bloque->etapa) == ALMACENAMIENTO) {
				if ((resultado == ALMACENAMIENTO_ERROR) && (!error_procesado)) {
					// reduccion global con error, no hay replanificacion
					liberar_cargas_job(job_id);
					registrar_error_job(job_id);
					yama_planificacion_send_resp(socket_cliente, ERROR, FINALIZADO_ERROR, job_id, NULL);
					error_procesado = true;
				} else {
					estado_bloque->etapa = FINALIZADO_OK;
				}
			}
		}
		index++;
	}
}



/**	╔════════════════════════════════════════════════════════════════════════════════════════╗
	║                                         HELPER                                         ║
	╚════════════════════════════════════════════════════════════════════════════════════════╝ **/

/**
 * @NAME incluir_carga_job
 */
void incluir_carga_job(int job_id, char * nodo) {
	bool existe_job = false;
	bool existe_nodo = false;
	t_yama_carga_job * carga_job;
	t_yama_carga_nodo * carga_nodo;
	int i = 0;
	int j;
	while (i < (carga_jobs->elements_count)) {
		carga_job = (t_yama_carga_job *) list_get(carga_jobs, i);
		if (carga_job->job_id == job_id) {
			existe_job = true;
			t_list * cargas = (carga_job->cargas);
			j = 0;
			while (j < (cargas->elements_count)) {
				carga_nodo = (t_yama_carga_nodo *) list_get(cargas, j);
				if (strcmp((carga_nodo->nodo), nodo) == 0) {
					existe_nodo = true;
					break;
				}
				j++;
			}
			break;
		}
		i++;
	}
	if (!existe_job) {
		carga_job = (t_yama_carga_job *) malloc(sizeof(t_yama_carga_job));
		carga_job->job_id = job_id;
		carga_job->cargas = list_create();
		list_add(carga_jobs, carga_job);
	} else {
		carga_job = (t_yama_carga_job *) list_get(carga_jobs, i);
	}
	if (!existe_nodo) {
		carga_nodo = (t_yama_carga_nodo *) malloc(sizeof(t_yama_carga_nodo));
		carga_nodo->disponibilidad = 0;
		carga_nodo->nodo = string_duplicate(nodo);
		carga_nodo->wl = 0;
		carga_nodo->wl_total = 0;
		list_add((carga_job->cargas), carga_nodo);
	}
}

/**
 * @NAME aumentar_carga_job
 */
void aumentar_carga_job(int job_id, char * nodo, int carga) {
	t_yama_carga_job * carga_job;
	t_yama_carga_nodo * carga_nodo;
	int i = 0;
	int j;
	while (i < (carga_jobs->elements_count)) {
		carga_job = (t_yama_carga_job *) list_get(carga_jobs, i);
		if (carga_job->job_id == job_id) {
			t_list * cargas = (carga_job->cargas);
			j = 0;
			while (j < (cargas->elements_count)) {
				carga_nodo = (t_yama_carga_nodo *) list_get(cargas, j);
				if (strcmp((carga_nodo->nodo), nodo) == 0) {
					aumentar_carga_nodo(nodo, carga);
					carga_nodo->wl += carga;
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
 * @NAME reducir_carga_job
 */
void reducir_carga_job(int job_id, char * nodo, int carga) {
	t_yama_carga_job * carga_job;
	t_yama_carga_nodo * carga_nodo;
	int i = 0;
	int j;
	while (i < (carga_jobs->elements_count)) {
		carga_job = (t_yama_carga_job *) list_get(carga_jobs, i);
		if (carga_job->job_id == job_id) {
			t_list * cargas = (carga_job->cargas);
			j = 0;
			while (j < (cargas->elements_count)) {
				carga_nodo = (t_yama_carga_nodo *) list_get(cargas, j);
				if (strcmp((carga_nodo->nodo), nodo) == 0) {
					reducir_carga_nodo(nodo, carga);
					carga_nodo->wl -= carga;
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
 * @NAME obtener_cargar_job
 */
int obtener_carga_job(int job_id, char * nodo) {
	t_yama_carga_job * carga_job;
	t_yama_carga_nodo * carga_nodo;
	int i = 0;
	int j;
	while (i < (carga_jobs->elements_count)) {
		carga_job = (t_yama_carga_job *) list_get(carga_jobs, i);
		if (carga_job->job_id == job_id) {
			t_list * cargas = (carga_job->cargas);
			j = 0;
			while (j < (cargas->elements_count)) {
				carga_nodo = (t_yama_carga_nodo *) list_get(cargas, j);
				if (strcmp((carga_nodo->nodo), nodo) == 0) {
					return (carga_nodo->wl);
				}
				j++;
			}
			break;
		}
		i++;
	}
	return 0;
}

/**
 * @NAME liberar_carga_job
 */
void liberar_carga_job(int job_id, char * nodo) {
	t_yama_carga_job * carga_job;
	t_yama_carga_nodo * carga_nodo;
	int i = 0;
	int j;
	while (i < (carga_jobs->elements_count)) {
		carga_job = (t_yama_carga_job *) list_get(carga_jobs, i);
		if (carga_job->job_id == job_id) {
			t_list * cargas = (carga_job->cargas);
			j = 0;
			while (j < (cargas->elements_count)) {
				carga_nodo = (t_yama_carga_nodo *) list_get(cargas, j);
				if (strcmp((carga_nodo->nodo), nodo) == 0) {
					carga_nodo = (t_yama_carga_job *) list_remove(cargas, j);
					reducir_carga_nodo((carga_nodo->nodo), (carga_nodo->wl));
					free(carga_nodo->nodo);
					free(carga_nodo);
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
 * @NAME liberar_cargas_job
 */
void liberar_cargas_job(int job_id) {
	t_yama_carga_job * carga_job;
	t_yama_carga_nodo * carga_nodo;
	int i = 0;
	int j;
	while (i < (carga_jobs->elements_count)) {
		carga_job = (t_yama_carga_job *) list_get(carga_jobs, i);
		if (carga_job->job_id == job_id) {
			carga_job = (t_yama_carga_job *) list_remove(carga_jobs, i);
			t_list * cargas = (carga_job->cargas);
			j = 0;
			while (j < (cargas->elements_count)) {
				carga_nodo = (t_yama_carga_nodo *) list_get(cargas, j);
				reducir_carga_nodo((carga_nodo->nodo), (carga_nodo->wl));
				j++;
			}
			list_destroy_and_destroy_elements(cargas, &cierre_cn);
			free(carga_job);
			return;
		}
		i++;
	}
}

/**
 * @NAME aumentar_carga_nodo
 */
void aumentar_carga_nodo(char * nodo, int carga) {
	t_yama_carga_nodo * carga_nodo;
	int index = 0;
	while (index < (carga_nodos->elements_count)) {
		carga_nodo = (t_yama_carga_nodo *) list_get(carga_nodos, index);
		if (strcmp((carga_nodo->nodo), nodo) == 0) {
			(carga_nodo->wl) += carga;
			(carga_nodo->wl_total) += carga;
			return;
		}
		index++;
	}
}

/**
 * @NAME reducir_carga_nodo
 */
void reducir_carga_nodo(char * nodo, int carga) {
	t_yama_carga_nodo * carga_nodo;
	int index = 0;
	while (index < (carga_nodos->elements_count)) {
		carga_nodo = (t_yama_carga_nodo *) list_get(carga_nodos, index);
		if (strcmp((carga_nodo->nodo), nodo) == 0) {
			(carga_nodo->wl) -= carga;
			return;
		}
		index++;
	}
}

/**
 * @NAME registrar_error_job
 */
void registrar_error_job(int job_id) {
	t_yama_estado_bloque * estado_bloque;
	int index = 0;
	while (index < (tabla_de_estados->elements_count)) {
		estado_bloque = (t_yama_estado_bloque *) list_get(tabla_de_estados, index);
		if ((estado_bloque->job_id) == job_id)
			estado_bloque->etapa = FINALIZADO_ERROR;
		index++;
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
	free(copia_md->node);
	free(copia_md->ip_port);
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
		free(input);
		printf("\n[user@yama ~]$:");
	}
}

/**
 * @NAME info_job
 */
void info_job(int job_id) {

	bool existe = false;
	printf("\n    #job-id             #phase       #file-block   		  #node                    #ip:port   #node-block                          #status    											              #archivo_temp    															    #archivo_rl_temp   #designado                                          #archivo_rg");
	printf("\n   ________   ________________   _______________   __________   _________________________   ___________   ______________________________   __________________________________________________   __________________________________________________   __________   __________________________________________________");

	t_yama_estado_bloque * estado_bloque;
	int i = 0;
	while (i < (tabla_de_estados->elements_count)) {
		estado_bloque = (t_yama_estado_bloque *) list_get(tabla_de_estados, i);
		if ((estado_bloque->job_id) == job_id) {
			bool existe = true;
			printf("\n   %8d", (estado_bloque->job_id));
			print_etapa(estado_bloque->etapa);
			printf(" | %15d", (estado_bloque->bloque_archivo));
			printf(" | %10s", (estado_bloque->nodo));
			printf(" | %25s", (estado_bloque->ip_puerto));
			printf(" | %11d", (estado_bloque->bloque_nodo));
			print_estado(estado_bloque->estado);
			printf(" | %50s", (estado_bloque->archivo_temp));
			printf(" | %50s", (estado_bloque->archivo_rl_temp));
			printf(" | %10s", ((estado_bloque->designado) ? "SI" : " "));
			printf(" | %50s", (estado_bloque->archivo_rg));
		}
		i++;
	}
	if (!existe) {
		printf("\n error: job id not exists.\nplease try again...");
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
