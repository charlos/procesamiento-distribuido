
#include <commons/collections/list.h>
#include <commons/config.h>
#include <commons/log.h>
#include <shared-library/file-system-prot.h>
#include <shared-library/socket.h>
#include <shared-library/yama-prot.h>
#include <stdio.h>
#include <stdlib.h>
#include "yama.h"

t_yama_conf * yama_conf;
t_log * log;
int fs_socket;
int new_job_id = 0;
int algoritmo;

int clock;
t_list * carga_nodos;
t_list * tabla_de_estados;

#define	SOCKET_BACKLOG 						20
#define	CLOCK 								1
#define	WCLOCK								2

void registrar_resultado_t_bloque(int *, int, char *, int, int);
void registrar_resultado_rl(int *, int, char *, int);
void registrar_error_job(int);
void reducir_carga_nodo(char *);
void reducir_carga_nodo_rl(int, char *);
void recibir_solicitudes_master(void);
void procesar_nueva_solicitud(int *, char *);
void posicionar_clock(void);
void planificar_etapa_reduccion_local_nodo(int *, int, char *);
void planificar_etapa_reduccion_global(int *, int);
void inicio(void);
void incluir_nodos_para_balanceo(t_fs_metadata_file *);
void crear_log(void);
void cierre_t(t_transformacion *);
void cierre_rl(t_red_local *);
void cierre_rg(t_red_global *);
void cierre_cmd(t_fs_copy_block *);
void cierre_bamd(t_fs_file_block_metadata *);
void chequear_inicio_reduccion_local(int *, int, char *);
void chequear_inicio_reduccion_global(int *, int);
void cargar_configuracion(void);
void calcular_disponibilidad_nodos(void);
void balanceo_de_carga(t_fs_file_block_metadata *, t_list *);
t_list * planificar_etapa_transformacion(t_fs_metadata_file *);
int registrar_resultado_transformacion_bloque(int *);
int registrar_resultado_reduccion_local(int *);
int pwl(char *);
int nueva_solicitud(int *);
int disponibilidad(char *);
int balanceo_de_carga_replanificacion(int, t_list *);
int atender_solicitud_master(int *);

int main(void) {
	cargar_configuracion();
	crear_log();
	inicio();

	fs_socket = connect_to_socket((yama_conf->fs_ip), (yama_conf->fs_puerto));
	int resp = fs_handshake(fs_socket, YAMA, NULL, NULL, NULL, log);
	if (resp != SUCCESS) {
		log_error(log, "error al conectarse a yama-fs");
		if (resp == UNSTEADY_FS) {
			log_error(log, "yama-fs no estable");
		} else {
			log_error(log, "yama-fs cod. error: %d", resp);
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
	yama_conf->fs_ip = config_get_string_value(conf, "FS_IP");
	yama_conf->fs_puerto = config_get_string_value(conf, "FS_PUERTO");
	yama_conf->retardo_plan = config_get_int_value(conf, "RETARDO_PLANIFICACION");
	yama_conf->algoritmo = config_get_string_value(conf, "ALGORITMO_BALANCEO");
	yama_conf->disp_base = config_get_int_value(conf, "DISP_BASE");
	yama_conf->log = config_get_string_value(conf, "LOG");
}

/**
 * @NAME crear_log
 */
void crear_log(void) {
	log = log_create((yama_conf->log), "yama_log", false, LOG_LEVEL_TRACE);
}

/**
 * @NAME inicio
 */
void inicio(void) {
	tabla_de_estados = list_create();
	carga_nodos = list_create();
	algoritmo = (strcmp((yama_conf->algoritmo), "CLOCK") == 0) ? CLOCK : WCLOCK;
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
						log_error(log, "Error al aceptar conexion");
					} else {
						log_trace(log, "Nueva conexion: socket %d", nuevaConexion);
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
	int cod_operacion = yama_recv_cod_operacion(socket_cliente, log);
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
		// TODO
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
	t_yama_nueva_solicitud_req * req = yama_nueva_solicitud_recv_req(socket_cliente, log);
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

	t_fs_get_md_file_resp * resp = fs_get_metadata_file(fs_socket, archivo, log);

	if ((resp->exec_code) == EXITO) {
		t_fs_metadata_file * metadata = (resp->metadata_file);

		new_job_id++;

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
			clock = index;
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

	int clock_aux = clock;
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

				(carga_nodo->wl)++;
				(carga_nodo->wl_total)++;
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

				clock = clock_aux;
				return;
			}
			index++;
		}

		clock_aux++; // avanzo clock
		if (clock_aux >= (carga_nodos->elements_count))
			clock_aux = 0;

		if (clock_aux == clock) {
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
	t_yama_reg_resultado_t_req * req = yama_registrar_resultado_t_recv_req(socket_cliente, log);
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
			if ((estado_bloque->etapa) == FINALIZADO_ERROR)	 {
				// el job finalizo antes, por un error previo
				// perteneciente a otra transformacion perteneciente al mismo job
				reducir_carga_nodo(nodo);
			} else if ((estado_bloque->etapa) == TRANSFORMACION) {
				if (resultado == TRANSF_ERROR) {
					// replanificacion transformacion
					reducir_carga_nodo(nodo);
					t_list * planificados = list_create();
					if (balanceo_de_carga_replanificacion(index, planificados) == EXITO) {
						yama_planificacion_send_resp(socket_cliente, EXITO, TRANSFORMACION, job_id, planificados);
					} else {
						registrar_error_job(job_id);
						yama_planificacion_send_resp(socket_cliente, ERROR, FINALIZADO_ERROR, job_id, NULL);
					}
					list_destroy_and_destroy_elements(planificados, &cierre_t);
				} else if (resultado == REDUC_LOCAL_OK) {
					estado_bloque->estado = resultado;
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

	int clock_aux = clock;

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

				(carga_nodo->wl)++;
				(carga_nodo->wl_total)++;
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
				clock = clock_aux;
				return EXITO;
			}
			index++;
		}

		clock_aux++; // avanzo clock
		if (clock_aux >= (carga_nodos->elements_count))
			clock_aux = 0;

		if (clock_aux == clock) {
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

	if (planificar_rl)
		planificar_etapa_reduccion_local_nodo(socket_cliente, job_id, nodo);

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
	t_yama_reg_resultado_rl_req * req = yama_registrar_resultado_rl_recv_req(socket_cliente, log);
	if ((req->exec_code) == CLIENTE_DESCONECTADO) {
		free(req);
		return CLIENTE_DESCONECTADO;
	}
	registrar_resultado_rl(socket_cliente, (req->job_id), (req->nodo), (req->resultado_rl));
	if ((req->resultado_rl) == REDUC_LOCAL_OK)
		chequear_inicio_reduccion_global(socket_cliente, (req->job_id));
	free(req->nodo);
	free(req);
	return EXITO;
}

/**
 * @NAME registrar_resultado_rl
 */
void registrar_resultado_rl(int * socket_cliente, int job_id, char * nodo, int resultado) {
	t_yama_estado_bloque * estado_bloque;
	int index = 0;
	while (index < (tabla_de_estados->elements_count)) {
		estado_bloque = (t_yama_estado_bloque *) list_get(tabla_de_estados, index);
		if (((estado_bloque->job_id) == job_id) && (strcmp((estado_bloque->nodo), nodo) == 0)) {

			if ((estado_bloque->etapa) == FINALIZADO_ERROR) {

				// el job finalizo antes,
				// por un error previo de alguna reduccion local realizada en otro nodo para el mismo job
				// reducir carga nodo, por cada transformación incluida en la reduccion local
				reducir_carga_nodo_rl(job_id, nodo);

			} else if ((estado_bloque->etapa) == REDUCCION_LOCAL) {
				if (resultado == REDUC_LOCAL_ERROR) {
					// reduccion local con error, no hay replanificacion
					reducir_carga_nodo_rl(job_id, nodo);
					registrar_error_job(job_id);
					yama_planificacion_send_resp(socket_cliente, ERROR, FINALIZADO_ERROR, job_id, NULL);
				} else if (resultado == TRANSF_OK) {
					estado_bloque->estado = resultado;
				}
			}
			return;
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

	if (planificar_rg)
		planificar_etapa_reduccion_global(socket_cliente, job_id);
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

	int carga = (cant_red_temp / 2) + (((cant_red_temp % 2) > 0) ? 1 : 0);
	(carga_nodo_designado->wl) += carga;
	(carga_nodo_designado->wl_total) += carga;

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



















/**
 * @NAME reducir_carga_nodo
 */
void reducir_carga_nodo(char * nodo) {
	t_yama_carga_nodo * carga_nodo;
	int j = 0;
	while (j < (carga_nodos->elements_count)) {
		carga_nodo = (t_yama_carga_nodo *) list_get(carga_nodos, j);
		if (strcmp((carga_nodo->nodo), nodo) == 0) {
			(carga_nodo->wl)--;
			return;
		}
		j++;
	}
}

/**
 * @NAME reducir_carga_nodo_rl
 */
void reducir_carga_nodo_rl(int job_id, char * nodo) {
	t_yama_estado_bloque * estado_bloque;
	int index = 0;
	while (index < (tabla_de_estados->elements_count)) {
		estado_bloque = (t_yama_estado_bloque *) list_get(tabla_de_estados, index);
		if (((estado_bloque->job_id) == job_id) && (strcmp((estado_bloque->nodo), nodo) == 0)) {
			reducir_carga_nodo(estado_bloque->nodo);
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
