
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


void cargar_configuracion(void);
void crear_log(void);
void inicio(void);
void recibir_solicitudes_master(void);
int atender_solicitud_master(int *);
int nueva_solicitud(int *);
void procesar_nueva_solicitud(int *, char *);
void cierre(void *);
void cierre_t(t_transformacion *);
void cierre_rl(t_red_local *);
void incluir_nodos_para_balanceo(t_fs_metadata_file *);
void calcular_disponibilidad_nodos(void);
int disponibilidad(char *);
int pwl(char *);
void posicionar_clock(void);
t_list * planificar_etapa_transformacion(t_fs_metadata_file *);
void balanceo_de_carga(t_list *, int, t_list *);
int registrar_resultado_transformacion_bloque(int *);
void registrar_resultado_t_bloque(int *, int, char *, int, int);
void chequear_inicio_reduccion_local(int *, int, char *);
void planificar_etapa_reduccion_local_nodo(int *, int, char *);



int balanceo_de_carga_replanificacion(int, t_list *);

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
	yama_conf->port = config_get_int_value(conf, "PUERTO");
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
	int listening_socket = open_socket(SOCKET_BACKLOG, (yama_conf->port));
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
		list_destroy_and_destroy_elements((metadata->block_list), &cierre);
		free(metadata);
	} else {
		yama_planificacion_send_resp(socket_cliente, (resp->exec_code), JOB_FINALIZADO_ERROR, -1, NULL);
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
		balanceo_de_carga((bloque_archivo_md->copies_list), (bloque_archivo_md->size), planificados);
		i++;
	}
	return planificados;
}

/**
 * @NAME balanceo_de_carga
 */
void balanceo_de_carga(t_list * copias_bloque_archivo, int bytes_ocupados, t_list * planificados) {

	int clock_aux = clock;

	t_fs_copy_block * copia;
	t_yama_carga_nodo * carga_nodo;

	int index;
	for (;;) {

		carga_nodo = (t_yama_carga_nodo *) list_get(carga_nodos, clock_aux);
		index = 0;

		while (index < (copias_bloque_archivo->elements_count)) {

			copia = (t_fs_copy_block *) list_get(copias_bloque_archivo, index);

			if ((strcmp((carga_nodo->nodo), (copia->node)) == 0) && ((carga_nodo->disponibilidad) > 0)) {

				char * archivo_temp = string_new();
				string_append_with_format(&archivo_temp, "/tmp/J%d-%s-B%dETF", new_job_id, (carga_nodo->nodo), (copia->node_block));

				t_transformacion * transformacion = (t_transformacion *) malloc (sizeof(t_transformacion));
				transformacion->nodo = string_duplicate(copia->node);
				transformacion->ip_port = string_duplicate(copia->ip_port);
				transformacion->bloque = (copia->node_block);
				transformacion->bytes_ocupados = bytes_ocupados;
				transformacion->archivo_temporal = string_duplicate(archivo_temp);
				list_add(planificados, transformacion);

				t_yama_estado_bloque * estado_bloque = (t_yama_estado_bloque *) malloc (sizeof(t_yama_estado_bloque));
				estado_bloque->job_id = new_job_id;
				estado_bloque->etapa = TRANSFORMACION;
				estado_bloque->nodo = string_duplicate(copia->node);
				estado_bloque->ip_port = string_duplicate(copia->ip_port);
				estado_bloque->bloque = (copia->node_block);
				estado_bloque->bytes_ocupados = bytes_ocupados;
				estado_bloque->estado = TRANSF_EN_PROCESO;
				estado_bloque->archivo_temp = string_duplicate(archivo_temp);
				estado_bloque->copias = copias_bloque_archivo;  // TODO: ¿esta lista sigue viva cuando libero la estructura entregada por FS?
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
		if (((estado_bloque->job_id) == job_id) && (strcmp((estado_bloque->nodo), nodo) == 0)
				&& ((estado_bloque->bloque) == bloque) && ((estado_bloque->etapa) == TRANSFORMACION)) {

			if (resultado == TRANSF_OK) {
				estado_bloque->estado = resultado;
			} else {
				//
				// error transformacion -> replanificacion
				//

				t_list * planificados = list_create();

				t_yama_carga_nodo * carga_nodo; // reduzco carga del nodo por error
				int j = 0;
				while (j < (carga_nodos->elements_count)) {
					carga_nodo = (t_yama_carga_nodo *) list_get(carga_nodos, j);
					if (strcmp((carga_nodo->nodo), (estado_bloque->nodo)) == 0) {
						(carga_nodo->wl)--;
						break;
					}
					j++;
				}

				if (balanceo_de_carga_replanificacion(index, planificados) == EXITO) {
					yama_planificacion_send_resp(socket_cliente, EXITO, REPLANIFICACION, job_id, planificados);
				} else {
					yama_planificacion_send_resp(socket_cliente, ERROR, JOB_FINALIZADO_ERROR, job_id, NULL);
				}
				list_destroy_and_destroy_elements(planificados, &cierre_t);

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

	t_list * copias_bloque_archivo = (estado_bloque->copias);
	t_fs_copy_block * copia_md;

	bool sin_nodos_disponibles = true;
	int index = 0;
	while (index < (copias_bloque_archivo->elements_count)) {
		copia_md = (t_fs_copy_block *) list_get(copias_bloque_archivo, index);
		if (strcmp((estado_bloque->nodo), (copia_md->node)) != 0) {
			sin_nodos_disponibles = false;
			break;
		}
	}

	if (sin_nodos_disponibles) {
		estado_bloque->etapa = REPLANIFICACION;
		estado_bloque->estado = TRANSF_ERROR_SIN_NODOS_DISP;
		return JOB_FINALIZADO_ERROR;
	}

	int clock_aux = clock;

	t_fs_copy_block * copia;
	t_yama_carga_nodo * carga_nodo;

	for (;;) {

		carga_nodo = (t_yama_carga_nodo *) list_get(carga_nodos, clock_aux);
		index = 0;

		while (index < (copias_bloque_archivo->elements_count)) {

			copia = (t_fs_copy_block *) list_get(copias_bloque_archivo, index);

			if ((strcmp((carga_nodo->nodo), (copia->node)) == 0) && (strcmp((carga_nodo->nodo), (estado_bloque->nodo)) != 0)
					&&((carga_nodo->disponibilidad) > 0)) {

				char * archivo_temp = string_new();
				string_append_with_format(&archivo_temp, "/tmp/J%d-%s-B%dETF", (estado_bloque->job_id), (carga_nodo->nodo), (copia->node_block));

				t_transformacion * transformacion = (t_transformacion *) malloc (sizeof(t_transformacion));
				transformacion->nodo = string_duplicate(copia_md->node);
				transformacion->ip_port = string_duplicate(copia_md->ip_port);
				transformacion->bloque = (copia_md->node_block);
				transformacion->bytes_ocupados = (estado_bloque->bytes_ocupados);
				transformacion->archivo_temporal = string_duplicate(archivo_temp);
				list_add(planificados, transformacion);

				free(estado_bloque->nodo);
				estado_bloque->nodo = string_duplicate(copia_md->node);
				free(estado_bloque->ip_port);
				estado_bloque->ip_port= string_duplicate(copia_md->ip_port);
				estado_bloque->bloque = (copia_md->node_block);
				estado_bloque->etapa = REPLANIFICACION;
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
			}
		}
	}
}

/**
 * @NAME chequear_inicio_reduccion_local
 */
void chequear_inicio_reduccion_local(int * socket_cliente, int job_id, char * nodo) {

	t_yama_estado_bloque * estado_bloque;
	int index = 0;
	while (index < (tabla_de_estados->elements_count)) {
		estado_bloque = (t_yama_estado_bloque *) list_get(tabla_de_estados, index);
		if (((estado_bloque->job_id) == job_id) && ((estado_bloque->etapa) == TRANSFORMACION)
				&& (strcmp((estado_bloque->nodo), nodo) == 0) && ((estado_bloque->estado) == TRANSF_EN_PROCESO)) {
			return;
		}
		index++;
	}

	planificar_etapa_reduccion_local_nodo(socket_cliente, job_id, nodo);

}

/**
 * @NAME planificar_etapa_reduccion_local_nodo
 */
void planificar_etapa_reduccion_local_nodo(int * socket_cliente, int job_id, char * nodo) {

	t_list * planificados = list_create();

	char * archivo_rl_temp = string_new();
	string_append_with_format(&archivo_rl_temp, "/tmp/J%d-%s-ERL", job_id, nodo);

	t_yama_estado_bloque * estado_bloque;
	int index = 0;
	while (index < (tabla_de_estados->elements_count)) {
		estado_bloque = (t_yama_estado_bloque *) list_get(tabla_de_estados, index);
		if (((estado_bloque->job_id) == job_id) && (strcmp((estado_bloque->nodo), nodo) == 0)) {

			t_red_local * red_local = (t_red_local *) malloc (sizeof(t_red_local));
			red_local->nodo = string_duplicate((estado_bloque->nodo));
			red_local->ip_port = string_duplicate((estado_bloque->ip_port));
			red_local->archivo_temp = string_duplicate((estado_bloque->archivo_temp));
			red_local->archivo_rl_temp = string_duplicate(archivo_rl_temp);
			list_add(planificados, red_local);

			estado_bloque->etapa = REDUCCION_LOCAL;
			estado_bloque->estado = REDUC_LOCAL_EN_PROCESO;
			estado_bloque->archivo_rl_temp = string_duplicate(archivo_rl_temp);

		}
		index++;
	}

	yama_planificacion_send_resp(socket_cliente, EXITO, REDUCCION_LOCAL, job_id, planificados);

	free(archivo_rl_temp);
	list_destroy_and_destroy_elements(planificados, &cierre_rl);
}
























void cierre(void * elemento) {
	free(elemento);
}

void cierre_t(t_transformacion * transformacion) {
	free(transformacion->nodo);
	free(transformacion->ip_port);
	free(transformacion->archivo_temporal);
	free(transformacion);
}

void cierre_rl(t_red_local * rl) {
	free(rl->nodo);
	free(rl->ip_port);
	free(rl->archivo_temp);
	free(rl->archivo_rl_temp);
	free(rl);
}


