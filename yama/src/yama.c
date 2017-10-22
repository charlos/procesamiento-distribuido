
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
int job_id = 0;
int algoritmo;

int clock;
t_list * carga_nodos;
t_list * tabla_de_estados;

#define	SOCKET_BACKLOG 						20
#define	CLOCK 								1
#define	WCLOCK								2

void recibir_solicitudes_master();
void procesar_nueva_solicitud(int *, char *);
void posicionar_clock();
void init(void);
void incluir_nodos_para_balanceo(t_fs_metadata_file *);
void crear_log(void);
void cierre_t(t_transformacion *);
void cierre(void *);
void cargar_configuracion(void);
void calcular_disponibilidad_nodos();
void balanceo_de_carga_transformacion(t_fs_file_block_metadata *, t_list *);
t_list * planificar_etapa_transformacion(t_fs_metadata_file *);
int pwl(char *);
int nueva_solicitud(int *);
int disponibilidad(char *);
int atender_solicitud_master(int *);

int main(void) {
	cargar_configuracion();
	crear_log();
	fs_socket = connect_to_socket((yama_conf->fs_ip), (yama_conf->fs_puerto));
	if (fs_handshake(fs_socket, YAMA, NULL, NULL, NULL, log) != SUCCESS) {
		// TODO: manejar error
		// fs handshake error
		exit(EXIT_FAILURE);
	}
	recibir_solicitudes_master();
	return EXIT_SUCCESS;
}

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

void crear_log(void) {
	log = log_create((yama_conf->log), "yama_log", false, LOG_LEVEL_TRACE);
}

void init() {
	tabla_de_estados = list_create();
	carga_nodos = list_create();
	algoritmo = (strcmp(yama_conf->algoritmo, "CLOCK") == 0) ? CLOCK : WCLOCK;
}

void recibir_solicitudes_master() {
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
					int res = atender_solicitud_master(&fd_seleccionado);
					if (res == CLIENTE_DESCONECTADO) {
						FD_CLR(fd_seleccionado, &master);
					}
				}
			}
		}
	}
}

int atender_solicitud_master(int * socket_cliente) {

	int cod_operacion = yama_recv_cod_operacion(socket_cliente, log);
	if (cod_operacion == CLIENTE_DESCONECTADO)
		return CLIENTE_DESCONECTADO;

	int res;
	switch(cod_operacion){
	case NUEVA_SOLICITUD:
		res = nueva_solicitud(socket_cliente);
		break;
	default:;
	}
	return res;
}

int nueva_solicitud(int * socket_cliente) {
	t_yama_nueva_solicitud_req * req = yama_nueva_solicitud_recv_req(socket_cliente, log);
	if (req->exec_code == CLIENTE_DESCONECTADO) {
		free(req);
		return CLIENTE_DESCONECTADO;
	}
	procesar_nueva_solicitud(socket_cliente, req->archivo);
	free(req->archivo);
	free(req);
	return EXITO;
}

void procesar_nueva_solicitud(int * socket_cliente, char * archivo) {

	t_fs_get_md_file_resp * resp = fs_get_metadata_file(fs_socket, archivo, log); //  TODO: chequear ante error, estructuras no liberadas

	if (resp->exec_code == EXITO) {

		t_fs_metadata_file * metadata = resp->metadata_file;

		job_id++;
		incluir_nodos_para_balanceo(metadata);
		calcular_disponibilidad_nodos();
		posicionar_clock();

		t_list * planificados = planificar_etapa_transformacion(metadata);
		yama_nueva_solicitud_send_resp(socket_cliente, EXITO, job_id, planificados);

		list_destroy_and_destroy_elements(planificados, &cierre_t);
		list_destroy_and_destroy_elements((metadata->block_list), &cierre);
		free(metadata);
		free(resp);
	} else {
		// TODO : manejar error
		// error al recibir datos de la solicitud
	}
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

void incluir_nodos_para_balanceo(t_fs_metadata_file * metadata) {

	t_list * bloques_archivo = (metadata->block_list);
	t_list * copias;
	t_fs_file_block_metadata * bloque_archivo_md;
	t_fs_block_copy * copia;
	t_yama_carga_nodo * carga_nodo;

	bool existe;
	int i = 0;
	int k = 0;
	int j;
	while (i < (bloques_archivo->elements_count)) {
		bloque_archivo_md = (t_fs_file_block_metadata *) list_get(bloques_archivo, i);
		copias = bloque_archivo_md->copies_list;
		j = 0;
		while (j < (copias->elements_count)) {
			copia = (t_fs_block_copy *) list_get(copias, j);
			existe = false;
			k = 0;
			while (k < carga_nodos->elements_count) {
				carga_nodo = (t_yama_carga_nodo *) list_get(carga_nodos, k);
				if (strcmp((carga_nodo->nodo), (copia->node))) {
					existe = true;
					break;
				}
				k++;
			}
			if (!existe) {
				carga_nodo = (t_yama_carga_nodo *) malloc(sizeof(t_yama_carga_nodo));
				carga_nodo->nodo = string_duplicate(copia->node);
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

void calcular_disponibilidad_nodos() {
	t_yama_carga_nodo * carga_nodo;
	int index = 0;
	while (index < (carga_nodos->elements_count)) {
		carga_nodo = (t_yama_carga_nodo *) list_get(carga_nodos, index);
		carga_nodo->disponibilidad = disponibilidad(carga_nodo->nodo);
		index++;
	}
}

int disponibilidad(char * nodo) {
	return ((yama_conf->disp_base) - pwl(nodo));
}

int pwl(char * nodo) {
	if (algoritmo == CLOCK)
		return 0;
	if (carga_nodos->elements_count == 0)
		return 0;
	t_yama_carga_nodo * carga_nodo;
	int wl_max = -1;
	int wl_nodo;
	int index = 0;
	while (index < carga_nodos->elements_count) {
		carga_nodo = (t_yama_carga_nodo *) list_get(carga_nodos, index);
		if ((wl_max < 0) || ((carga_nodo->wl) > wl_max))
			wl_max = (carga_nodo->wl);
		if (strcmp(carga_nodo->nodo, nodo))
			wl_nodo = (carga_nodo->wl);
		index++;
	}
	return wl_max - wl_nodo;
}

void posicionar_clock() {

	int wl_max = -1;
	int wl_total_max = -1;
	int wl;
	int wl_total;

	t_yama_carga_nodo * carga_nodo;
	int index = 0;
	while (index < carga_nodos->elements_count) {
		carga_nodo = (t_yama_carga_nodo *) list_get(carga_nodos, index);
		wl = carga_nodo->wl;
		wl_total = carga_nodo->wl_total;
		if ((wl_max < 0) || (wl > wl_max) || ((wl == wl_max) && (wl_max > wl_total_max))) {
			wl_max = wl;
			wl_total_max = wl_total;
			clock = index;
		}
		index++;
	}

}

t_list * planificar_etapa_transformacion(t_fs_metadata_file * metadata) {
	t_list * planificados = list_create();
	t_fs_file_block_metadata * bloque_archivo_md;
	t_list * bloques_archivo = metadata->block_list;
	int i = 0;
	while (i < bloques_archivo->elements_count) {
		bloque_archivo_md = (t_fs_file_block_metadata *) list_get(bloques_archivo, i);
		balanceo_de_carga_transformacion(bloque_archivo_md, planificados);
		i++;
	}
	return planificados;
}

void balanceo_de_carga_transformacion(t_fs_file_block_metadata * bloque_archivo_md, t_list * planificados) {

	int clock_aux = clock;
	int index;
	bool planificado = false;
	t_list * copias = bloque_archivo_md->copies_list;
	t_fs_block_copy * copia;
	t_yama_carga_nodo * carga_nodo;

	while (!planificado) {
		carga_nodo = (t_yama_carga_nodo *) list_get(carga_nodos, clock_aux);
		index = 0;
		while (!planificado || index < (copias->elements_count)) {
			copia = (t_fs_block_copy *) list_get(copias, index);
			if (strcmp((carga_nodo->nodo), (copia->node)) == 0) {
				if ((carga_nodo->disponibilidad) > 0) {

					char * archivo_temp = string_new();
					string_append_with_format(&archivo_temp, "/tmp/J%d-%s-B%dETF", job_id, (carga_nodo->nodo), (copia->node_block));

					t_transformacion * transformacion = (t_transformacion *) malloc (sizeof(t_transformacion));
					transformacion->nodo = string_duplicate(carga_nodo->nodo);
					transformacion->ip_port = string_duplicate(copia->ip_port);
					transformacion->bloque = copia->node_block;
					transformacion->bytes_ocupados = bloque_archivo_md->size;
					transformacion->archivo_temporal = string_duplicate(archivo_temp);
					list_add(planificados, transformacion);

					t_yama_estado_bloque * estado_bloque = (t_yama_estado_bloque *) malloc (sizeof(t_yama_estado_bloque));
					estado_bloque->job_id = job_id;
					estado_bloque->bloque = (copia->node_block);
					estado_bloque->copies_list = copias;
					estado_bloque->etapa = TRANSFORMACION;
					estado_bloque->archivo_temporal = string_duplicate(archivo_temp);
					estado_bloque->estado = TRANSFORMACION_EN_PROCESO;
					list_add(estado_bloque, tabla_de_estados);

					free(archivo_temp);
					(carga_nodo->wl_total)++;
					(carga_nodo->disponibilidad)--;
					planificado = true;
				} else {
					carga_nodo->disponibilidad = yama_conf->disp_base;
				}
			}
			index++;
		}
	}

	clock_aux++;
	if (clock_aux > (carga_nodos->elements_count)) {
		clock_aux = 0;
	}
	if (clock_aux == clock) {
		// TODO: vuelta completa
		// ¿sumar disponibilidad base para todos los nodos?
	}
	clock = clock_aux;
}
