#include "yama-prot.h"
#include "socket.h"

void closure_tt(t_transformacion *);
void closure_a(t_almacenamiento *);
void agregar_transformaciones(void *, int, t_list *);
void agregar_reducciones_locales(void *, int, t_list *);
void agregar_reducciones_globales(void *, int, t_list *);
void agregar_almacenamientos(void *, int, t_list *);
int recv_transformaciones(int, t_list *, t_log *);
int recv_reducciones_locales(int, t_list *, t_log *);
int recv_reducciones_globales(int, t_list *, t_log *);
int recv_almacenamientos(int, t_list *, t_log *);


/**	╔════════════════════════╗
	║ RECEIVE OPERATION CODE ║
	╚════════════════════════╝ **/

int yama_recv_cod_operacion(int * socket_cliente, t_log * log) {
	uint8_t prot_cod_operacion = sizeof(uint8_t);
	uint8_t cod_operacion;
	int bytes_recv = socket_recv(socket_cliente, &cod_operacion, prot_cod_operacion);
	if (bytes_recv <= 0) {
		if (log) log_error(log, "------ CLIENTE %d >> desconectado", * socket_cliente);
		return CLIENTE_DESCONECTADO;
	}
	return cod_operacion;
}


/**	╔═════════════════╗
	║ NUEVA SOLICITUD ║
	╚═════════════════╝ **/

t_yama_planificacion_resp * yama_nueva_solicitud(int socket_servidor, char * archivo, t_log * log) {

	/**	╔════════════════════════╦════════════════════════╦═══════════════════╗
	    ║ cod operacion (1 byte) ║ archivo size (4 bytes) ║ nombre de archivo ║
	    ╚════════════════════════╩════════════════════════╩═══════════════════╝ **/

	uint8_t prot_cod_operacion = sizeof(uint8_t);
	uint8_t prot_archivo_size = sizeof(uint32_t);

	uint8_t  req_cod_operacion = NUEVA_SOLICITUD;
	uint32_t req_archivo_size = strlen(archivo) + 1;

	int msg_size = sizeof(char) * (prot_cod_operacion + prot_archivo_size + req_archivo_size);
	void * request = malloc(msg_size);
	memcpy(request, &req_cod_operacion, prot_cod_operacion);
	memcpy(request + prot_cod_operacion, &req_archivo_size, prot_archivo_size);
	memcpy(request + prot_cod_operacion + prot_archivo_size, archivo, req_archivo_size);
	socket_send(&socket_servidor, request, msg_size, 0);
	free(request);

	return yama_resp_planificacion(socket_servidor, log);

}

t_yama_nueva_solicitud_req * yama_nueva_solicitud_recv_req(int * socket_cliente, t_log * log) {
	t_yama_nueva_solicitud_req * request = malloc(sizeof(t_yama_nueva_solicitud_req));
	uint8_t prot_archivo_size = sizeof(uint32_t);
	uint32_t archivo_size;
	int bytes_recv = socket_recv(socket_cliente, &archivo_size, prot_archivo_size);
	if (bytes_recv <= 0) {
		if (log) log_error(log, "------ CLIENTE %d >> desconectado", * socket_cliente);
		request->exec_code = CLIENTE_DESCONECTADO;
		return request;
	}
	request->archivo = malloc(sizeof(char) * archivo_size);
	bytes_recv = socket_recv(socket_cliente, (request->archivo), archivo_size);
	if (bytes_recv <= 0) {
		if (log) log_error(log, "------ CLIENTE %d >> desconectado", * socket_cliente);
		free(request->archivo);
		request->exec_code = CLIENTE_DESCONECTADO;
		return request;
	}
	request->exec_code = EXITO;
	return request;
}


/**	╔══════════════════════════════════════════════╗
	║ REGISTRAR RESULTADO TRANSFORMACION DE BLOQUE ║
	╚══════════════════════════════════════════════╝ **/

void yama_registrar_resultado_transf_bloque(int socket_servidor, int job_id, char * nodo, int bloque, int cod_resultado_t, t_log * log) {

	/**	╔════════════════════════╦══════════════════╦═════════════════════╦══════╦══════════════════╦═══════════════════════════════╗
		║ cod operacion (1 byte) ║ job id (4 bytes) ║ nodo size (4 bytes) ║ nodo ║ bloque (4 bytes) ║ cod resultado transf (1 byte) ║
		╚════════════════════════╩══════════════════╩═════════════════════╩══════╩══════════════════╩═══════════════════════════════╝ **/

	uint8_t prot_cod_operacion = sizeof(uint8_t);
	uint8_t prot_job_id = sizeof(uint32_t);
	uint8_t prot_nodo_size = sizeof(uint8_t);
	uint8_t prot_bloque = sizeof(uint32_t);
	uint8_t prot_cod_resultado_t = sizeof(uint8_t);

	uint8_t req_cod_operacion = REGISTRAR_RES_TRANSF_BLOQUE;
	uint32_t req_job_id = job_id;
	uint8_t req_nodo_size = strlen(nodo) + 1;
	uint32_t req_bloque = bloque;
	int8_t req_resultado_t = cod_resultado_t;

	int msg_size = sizeof(char) * (prot_cod_operacion + prot_job_id + prot_nodo_size + req_nodo_size + prot_bloque + prot_cod_resultado_t);
	void * request = malloc(msg_size);
	memcpy(request, &req_cod_operacion, prot_cod_operacion);
	memcpy(request + prot_cod_operacion, &req_job_id, prot_job_id);
	memcpy(request + prot_cod_operacion + prot_job_id, &req_nodo_size, prot_nodo_size);
	memcpy(request + prot_cod_operacion + prot_job_id + prot_nodo_size, nodo, req_nodo_size);
	memcpy(request + prot_cod_operacion + prot_job_id + prot_nodo_size + req_nodo_size, &req_bloque, prot_bloque);
	memcpy(request + prot_cod_operacion + prot_job_id + prot_nodo_size + req_nodo_size + prot_bloque, &req_resultado_t, prot_cod_resultado_t);
	socket_send(&socket_servidor, request, msg_size, 0);
	free(request);

}

t_yama_reg_resultado_t_req * yama_registrar_resultado_t_recv_req(int * socket_cliente, t_log * log) {

	t_yama_reg_resultado_t_req * request = malloc(sizeof(t_yama_reg_resultado_t_req));

	uint8_t prot_job_id = sizeof(uint32_t);
	int bytes_recv = socket_recv(socket_cliente, &(request->job_id), prot_job_id);
	if (bytes_recv <= 0) {
		if (log) log_error(log, "------ CLIENTE %d >> desconectado", * socket_cliente);
		request->exec_code = CLIENTE_DESCONECTADO;
		return request;
	}

	uint8_t prot_nodo_size = sizeof(uint8_t);
	uint8_t nodo_size;
	bytes_recv = socket_recv(socket_cliente, &nodo_size, prot_nodo_size);
	if (bytes_recv <= 0) {
		if (log) log_error(log, "------ CLIENTE %d >> desconectado", * socket_cliente);
		request->exec_code = CLIENTE_DESCONECTADO;
		return request;
	}
	request->nodo = malloc(sizeof(char) * nodo_size);
	bytes_recv = socket_recv(socket_cliente, (request->nodo), nodo_size);
	if (bytes_recv <= 0) {
		if (log) log_error(log, "------ CLIENTE %d >> desconectado", * socket_cliente);
		free(request->nodo);
		request->exec_code = CLIENTE_DESCONECTADO;
		return request;
	}

	uint8_t prot_bloque = sizeof(uint32_t);
	bytes_recv = socket_recv(socket_cliente, &(request->bloque), prot_bloque);
	if (bytes_recv <= 0) {
		free(request->nodo);
		if (log) log_error(log, "------ CLIENTE %d >> desconectado", * socket_cliente);
		request->exec_code = CLIENTE_DESCONECTADO;
		return request;
	}

	uint8_t prot_cod_resultado_t = sizeof(uint8_t);
	bytes_recv = socket_recv(socket_cliente, &(request->resultado_t), prot_cod_resultado_t);
	if (bytes_recv <= 0) {
		free(request->nodo);
		if (log) log_error(log, "------ CLIENTE %d >> desconectado", * socket_cliente);
		request->exec_code = CLIENTE_DESCONECTADO;
		return request;
	}

	request->exec_code = EXITO;
	return request;
}


/**	╔════════════════════════════════════════════════════════════════╗
	║ REGISTRAR RESULTADO (REDUCCION LOCAL O GLOBAL, ALMACENAMIENTO) ║
	╚════════════════════════════════════════════════════════════════╝ **/

void yama_registrar_resultado(int socket_servidor, int job_id, char * nodo, char tipo, int cod_resultado, t_log * log) {

	/**	╔════════════════════════╦══════════════════╦═════════════════════╦══════╦═══════════════════════════╗
		║ cod operacion (1 byte) ║ job id (4 bytes) ║ nodo size (4 bytes) ║ nodo ║ cod resultado rl (1 byte) ║
		╚════════════════════════╩══════════════════╩═════════════════════╩══════╩═══════════════════════════╝ **/

	uint8_t prot_cod_operacion = sizeof(uint8_t);
	uint8_t prot_job_id = sizeof(uint32_t);
	uint8_t prot_nodo_size = sizeof(uint8_t);
	uint8_t prot_cod_resultado = sizeof(uint8_t);

	uint8_t req_cod_operacion = ((tipo == RESP_REDUCCION_LOCAL) ? REGISTRAR_RES_REDUCCION_LOCAL : ((tipo == RESP_REDUCCION_GLOBAL) ? REGISTRAR_RES_REDUCCION_GLOBAL : REGISTRAR_RES_ALMACENAMIENTO));
	uint32_t req_job_id = job_id;
	uint8_t req_nodo_size = strlen(nodo) + 1;
	int8_t req_resultado = cod_resultado;

	int msg_size = sizeof(char) * (prot_cod_operacion + prot_job_id + prot_nodo_size + req_nodo_size + prot_cod_resultado);
	void * request = malloc(msg_size);
	memcpy(request, &req_cod_operacion, prot_cod_operacion);
	memcpy(request + prot_cod_operacion, &req_job_id, prot_job_id);
	memcpy(request + prot_cod_operacion + prot_job_id, &req_nodo_size, prot_nodo_size);
	memcpy(request + prot_cod_operacion + prot_job_id + prot_nodo_size, nodo, req_nodo_size);
	memcpy(request + prot_cod_operacion + prot_job_id + prot_nodo_size + req_nodo_size, &req_resultado, prot_cod_resultado);
	socket_send(&socket_servidor, request, msg_size, 0);
	free(request);

}

t_yama_reg_resultado_req * yama_registrar_resultado_recv_req(int * socket_cliente, t_log * log) {

	t_yama_reg_resultado_req * request = malloc(sizeof(t_yama_reg_resultado_req));

	uint8_t prot_job_id = sizeof(uint32_t);
	int bytes_recv = socket_recv(socket_cliente, &(request->job_id), prot_job_id);
	if (bytes_recv <= 0) {
		if (log) log_error(log, "------ CLIENTE %d >> desconectado", * socket_cliente);
		request->exec_code = CLIENTE_DESCONECTADO;
		return request;
	}

	uint8_t prot_nodo_size = sizeof(uint8_t);
	uint8_t nodo_size;
	bytes_recv = socket_recv(socket_cliente, &nodo_size, prot_nodo_size);
	if (bytes_recv <= 0) {
		if (log) log_error(log, "------ CLIENTE %d >> desconectado", * socket_cliente);
		request->exec_code = CLIENTE_DESCONECTADO;
		return request;
	}
	request->nodo = malloc(sizeof(char) * nodo_size);
	bytes_recv = socket_recv(socket_cliente, (request->nodo), nodo_size);
	if (bytes_recv <= 0) {
		if (log) log_error(log, "------ CLIENTE %d >> desconectado", * socket_cliente);
		free(request->nodo);
		request->exec_code = CLIENTE_DESCONECTADO;
		return request;
	}

	uint8_t prot_cod_resultado = sizeof(uint8_t);
	bytes_recv = socket_recv(socket_cliente, &(request->resultado), prot_cod_resultado);
	if (bytes_recv <= 0) {
		free(request->nodo);
		if (log) log_error(log, "------ CLIENTE %d >> desconectado", * socket_cliente);
		request->exec_code = CLIENTE_DESCONECTADO;
		return request;
	}

	request->exec_code = EXITO;
	return request;
}


/**	╔════════════════════╗
	║ YAMA PLANIFICACION ║
	╚════════════════════╝ **/

t_yama_planificacion_resp * yama_resp_planificacion(int socket_servidor, t_log * log) {

	t_yama_planificacion_resp * resp = malloc(sizeof(t_yama_planificacion_resp));

	uint8_t resp_prot_cod = sizeof(int16_t);
	int bytes_recv = socket_recv(&socket_servidor, &(resp->exec_code), resp_prot_cod);
	if (bytes_recv <= 0) {
		if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
		resp->exec_code = SERVIDOR_DESCONECTADO;
		return resp;
	}

	uint8_t resp_prot_etapa = sizeof(uint8_t);
	bytes_recv = socket_recv(&socket_servidor, &(resp->etapa), resp_prot_etapa);
	if (bytes_recv <= 0) {
		if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
		resp->exec_code = SERVIDOR_DESCONECTADO;
		return resp;
	}

	uint8_t resp_prot_job_id = sizeof(uint32_t);
	bytes_recv = socket_recv(&socket_servidor, &(resp->job_id), resp_prot_job_id);
	if (bytes_recv <= 0) {
		if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
		resp->exec_code = SERVIDOR_DESCONECTADO;
		return resp;
	}

	if ((resp->exec_code) != EXITO)
		return resp;

	t_list * planificados = list_create();

	if ((resp->etapa) == TRANSFORMACION) {
		resp->exec_code = recv_transformaciones(socket_servidor, planificados, log);
		if ((resp->exec_code) == EXITO) {
			resp->planificados = planificados;
		} else {
			list_destroy_and_destroy_elements(planificados, &closure_tt);
		}
	} else if ((resp->etapa) == REDUCCION_LOCAL) {
		resp->exec_code = recv_reducciones_locales(socket_servidor, planificados, log);
		if ((resp->exec_code) == EXITO) {
			resp->planificados = planificados;
		} else {
			list_destroy_and_destroy_elements(planificados, &closure_rl);
		}
	} else if ((resp->etapa) == REDUCCION_GLOBAL) {
		resp->exec_code = recv_reducciones_globales(socket_servidor, planificados, log);
		if ((resp->exec_code) == EXITO) {
			resp->planificados = planificados;
		} else {
			list_destroy_and_destroy_elements(planificados, &closure_rg);
		}
	} else if ((resp->etapa) == ALMACENAMIENTO) {
		resp->exec_code = recv_almacenamientos(socket_servidor, planificados, log);
		if ((resp->exec_code) == EXITO) {
			resp->planificados = planificados;
		} else {
			list_destroy_and_destroy_elements(planificados, &closure_a);
		}
	}

	return resp;
}

void yama_planificacion_send_resp(int * socket_cliente, int cod_resp, int etapa, int job_id, t_list * planificados) {

	uint8_t resp_prot_cod = sizeof(int16_t);
	uint8_t resp_prot_etapa = sizeof(uint8_t);
	uint8_t resp_prot_job_id = sizeof(uint32_t);
	uint8_t resp_prot_cant_elem = sizeof(uint32_t);

	int resp_size = (resp_prot_cod + resp_prot_etapa + resp_prot_job_id + resp_prot_cant_elem);

	if (cod_resp == EXITO) {
		if (etapa == TRANSFORMACION) {
			t_transformacion * trans;
			int i = 0;
			while (i < (planificados->elements_count)) {
				trans = (t_transformacion *) list_get(planificados, i);
				resp_size += (3 * sizeof(uint8_t)) + (2 * sizeof(int32_t)) + (strlen(trans->nodo) + 1) + (strlen(trans->ip_puerto) + 1) + (strlen(trans->archivo_temporal) + 1);
				i++;
			}
		} else if (etapa == REDUCCION_LOCAL) {
			t_red_local * red_local;
			int i = 0;
			while (i < (planificados->elements_count)) {
				red_local = (t_red_local *) list_get(planificados, i);
				resp_size += (3 * sizeof(uint8_t)) + sizeof(int32_t) + (strlen(red_local->nodo) + 1) + (strlen(red_local->ip_puerto) + 1) + (strlen(red_local->archivos_temp) + 1) + (strlen(red_local->archivo_rl_temp) + 1);
				i++;
			}
		} else if (etapa == REDUCCION_GLOBAL) {
			t_red_global * red_global;
			int i = 0;
			while (i < (planificados->elements_count)) {
				red_global = (t_red_global *) list_get(planificados, i);
				resp_size += (5 * sizeof(uint8_t)) + (strlen(red_global->nodo) + 1)
								+ (strlen(red_global->ip_puerto) + 1) + (strlen(red_global->archivo_rl_temp) + 1) + (strlen(red_global->archivo_rg) + 1);
				i++;
			}
		} else if (etapa == ALMACENAMIENTO) {
			t_almacenamiento * almacenamiento;
			int i = 0;
			while (i < (planificados->elements_count)) {
				almacenamiento = (t_almacenamiento *) list_get(planificados, i);
				resp_size += (3 * sizeof(uint8_t)) + (strlen(almacenamiento->nodo) + 1)
								+ (strlen(almacenamiento->ip_puerto) + 1) + (strlen(almacenamiento->archivo_rg) + 1);
				i++;
			}
		}
	}

	int memcpy_pos;
	void * response = malloc(resp_size);
	memcpy(response, &cod_resp, resp_prot_cod);
	memcpy_pos = resp_prot_cod;
	memcpy(response + memcpy_pos, &etapa, resp_prot_etapa);
	memcpy_pos += resp_prot_etapa;
	memcpy(response + memcpy_pos, &job_id, resp_prot_job_id);
	memcpy_pos += resp_prot_job_id;

	if (cod_resp == EXITO) {
		if (etapa == TRANSFORMACION) {
			agregar_transformaciones(response, memcpy_pos, planificados);
		} else if (etapa == REDUCCION_LOCAL) {
			agregar_reducciones_locales(response, memcpy_pos, planificados);
		} else if (etapa == REDUCCION_GLOBAL) {
			agregar_reducciones_globales(response, memcpy_pos, planificados);
		} if (etapa == ALMACENAMIENTO) {
			agregar_almacenamientos(response, memcpy_pos, planificados);
		}
	}

	socket_write(socket_cliente, response, resp_size);
	free(response);

}

void agregar_transformaciones(void * response, int memcpy_pos, t_list * transformaciones) {

	uint8_t resp_prot_cant_elem = sizeof(uint32_t);
	uint8_t resp_prot_nodo_size = sizeof(uint8_t);
	uint8_t resp_prot_ip_puerto_size = sizeof(uint8_t);
	uint8_t resp_prot_bloque = sizeof(uint32_t);
	uint8_t resp_prot_bytes = sizeof(uint32_t);
	uint8_t resp_prot_arch_temp_size = sizeof(uint8_t);

	memcpy(response + memcpy_pos, &(transformaciones->elements_count), resp_prot_cant_elem);
	memcpy_pos += resp_prot_cant_elem;

	t_transformacion * trans;
	uint8_t length;
	int i = 0;

	while (i < (transformaciones->elements_count)) {
		trans = (t_transformacion *) list_get(transformaciones, i);

		length = (strlen(trans->nodo) + 1);
		memcpy(response + memcpy_pos, &length, resp_prot_nodo_size);
		memcpy_pos += resp_prot_nodo_size;

		memcpy(response + memcpy_pos, (trans->nodo), length);
		memcpy_pos += length;

		length = (strlen(trans->ip_puerto) + 1);
		memcpy(response + memcpy_pos, &length, resp_prot_ip_puerto_size);
		memcpy_pos += resp_prot_ip_puerto_size;

		memcpy(response + memcpy_pos, (trans->ip_puerto), length);
		memcpy_pos += length;

		memcpy(response + memcpy_pos, &(trans->bloque), resp_prot_bloque);
		memcpy_pos += resp_prot_bloque;

		memcpy(response + memcpy_pos, &(trans->bytes_ocupados), resp_prot_bytes);
		memcpy_pos += resp_prot_bytes;

		length = (strlen(trans->archivo_temporal) + 1);
		memcpy(response + memcpy_pos, &length, resp_prot_arch_temp_size);
		memcpy_pos += resp_prot_arch_temp_size;

		memcpy(response + memcpy_pos, (trans->archivo_temporal), length);
		memcpy_pos += length;

		i++;
	}
}

int recv_transformaciones(int socket_servidor, t_list * lista_tranfs, t_log * log) {

	uint8_t resp_prot_cant_elem = sizeof(uint32_t);
	uint8_t resp_prot_nodo_size = sizeof(uint8_t);
	uint8_t resp_prot_ip_puerto_size = sizeof(uint8_t);
	uint8_t resp_prot_bloque = sizeof(uint32_t);
	uint8_t resp_prot_bytes = sizeof(uint32_t);
	uint8_t resp_prot_arch_temp_size = sizeof(uint8_t);

	uint32_t cant_transf;
	int bytes_recv = socket_recv(&socket_servidor, &(cant_transf), resp_prot_cant_elem);
	if (bytes_recv <= 0) {
		if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
		return SERVIDOR_DESCONECTADO;
	}

	t_transformacion * transf;
	uint8_t length;
	int i;

	for (i = 0; i < cant_transf; i++) {

		transf = (t_transformacion *) malloc(sizeof(t_transformacion));

		bytes_recv = socket_recv(&socket_servidor, &length, resp_prot_nodo_size);
		if (bytes_recv <= 0) {
			if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
			closure_tt(transf);
			return SERVIDOR_DESCONECTADO;
		}
		transf->nodo = malloc(sizeof(char) * length);
		bytes_recv = socket_recv(&socket_servidor, (transf->nodo), length);
		if (bytes_recv <= 0) {
			if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
			closure_tt(transf);
			return SERVIDOR_DESCONECTADO;
		}

		bytes_recv = socket_recv(&socket_servidor, &length, resp_prot_ip_puerto_size);
		if (bytes_recv <= 0) {
			if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
			closure_tt(transf);
			return SERVIDOR_DESCONECTADO;
		}
		transf->ip_puerto = malloc(sizeof(char) * length);
		bytes_recv = socket_recv(&socket_servidor, (transf->ip_puerto), length);
		if (bytes_recv <= 0) {
			if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
			closure_tt(transf);
			return SERVIDOR_DESCONECTADO;
		}

		bytes_recv = socket_recv(&socket_servidor, &(transf->bloque), resp_prot_bloque);
		if (bytes_recv <= 0) {
			if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
			closure_tt(transf);
			return SERVIDOR_DESCONECTADO;
		}

		bytes_recv = socket_recv(&socket_servidor, &(transf->bytes_ocupados), resp_prot_bytes);
		if (bytes_recv <= 0) {
			if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
			closure_tt(transf);
			return SERVIDOR_DESCONECTADO;
		}

		bytes_recv = socket_recv(&socket_servidor, &length, resp_prot_arch_temp_size);
		if (bytes_recv <= 0) {
			if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
			closure_tt(transf);
			return SERVIDOR_DESCONECTADO;
		}
		transf->archivo_temporal = malloc(sizeof(char) * length);
		bytes_recv = socket_recv(&socket_servidor, (transf->archivo_temporal), length);
		if (bytes_recv <= 0) {
			if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
			closure_tt(transf);
			return SERVIDOR_DESCONECTADO;
		}

		list_add(lista_tranfs, transf);
	}

	return EXITO;
}

void agregar_reducciones_locales(void * response, int memcpy_pos, t_list * reducciones_l) {

	uint8_t resp_prot_cant_elem = sizeof(uint32_t);
	uint8_t resp_prot_nodo_size = sizeof(uint8_t);
	uint8_t resp_prot_ip_puerto_size = sizeof(uint8_t);
	uint8_t resp_prot_arch_temp_size = sizeof(uint32_t);
	uint8_t resp_prot_arch_rl_temp_size = sizeof(uint8_t);

	memcpy(response + memcpy_pos, &(reducciones_l->elements_count), resp_prot_cant_elem);
	memcpy_pos += resp_prot_cant_elem;

	t_red_local * red_local;
	uint8_t uint8_length;
	uint32_t uint32_length;
	int i = 0;

	while (i < (reducciones_l->elements_count)) {
		red_local = (t_red_local *) list_get(reducciones_l, i);

		uint8_length = (strlen(red_local->nodo) + 1);
		memcpy(response + memcpy_pos, &uint8_length, resp_prot_nodo_size);
		memcpy_pos += resp_prot_nodo_size;

		memcpy(response + memcpy_pos, (red_local->nodo), uint8_length);
		memcpy_pos += uint8_length;

		uint8_length = (strlen(red_local->ip_puerto) + 1);
		memcpy(response + memcpy_pos, &uint8_length, resp_prot_ip_puerto_size);
		memcpy_pos += resp_prot_ip_puerto_size;

		memcpy(response + memcpy_pos, (red_local->ip_puerto), uint8_length);
		memcpy_pos += uint8_length;

		uint32_length = (strlen(red_local->archivos_temp) + 1);
		memcpy(response + memcpy_pos, &uint32_length, resp_prot_arch_temp_size);
		memcpy_pos += resp_prot_arch_temp_size;

		memcpy(response + memcpy_pos, (red_local->archivos_temp), uint32_length);
		memcpy_pos += uint32_length;

		uint8_length = (strlen(red_local->archivo_rl_temp) + 1);
		memcpy(response + memcpy_pos, &uint8_length, resp_prot_arch_rl_temp_size);
		memcpy_pos += resp_prot_arch_rl_temp_size;

		memcpy(response + memcpy_pos, (red_local->archivo_rl_temp), uint8_length);
		memcpy_pos += uint8_length;

		i++;
	}
}

int recv_reducciones_locales(int socket_servidor, t_list * reducciones, t_log * log) {

	uint8_t resp_prot_cant_elem = sizeof(uint32_t);
	uint8_t resp_prot_nodo_size = sizeof(uint8_t);
	uint8_t resp_prot_ip_puerto_size = sizeof(uint8_t);
	uint8_t resp_prot_arch_temp_size = sizeof(uint32_t);
	uint8_t resp_prot_arch_rl_temp_size = sizeof(uint8_t);

	uint32_t cant_red;
	int bytes_recv = socket_recv(&socket_servidor, &(cant_red), resp_prot_cant_elem);
	if (bytes_recv <= 0) {
		if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
		return SERVIDOR_DESCONECTADO;
	}

	t_red_local * red_local;
	uint8_t uint8_length;
	uint32_t uint32_length;
	int i;

	for (i = 0; i < cant_red; i++) {

		red_local = (t_red_local *) malloc(sizeof(t_red_local));

		bytes_recv = socket_recv(&socket_servidor, &uint8_length, resp_prot_nodo_size);
		if (bytes_recv <= 0) {
			if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
			closure_rl(red_local);
			return SERVIDOR_DESCONECTADO;
		}
		red_local->nodo = malloc(sizeof(char) * uint8_length);
		bytes_recv = socket_recv(&socket_servidor, (red_local->nodo), uint8_length);
		if (bytes_recv <= 0) {
			if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
			closure_rl(red_local);
			return SERVIDOR_DESCONECTADO;
		}

		bytes_recv = socket_recv(&socket_servidor, &uint8_length, resp_prot_ip_puerto_size);
		if (bytes_recv <= 0) {
			if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
			closure_rl(red_local);
			return SERVIDOR_DESCONECTADO;
		}
		red_local->ip_puerto = malloc(sizeof(char) * uint8_length);
		bytes_recv = socket_recv(&socket_servidor, (red_local->ip_puerto), uint8_length);
		if (bytes_recv <= 0) {
			if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
			closure_rl(red_local);
			return SERVIDOR_DESCONECTADO;
		}

		bytes_recv = socket_recv(&socket_servidor, &uint32_length, resp_prot_arch_temp_size);
		if (bytes_recv <= 0) {
			if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
			closure_rl(red_local);
			return SERVIDOR_DESCONECTADO;
		}
		red_local->archivos_temp = malloc(sizeof(char) * uint32_length);
		bytes_recv = socket_recv(&socket_servidor, (red_local->archivos_temp), uint32_length);
		if (bytes_recv <= 0) {
			if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
			closure_rl(red_local);
			return SERVIDOR_DESCONECTADO;
		}

		bytes_recv = socket_recv(&socket_servidor, &uint8_length, resp_prot_arch_rl_temp_size);
		if (bytes_recv <= 0) {
			if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
			closure_rl(red_local);
			return SERVIDOR_DESCONECTADO;
		}
		red_local->archivo_rl_temp = malloc(sizeof(char) * uint8_length);
		bytes_recv = socket_recv(&socket_servidor, (red_local->archivo_rl_temp), uint8_length);
		if (bytes_recv <= 0) {
			if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
			closure_rl(red_local);
			return SERVIDOR_DESCONECTADO;
		}

		list_add(reducciones, red_local);
	}

	return EXITO;
}

void agregar_reducciones_globales(void * response, int memcpy_pos, t_list * reducciones_g) {

	uint8_t resp_prot_cant_elem = sizeof(uint32_t);
	uint8_t resp_prot_nodo_size = sizeof(uint8_t);
	uint8_t resp_prot_ip_puerto_size = sizeof(uint8_t);
	uint8_t resp_prot_arch_rl_temp_size = sizeof(uint8_t);
	uint8_t resp_prot_designado = sizeof(uint8_t);
	uint8_t resp_prot_arch_rg_size = sizeof(uint8_t);

	memcpy(response + memcpy_pos, &(reducciones_g->elements_count), resp_prot_cant_elem);
	memcpy_pos += resp_prot_cant_elem;

	t_red_global * red_global;
	uint8_t length;
	int i = 0;

	while (i < (reducciones_g->elements_count)) {
		red_global = (t_red_local *) list_get(reducciones_g, i);

		length = (strlen(red_global->nodo) + 1);
		memcpy(response + memcpy_pos, &length, resp_prot_nodo_size);
		memcpy_pos += resp_prot_nodo_size;

		memcpy(response + memcpy_pos, (red_global->nodo), length);
		memcpy_pos += length;

		length = (strlen(red_global->ip_puerto) + 1);
		memcpy(response + memcpy_pos, &length, resp_prot_ip_puerto_size);
		memcpy_pos += resp_prot_ip_puerto_size;

		memcpy(response + memcpy_pos, (red_global->ip_puerto), length);
		memcpy_pos += length;

		length = (strlen(red_global->archivo_rl_temp) + 1);
		memcpy(response + memcpy_pos, &length, resp_prot_arch_rl_temp_size);
		memcpy_pos += resp_prot_arch_rl_temp_size;

		memcpy(response + memcpy_pos, (red_global->archivo_rl_temp), length);
		memcpy_pos += length;

		memcpy(response + memcpy_pos, &(red_global->designado), resp_prot_designado);
		memcpy_pos += resp_prot_designado;

		length = (strlen(red_global->archivo_rg) + 1);
		memcpy(response + memcpy_pos, &length, resp_prot_arch_rg_size);
		memcpy_pos += resp_prot_arch_rg_size;

		memcpy(response + memcpy_pos, (red_global->archivo_rg), length);
		memcpy_pos += length;

		i++;
	}
}

int recv_reducciones_globales(int socket_servidor, t_list * reducciones, t_log * log) {

	uint8_t resp_prot_cant_elem = sizeof(uint32_t);
	uint8_t resp_prot_nodo_size = sizeof(uint8_t);
	uint8_t resp_prot_ip_puerto_size = sizeof(uint8_t);
	uint8_t resp_prot_arch_rl_temp_size = sizeof(uint8_t);
	uint8_t resp_prot_designado = sizeof(uint8_t);
	uint8_t resp_prot_arch_rg_size = sizeof(uint8_t);

	uint32_t cant_red;
	int bytes_recv = socket_recv(&socket_servidor, &(cant_red), resp_prot_cant_elem);
	if (bytes_recv <= 0) {
		if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
		return SERVIDOR_DESCONECTADO;
	}

	t_red_global * red_global;
	uint8_t length;
	int i;

	for (i = 0; i < cant_red; i++) {

		red_global = (t_red_global *) malloc(sizeof(t_red_global));

		bytes_recv = socket_recv(&socket_servidor, &length, resp_prot_nodo_size);
		if (bytes_recv <= 0) {
			if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
			closure_rg(red_global);
			return SERVIDOR_DESCONECTADO;
		}
		red_global->nodo = malloc(sizeof(char) * length);
		bytes_recv = socket_recv(&socket_servidor, (red_global->nodo), length);
		if (bytes_recv <= 0) {
			if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
			closure_rg(red_global);
			return SERVIDOR_DESCONECTADO;
		}

		bytes_recv = socket_recv(&socket_servidor, &length, resp_prot_ip_puerto_size);
		if (bytes_recv <= 0) {
			if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
			closure_rg(red_global);
			return SERVIDOR_DESCONECTADO;
		}
		red_global->ip_puerto = malloc(sizeof(char) * length);
		bytes_recv = socket_recv(&socket_servidor, (red_global->ip_puerto), length);
		if (bytes_recv <= 0) {
			if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
			closure_rg(red_global);
			return SERVIDOR_DESCONECTADO;
		}

		bytes_recv = socket_recv(&socket_servidor, &length, resp_prot_arch_rl_temp_size);
		if (bytes_recv <= 0) {
			if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
			closure_rg(red_global);
			return SERVIDOR_DESCONECTADO;
		}
		red_global->archivo_rl_temp = malloc(sizeof(char) * length);
		bytes_recv = socket_recv(&socket_servidor, (red_global->archivo_rl_temp), length);
		if (bytes_recv <= 0) {
			if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
			closure_rg(red_global);
			return SERVIDOR_DESCONECTADO;
		}

		bytes_recv = socket_recv(&socket_servidor, &(red_global->designado), resp_prot_designado);
		if (bytes_recv <= 0) {
			if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
			closure_rg(red_global);
			return SERVIDOR_DESCONECTADO;
		}

		bytes_recv = socket_recv(&socket_servidor, &length, resp_prot_arch_rg_size);
		if (bytes_recv <= 0) {
			if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
			closure_rg(red_global);
			return SERVIDOR_DESCONECTADO;
		}
		red_global->archivo_rg = malloc(sizeof(char) * length);
		bytes_recv = socket_recv(&socket_servidor, (red_global->archivo_rg), length);
		if (bytes_recv <= 0) {
			if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
			closure_rg(red_global);
			return SERVIDOR_DESCONECTADO;
		}

		list_add(reducciones, red_global);
	}

	return EXITO;
}

void agregar_almacenamientos(void * response, int memcpy_pos, t_list * almacenamientos) {

	uint8_t resp_prot_cant_elem = sizeof(uint32_t);
	uint8_t resp_prot_nodo_size = sizeof(uint8_t);
	uint8_t resp_prot_ip_puerto_size = sizeof(uint8_t);
	uint8_t resp_prot_arch_rg_size = sizeof(uint8_t);

	memcpy(response + memcpy_pos, &(almacenamientos->elements_count), resp_prot_cant_elem);
	memcpy_pos += resp_prot_cant_elem;

	t_almacenamiento * almacenamiento;
	uint8_t length;
	int i = 0;

	while (i < (almacenamientos->elements_count)) {
		almacenamiento = (t_almacenamiento *) list_get(almacenamientos, i);

		length = (strlen(almacenamiento->nodo) + 1);
		memcpy(response + memcpy_pos, &length, resp_prot_nodo_size);
		memcpy_pos += resp_prot_nodo_size;

		memcpy(response + memcpy_pos, (almacenamiento->nodo), length);
		memcpy_pos += length;

		length = (strlen(almacenamiento->ip_puerto) + 1);
		memcpy(response + memcpy_pos, &length, resp_prot_ip_puerto_size);
		memcpy_pos += resp_prot_ip_puerto_size;

		memcpy(response + memcpy_pos, (almacenamiento->ip_puerto), length);
		memcpy_pos += length;

		length = (strlen(almacenamiento->archivo_rg) + 1);
		memcpy(response + memcpy_pos, &length, resp_prot_arch_rg_size);
		memcpy_pos += resp_prot_arch_rg_size;

		memcpy(response + memcpy_pos, (almacenamiento->archivo_rg), length);
		memcpy_pos += length;

		i++;
	}
}

int recv_almacenamientos(int socket_servidor, t_list * lista_almacenamientos, t_log * log) {

	uint8_t resp_prot_cant_elem = sizeof(uint32_t);
	uint8_t resp_prot_nodo_size = sizeof(uint8_t);
	uint8_t resp_prot_ip_puerto_size = sizeof(uint8_t);
	uint8_t resp_prot_arch_rg_size = sizeof(uint8_t);

	uint32_t cant;
	int bytes_recv = socket_recv(&socket_servidor, &(cant), resp_prot_cant_elem);
	if (bytes_recv <= 0) {
		if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
		return SERVIDOR_DESCONECTADO;
	}

	t_almacenamiento * almacenamiento;
	uint8_t length;
	int i;

	for (i = 0; i < cant; i++) {

		almacenamiento = (t_almacenamiento *) malloc(sizeof(t_almacenamiento));

		bytes_recv = socket_recv(&socket_servidor, &length, resp_prot_nodo_size);
		if (bytes_recv <= 0) {
			if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
			closure_a(almacenamiento);
			return SERVIDOR_DESCONECTADO;
		}
		almacenamiento->nodo = malloc(sizeof(char) * length);
		bytes_recv = socket_recv(&socket_servidor, (almacenamiento->nodo), length);
		if (bytes_recv <= 0) {
			if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
			closure_a(almacenamiento);
			return SERVIDOR_DESCONECTADO;
		}

		bytes_recv = socket_recv(&socket_servidor, &length, resp_prot_ip_puerto_size);
		if (bytes_recv <= 0) {
			if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
			closure_a(almacenamiento);
			return SERVIDOR_DESCONECTADO;
		}
		almacenamiento->ip_puerto = malloc(sizeof(char) * length);
		bytes_recv = socket_recv(&socket_servidor, (almacenamiento->ip_puerto), length);
		if (bytes_recv <= 0) {
			if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
			closure_a(almacenamiento);
			return SERVIDOR_DESCONECTADO;
		}

		bytes_recv = socket_recv(&socket_servidor, &length, resp_prot_arch_rg_size);
		if (bytes_recv <= 0) {
			if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
			closure_a(almacenamiento);
			return SERVIDOR_DESCONECTADO;
		}
		almacenamiento->archivo_rg = malloc(sizeof(char) * length);
		bytes_recv = socket_recv(&socket_servidor, (almacenamiento->archivo_rg), length);
		if (bytes_recv <= 0) {
			if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
			closure_a(almacenamiento);
			return SERVIDOR_DESCONECTADO;
		}

		list_add(lista_almacenamientos, almacenamiento);
	}

	return EXITO;
}

void closure_tt(t_transformacion * element) {
	if (element->nodo) free(element->nodo);
	if (element->ip_puerto) free(element->ip_puerto);
	if (element->archivo_temporal) free(element->archivo_temporal);
	free(element);
}

void closure_rl(t_red_local * element) {
	if (element->nodo) free(element->nodo);
	if (element->ip_puerto) free(element->ip_puerto);
	if (element->archivos_temp) free(element->archivos_temp);
	if (element->archivo_rl_temp) free(element->archivo_rl_temp);
	free(element);
}

void closure_rg(t_red_global * element) {
	if (element->nodo) free(element->nodo);
	if (element->ip_puerto) free(element->ip_puerto);
	if (element->archivo_rl_temp) free(element->archivo_rl_temp);
	if (element->archivo_rg) free(element->archivo_rg);
	free(element);
}

void closure_a(t_almacenamiento * element) {
	if (element->nodo) free(element->nodo);
	if (element->ip_puerto) free(element->ip_puerto);
	if (element->archivo_rg) free(element->archivo_rg);
	free(element);
}
