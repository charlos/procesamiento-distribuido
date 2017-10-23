#include "yama-prot.h"
#include "socket.h"

void agregar_transformaciones(void *, int *, t_list *);
int recv_transformaciones(int, t_list *, t_log *);
void closure_tt(t_transformacion *);

int yama_recv_cod_operacion(int * socket_cliente, t_log * log) {
	uint8_t prot_cod_operacion = 1;
	uint8_t cod_operacion;
	int bytes_recv = socket_recv(socket_cliente, &cod_operacion, prot_cod_operacion);
	if (bytes_recv <= 0) {
		if (log) log_error(log, "------ CLIENTE %d >> desconectado", * socket_cliente);
		return CLIENTE_DESCONECTADO;
	}
	return cod_operacion;
}

void agregar_transformaciones(void * response, int * memcpy_pos, t_list * transformaciones) {

	uint8_t resp_prot_cant_elem = 4;
	uint8_t resp_prot_nodo_size = 4;
	uint8_t resp_prot_ip_puerto_size = 4;
	uint8_t resp_prot_bloque = 4;
	uint8_t resp_prot_bytes = 4;
	uint8_t resp_prot_arch_temp_size = 4;

	memcpy(response + (* memcpy_pos), &(transformaciones->elements_count), resp_prot_cant_elem);
	(* memcpy_pos) += resp_prot_cant_elem;

	int i = 0;
	int length;
	t_transformacion * trans;

	while (i < (transformaciones->elements_count)) {
		trans = (t_transformacion *) list_get(transformaciones, i);

		length = (strlen(trans->nodo) + 1);
		memcpy(response + (* memcpy_pos), &length, resp_prot_nodo_size);
		(* memcpy_pos) += resp_prot_nodo_size;

		memcpy(response + (* memcpy_pos), (trans->nodo), length);
		(* memcpy_pos) += length;

		length = (strlen(trans->ip_port) + 1);
		memcpy(response + (* memcpy_pos), &length, resp_prot_ip_puerto_size);
		(* memcpy_pos) += resp_prot_ip_puerto_size;

		memcpy(response + (* memcpy_pos), (trans->ip_port), length);
		(* memcpy_pos) += length;

		memcpy(response + (* memcpy_pos), &(trans->bloque), resp_prot_bloque);
		(* memcpy_pos) += resp_prot_bloque;

		memcpy(response + (* memcpy_pos), &(trans->bytes_ocupados), resp_prot_bytes);
		(* memcpy_pos) += resp_prot_bytes;

		length = (strlen(trans->archivo_temporal) + 1);
		memcpy(response + (* memcpy_pos), &length, resp_prot_arch_temp_size);
		(* memcpy_pos) += resp_prot_arch_temp_size;

		memcpy(response + (* memcpy_pos), (trans->archivo_temporal), length);
		(* memcpy_pos) += length;
		i++;
	}
}

int recv_transformaciones(int socket_servidor, t_list * lista_tranfs, t_log * log) {

	uint8_t resp_prot_cant_elem = 4;
	uint8_t resp_prot_nodo_size = 4;
	uint8_t resp_prot_ip_puerto_size = 4;
	uint8_t resp_prot_bloque = 4;
	uint8_t resp_prot_bytes = 4;
	uint8_t resp_prot_arch_temp_size = 4;

	int cant_transf;
	int bytes_recv = socket_recv(&socket_servidor, &(cant_transf), resp_prot_cant_elem);
	if (bytes_recv <= 0) {
		if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
		return SERVIDOR_DESCONECTADO;
	}

	t_transformacion * transf;

	int i;
	int length;

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
		transf->ip_port = malloc(sizeof(char) * length);
		bytes_recv = socket_recv(&socket_servidor, (transf->ip_port), length);
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




/**	╔═════════════════╗
	║ NUEVA SOLICITUD ║
	╚═════════════════╝ **/

t_yama_transformaciones_resp * yama_nueva_solicitud(int socket_servidor, char * archivo, t_log * log) {

	/**	╔════════════════════════╦════════════════════════╦═══════════════════╗
	    ║ cod operacion (1 byte) ║ archivo size (4 bytes) ║ nombre de archivo ║
	    ╚════════════════════════╩════════════════════════╩═══════════════════╝ **/

	uint8_t prot_cod_operacion = 1;
	uint8_t prot_archivo_size = 4;

	uint8_t  req_cod_operacion = NUEVA_SOLICITUD;
	uint32_t req_archivo_size = strlen(archivo) + 1;

	int msg_size = sizeof(char) * (prot_cod_operacion + prot_archivo_size + req_archivo_size);
	void * request = malloc(msg_size);
	memcpy(request, &req_cod_operacion, prot_cod_operacion);
	memcpy(request + prot_cod_operacion, &req_archivo_size, prot_archivo_size);
	memcpy(request + prot_cod_operacion + prot_archivo_size, archivo, req_archivo_size);
	socket_send(&socket_servidor, request, msg_size, 0);
	free(request);

	t_yama_transformaciones_resp * resp = malloc(sizeof(t_yama_transformaciones_resp));
	uint8_t resp_prot_cod = 2;
	int bytes_recv = socket_recv(&socket_servidor, &(resp->exec_code), resp_prot_cod);
	if (bytes_recv <= 0) {
		if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
		resp->exec_code = SERVIDOR_DESCONECTADO;
		return resp;
	}

	if (resp->exec_code != EXITO)
		return resp;

	uint8_t resp_prot_job_id = 4;
	bytes_recv = socket_recv(&socket_servidor, &(resp->job_id), resp_prot_job_id);
	if (bytes_recv <= 0) {
		if (log) log_error(log, "------ SERVIDOR %d >> desconectado", socket_servidor);
		resp->exec_code = SERVIDOR_DESCONECTADO;
		return resp;
	}

	t_list * transf_list = list_create();
	int res = recv_transformaciones(socket_servidor, transf_list, log);
	if (res == EXITO) {
		resp->transformaciones = transf_list;
	} else {
		list_destroy_and_destroy_elements(transf_list, &closure_tt);
	}

	resp->exec_code = resp;
	resp->estado = TRANSFORMACION;
	return resp;
}

t_yama_nueva_solicitud_req * yama_nueva_solicitud_recv_req(int * socket_cliente, t_log * log) {
	t_yama_nueva_solicitud_req * request = malloc(sizeof(t_yama_nueva_solicitud_req));
	uint8_t prot_archivo_size = 4;
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


void yama_nueva_solicitud_send_resp(int * socket_cliente, int cod_resp, int job_id, t_list * transformaciones) {

	uint8_t resp_prot_cod = 2;
	uint8_t resp_prot_job_id = 4;

	int i;
	t_transformacion * trans;
	int resp_size = resp_prot_cod;
	if (cod_resp == EXITO) {
		resp_size += (sizeof(int32_t) * 2);
		i = 0;
		while (i < (transformaciones->elements_count)) {
			trans = (t_transformacion *) list_get(transformaciones, i);
			resp_size += (5 * sizeof(int32_t)) + (strlen(trans->nodo) + 1) + (strlen(trans->ip_port) + 1) + (strlen(trans->archivo_temporal) + 1);
			i++;
		}
	}

	void * response = malloc(resp_size);
	memcpy(response, &cod_resp, resp_prot_cod);

	if (cod_resp == EXITO) {
		int memcpy_pos = resp_prot_cod;
		memcpy(response + memcpy_pos, &job_id, resp_prot_job_id);
		memcpy_pos += resp_prot_job_id;
		agregar_transformaciones(response, &memcpy_pos, transformaciones);
	}

	socket_write(socket_cliente, response, resp_size);
	free(response);
}

void closure_tt(t_transformacion * element) {
	if (element->nodo) free(element->nodo);
	if (element->ip_port) free(element->ip_port);
	if (element->archivo_temporal) free(element->archivo_temporal);
	free(element);
}




/**	╔════════════════════════════════════╗
	║ RESULTADO TRANSFORMACION DE BLOQUE ║
	╚════════════════════════════════════╝ **/

void yama_resultado_transf_bloque(int socket_servidor, int job_id, char * nodo, int bloque, int cod_resultado_transf, t_log * log) {

	/**	╔════════════════════════╦══════════════════╦═════════════════════╦══════╦══════════════════╦═══════════════════════════════╗
		║ cod operacion (1 byte) ║ job id (4 bytes) ║ nodo size (4 bytes) ║ nodo ║ bloque (4 bytes) ║ cod_resultado_transf (1 byte) ║
		╚════════════════════════╩══════════════════╩═════════════════════╩══════╩══════════════════╩═══════════════════════════════╝ **/

	uint8_t prot_cod_operacion = 1;
	uint8_t prot_job_id = 4;
	uint8_t prot_nodo_size = 4;
	uint8_t prot_bloque = 4;
	uint8_t prot_cod_resultado_transf = 1;

	uint8_t req_cod_operacion = RESULTADO_TRANSFORMACION_BLOQUE;
	uint32_t req_job_id = job_id;
	uint32_t req_nodo_size = strlen(nodo) + 1;
	uint32_t req_bloque = bloque;
	int8_t req_resultado_transf = cod_resultado_transf;

	int msg_size = sizeof(char) * (prot_cod_operacion + prot_job_id + prot_nodo_size + req_nodo_size + prot_bloque + prot_cod_resultado_transf);
	void * request = malloc(msg_size);
	memcpy(request, &req_cod_operacion, prot_cod_operacion);
	memcpy(request + prot_cod_operacion, &req_job_id, prot_job_id);
	memcpy(request + prot_cod_operacion + prot_job_id, &req_nodo_size, prot_nodo_size);
	memcpy(request + prot_cod_operacion + prot_job_id + prot_nodo_size, nodo, req_nodo_size);
	memcpy(request + prot_cod_operacion + prot_job_id + prot_nodo_size + req_nodo_size, &req_bloque, prot_bloque);
	memcpy(request + prot_cod_operacion + prot_job_id + prot_nodo_size + req_nodo_size + prot_bloque, &req_resultado_transf, prot_cod_resultado_transf);
	socket_send(&socket_servidor, request, msg_size, 0);
	free(request);

}

t_yama_resultado_transf_bloque_req * yama_resultado_transf_bloque_recv_req(int * socket_cliente, t_log * log) {

	t_yama_resultado_transf_bloque_req * request = malloc(sizeof(t_yama_resultado_transf_bloque_req));

	uint8_t prot_job_id = 4;
	int bytes_recv = socket_recv(socket_cliente, &(request->job_id), prot_job_id);
	if (bytes_recv <= 0) {
		if (log) log_error(log, "------ CLIENTE %d >> desconectado", * socket_cliente);
		request->exec_code = CLIENTE_DESCONECTADO;
		return request;
	}
	int nodo_size;
	uint8_t prot_nodo_size = 4;
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
		request->exec_code = CLIENTE_DESCONECTADO;
		return request;
	}
	uint8_t prot_bloque = 4;
	bytes_recv = socket_recv(socket_cliente, &(request->bloque), prot_bloque);
	if (bytes_recv <= 0) {
		free(request->nodo);
		if (log) log_error(log, "------ CLIENTE %d >> desconectado", * socket_cliente);
		request->exec_code = CLIENTE_DESCONECTADO;
		return request;
	}
	uint8_t prot_cod_resultado_transf = 1;
	bytes_recv = socket_recv(socket_cliente, &(request->resultado_transf), prot_cod_resultado_transf);
	if (bytes_recv <= 0) {
		free(request->nodo);
		if (log) log_error(log, "------ CLIENTE %d >> desconectado", * socket_cliente);
		request->exec_code = CLIENTE_DESCONECTADO;
		return request;
	}
	request->exec_code = EXITO;
	return request;
}


