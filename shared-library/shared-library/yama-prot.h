#include <commons/collections/list.h>
#include <commons/collections/node.h>
#include <commons/log.h>
#include <stdint.h>

#ifndef YAMA_PROTOCOL_H_
#define YAMA_PROTOCOL_H_

// CODIGOS DE OPERACION
#define NUEVA_SOLICITUD         		   1
#define RESULTADO_TRANSFORMACION   		   2

// ESTADOS TRANSFORMACION
#define TRANSFORMACION			   		   1
#define TRANS_REPLANIFICACION			   2
#define TRANS_FINALIZADO_OK	   			   3
#define TRANS_ERROR_FATAL	       		   4

// RESULTADO TRAN
#define TRANS_OK 	  		   	   		   1
#define TRANS_CON_ERROR	   	  			  -1

// CODIGOS DE RESPUESTAS
#define	EXITO      				  	       1
#define	ERROR							-200
#define	CLIENTE_DESCONECTADO		  	-201
#define	SERVIDOR_DESCONECTADO		   	-202

typedef struct {
	int16_t exec_code;
	char * archivo;
} t_yama_nueva_solicitud_req;

typedef struct {
	char * nodo;
	char * ip_port;
	uint32_t bloque;
	uint32_t bytes_ocupados;
	char * archivo_temporal;
} t_transformacion;

typedef struct {
	int16_t exec_code;
	uint8_t estado;
	uint32_t job_id;
	t_list * transformaciones;
} t_yama_transformaciones_resp;

int yama_recv_cod_operacion(int *, t_log *);
t_yama_transformaciones_resp * yama_nueva_solicitud(int, char *, t_log *);
t_yama_nueva_solicitud_req * yama_nueva_solicitud_recv_req(int *, t_log *);
void yama_nueva_solicitud_send_resp(int *, int, int, t_list *);

typedef struct {
	int16_t exec_code;
	uint32_t job_id;
	char * nodo;
	uint32_t bloque;
	int8_t resultado_transf;
} t_yama_resultado_transf_bloque_req;

void yama_resultado_transf_bloque(int, int, char *, int, int, t_log *);
t_yama_resultado_transf_bloque_req * yama_resultado_transf_bloque_recv_req(int *, t_log *);

#endif /* YAMA_PROTOCOL_H_ */
