#include <commons/collections/list.h>
#include <commons/collections/node.h>
#include <commons/log.h>
#include <stdint.h>

#ifndef YAMA_PROTOCOL_H_
#define YAMA_PROTOCOL_H_

// CODIGOS DE OPERACION
#define NUEVA_SOLICITUD         		   1
#define REGISTRAR_RES_TRANSF_BLOQUE    	   2
#define REGISTRAR_RES_REDUCCION_LOCAL      3

// ETAPAS JOBS
#define TRANSFORMACION         		  	   1
#define REPLANIFICACION       		  	   2
#define REDUCCION_LOCAL          	   	   3
#define REDUCCION_GLOBAL   	           	   4
#define JOB_FINALIZADO_OK		           5
#define JOB_FINALIZADO_ERROR		   	   6

// ESTADOS BLOQUES
#define TRANSF_EN_PROCESO			  	   1
#define TRANSF_OK 	  		   	   		   2
#define TRANSF_ERROR             		   3
#define TRANSF_ERROR_SIN_NODOS_DISP        4
#define REDUC_LOCAL_EN_PROCESO			   1
#define REDUC_LOCAL_OK 	  		   	   	   2
#define REDUC_LOCAL_ERROR             	   3

// CODIGOS DE RESPUESTAS
#define	EXITO      				  	       1
#define	ERROR							-200
#define	CLIENTE_DESCONECTADO		  	-201
#define	SERVIDOR_DESCONECTADO		   	-202

/**
 * @NAME yama_recv_cod_operacion
 * @DESC
 *
 */
int yama_recv_cod_operacion(int *, t_log *);

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
	char * nodo;
	char * ip_port;
	char * archivo_temp;
	char * archivo_rl_temp;
} t_red_local;

typedef struct {
	int16_t exec_code;
	uint8_t etapa;
	int32_t job_id;
	t_list * planificados;
} t_yama_planificacion_resp;


/**
 * @NAME yama_nueva_solicitud
 * @DESC
 *
 */
t_yama_planificacion_resp * yama_nueva_solicitud(int, char *, t_log *);

/**
 * @NAME yama_nueva_solicitud_recv_req
 * @DESC
 *
 */
t_yama_nueva_solicitud_req * yama_nueva_solicitud_recv_req(int *, t_log *);



typedef struct {
	int16_t exec_code;
	uint32_t job_id;
	char * nodo;
	uint32_t bloque;
	uint8_t resultado_t;
} t_yama_reg_resultado_t_req;

/**
 * @NAME yama_registrar_resultado_transf_bloque
 * @DESC
 *
 */
void yama_registrar_resultado_transf_bloque(int, int, char *, int, int, t_log *);

/**
 * @NAME yama_registrar_resultado_t_recv_req
 * @DESC
 *
 */
t_yama_reg_resultado_t_req * yama_registrar_resultado_t_recv_req(int *, t_log *);



typedef struct {
	int16_t exec_code;
	uint32_t job_id;
	char * nodo;
	uint8_t resultado_rl;
} t_yama_reg_resultado_rl_req;

/**
 * @NAME yama_registrar_resultado_reduccion_local
 * @DESC
 *
 */
void yama_registrar_resultado_reduccion_local(int, int, char *, int, t_log *);

/**
 * @NAME yama_registrar_resultado_rl_recv_req
 * @DESC
 *
 */
t_yama_reg_resultado_rl_req * yama_registrar_resultado_rl_recv_req(int *, t_log *);








/**
 * @NAME yama_resp_planificacion
 * @DESC
 *
 */
t_yama_planificacion_resp * yama_resp_planificacion(int, t_log *);

/**
 * @NAME yama_planificacion_send_resp
 * @DESC
 *
 */
void yama_planificacion_send_resp(int *, int, int, int, t_list *);

#endif /* YAMA_PROTOCOL_H_ */
