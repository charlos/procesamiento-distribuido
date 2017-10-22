
#ifndef YAMA_H_
#define YAMA_H_

typedef struct {
	uint32_t port;
	char * fs_ip;
	char * fs_puerto;
	uint32_t retardo_plan;
	char * algoritmo;
	uint32_t disp_base;
	char * log;
} t_yama_conf;

typedef struct {
	uint32_t job_id;
	char * nodo;
	uint32_t bloque;
	t_list * copies_list;
	int8_t etapa;
	char * archivo_temporal;
	int8_t estado;
} t_yama_estado_bloque;

typedef struct {
	uint32_t disponibilidad;
	char * nodo;
	uint32_t wl;
	uint32_t wl_total;
} t_yama_carga_nodo;

#endif /* YAMA_H_ */
