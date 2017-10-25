
#ifndef YAMA_H_
#define YAMA_H_

typedef struct {
	uint32_t puerto;
	char * fs_ip;
	char * fs_puerto;
	uint32_t retardo_plan;
	char * algoritmo;
	uint32_t disp_base;
	char * log;
} t_yama_conf;

typedef struct {
	char * nodo;
	char * ip_puerto;
	int32_t bloque_nodo;
	uint8_t planificada;
} t_yama_copia_bloque;

typedef struct {
	uint32_t job_id;
	uint8_t etapa;
	uint32_t bloque_archivo;
	char * nodo;
	char * ip_puerto;
	uint32_t bloque_nodo;
	uint32_t bytes_ocupados;
	uint8_t estado;
	char * archivo_temp;
	char * archivo_rl_temp;
	uint8_t designado;
	char * archivo_rg;
	t_list * copias;
} t_yama_estado_bloque;

typedef struct {
	uint32_t disponibilidad;
	char * nodo;
	uint32_t wl;
	uint32_t wl_total;
} t_yama_carga_nodo;

#endif /* YAMA_H_ */
