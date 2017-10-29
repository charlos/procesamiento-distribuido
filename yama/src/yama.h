
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
	uint32_t disponibilidad;
	char * nodo;
	uint32_t wl;
	uint32_t wl_total;
} t_yama_carga_nodo;

typedef struct {
	char * nodo;
	char * ip_puerto;
	int32_t bloque_nodo;
	uint8_t planificada;
} t_yama_copia_bloque;

typedef struct {
	uint32_t job_id;
	uint8_t etapa;
	t_list * nodos;
} t_yama_job;

typedef struct {
	char * nodo;
	char * ip_puerto;
	uint8_t estado;
	t_list * transformaciones;
	char * archivo_rl_temp;
	uint8_t designado;
	char * archivo_rg;
	uint32_t wl_total_nodo;
	t_yama_carga_nodo * carga;
} t_yama_nodo_job;

typedef struct {
	uint32_t bloque_archivo;
	uint32_t bloque_nodo;
	uint32_t bytes_ocupados;
	t_list * copias;
	char * archivo_temp;
	uint8_t estado;
} t_yama_transformacion;

#endif /* YAMA_H_ */
