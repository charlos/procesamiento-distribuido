/*
 * mock.c
 *
 *  Created on: 23/10/2017
 *      Author: utnso
 */


#include <commons/collections/list.h>
#include <commons/config.h>
#include <commons/string.h>
#include <commons/log.h>
#include <shared-library/socket.h>
#include <shared-library/yama-prot.h>
#include <shared-library/worker-prot.h>
#include <shared-library/master-prot.h>

#define SOCKET_BACKLOG 10
#define PATH   "./"
FILE *r_local;
typedef struct {
	int fd;
	int longitud_linea;
	char *linea;
	bool es_designado;
	FILE * archivo_rl_designado;
	bool termine_leer_rl_asignado;
} t_estructura_loca_apareo;

t_red_global *armar_reduccion(char *nodo, int designado, char *ip_port, char *arch_reduccion_local, char *arch_reduccion_global){
	t_red_global * reduccion = malloc(sizeof(t_red_global));
	reduccion->archivo_rg = arch_reduccion_global;
	reduccion->archivo_rl_temp = arch_reduccion_local;
	reduccion->nodo = string_from_format("nombre_nodo_%s", nodo);
	reduccion->ip_puerto = ip_port;
	reduccion->designado = designado;

	return reduccion;
}

t_transformacion *armar_transformacion(char *x){
	t_transformacion *transformacion = malloc(sizeof(t_transformacion));
	transformacion->archivo_temporal = string_from_format("archivo_temporal%s", x);
	transformacion->ip_puerto = string_duplicate("127.0.0.1:5050");
	transformacion->nodo = string_from_format("Nodo%s", x);
	transformacion->bloque = 1;
	transformacion->bytes_ocupados = 512;

	return transformacion;
}

t_list *armar_lista_reducciones(){

	t_list *planificados = list_create();
	t_red_global *designado = armar_reduccion("1", 1, "127.0.0.1:6000", string_duplicate("./temp"), string_duplicate("./resultado"));
	list_add(planificados, designado);
	t_red_global *nodo2 = armar_reduccion("2", 0, "127.0.0.1:5070", string_duplicate("./temp1"), string_duplicate("./resultado"));
	list_add(planificados, nodo2);
	t_red_global *nodo3 = armar_reduccion("3", 0, "127.0.0.1:5080", string_duplicate("./temp2"), string_duplicate("./resultado"));
	list_add(planificados, nodo3);

	return planificados;
}

void leer_linea(t_estructura_loca_apareo * est_apareo) {
	if(est_apareo->es_designado){
		size_t size;
		if (!(est_apareo->termine_leer_rl_asignado)) {
			if (getline(&est_apareo->linea, &size, est_apareo->archivo_rl_designado) == -1) {
				est_apareo->termine_leer_rl_asignado = true;
				free(est_apareo->linea);
				est_apareo->linea = NULL;
				est_apareo->longitud_linea = 0;
				fclose(est_apareo->archivo_rl_designado);
			} else {
				est_apareo->longitud_linea = strlen(est_apareo->linea);
				fwrite(est_apareo->linea, sizeof(char), est_apareo->longitud_linea, r_local);
			}
		}
	} else {
		if (est_apareo->fd > 0) {
			int recibido = socket_recv(&(est_apareo->fd), &(est_apareo->longitud_linea), sizeof(int));
			if (est_apareo->longitud_linea == 0) {
				int recibido = 1;
				socket_send(&(est_apareo->fd), &recibido, sizeof(int), 0);
				close_socket(est_apareo->fd);
				free(est_apareo->linea);
				est_apareo->fd = -1;
				char fin = '\0';
				fwrite(&fin, sizeof(char), 1, est_apareo->archivo_rl_designado);
				fclose(est_apareo->archivo_rl_designado);
			}else {
				est_apareo->linea = realloc(est_apareo->linea, est_apareo->longitud_linea);
				socket_recv(&(est_apareo->fd), est_apareo->linea, (est_apareo->longitud_linea));
				fwrite(est_apareo->linea, sizeof(char), est_apareo->longitud_linea, est_apareo->archivo_rl_designado);
			}
		}
	}
}

//
// TODO : El proceso debe finalizar si no se pudo conectar al nodo
//
t_estructura_loca_apareo * convertir_a_estructura_loca(t_red_global *red_global){
	t_estructura_loca_apareo * apareo = malloc(sizeof(t_estructura_loca_apareo));
	if(red_global->designado){
		char * ruta_rl_nodo_designado = string_from_format("%s%s", PATH, red_global->archivo_rl_temp);
		apareo->archivo_rl_designado = fopen(ruta_rl_nodo_designado, "r");
		apareo->linea = NULL;
		apareo->es_designado = true;
		apareo->termine_leer_rl_asignado = false;
	} else {
		ip_port_combo * combo= split_ipport(red_global->ip_puerto);
		apareo->fd = connect_to_socket(combo->ip, combo->port);
		int resultado_enviado = local_reduction_file_req_send(apareo->fd, red_global->archivo_rl_temp);
//		if(resultado_enviado == -1)
//			log_error(logger, "Hubo un problema al enviar nombre de archivo reduccion local a worker auxiliar. socket: %d", apareo->fd);
		apareo->es_designado = false;
		apareo->termine_leer_rl_asignado = true;
		apareo->linea = string_new();
		apareo->archivo_rl_designado = fopen(string_from_format("reduccion_local_auxiliar%d", apareo->fd), "w+");
		liberar_combo_ip(combo);
	}
	return apareo;
}

int es_designado(t_red_global *nodo){
	return nodo->designado;
}

bool quedan_datos_por_leer(t_list *lista){
	int linea_no_nula(t_estructura_loca_apareo * estructura){
		return estructura->longitud_linea > 0;
	}
	return list_any_satisfy(lista, linea_no_nula);
}
t_red_global* merge_global(t_list * lista_reduc_global){

	t_list * lista = list_map(lista_reduc_global, convertir_a_estructura_loca);
	r_local = fopen(string_duplicate("Probando_designado"), "w+");
	t_red_global * nodo_designado = list_remove_by_condition(lista_reduc_global, es_designado);

	char * ruta_reduccion_global = string_from_format("%s%s%s", PATH, nodo_designado->archivo_rg, "_temp");

	FILE * resultado_apareo_global = fopen(ruta_reduccion_global, "w+");

	char * buffer;

	list_iterate(lista, leer_linea);

	t_estructura_loca_apareo * apareo;
	t_estructura_loca_apareo * aux = NULL;

	int i;
	while (quedan_datos_por_leer(lista)) {

		for (i = 0; i < list_size(lista); i++) {
			apareo = list_get(lista, i);
			if ((apareo->longitud_linea > 0) && (aux == NULL || (strcmp(apareo->linea, aux->linea) < 0))) {
				aux = apareo;
			}
		}

		buffer = string_duplicate(aux->linea);
		fwrite(buffer, sizeof(char), strlen(buffer), resultado_apareo_global);
		free(buffer);
		leer_linea(aux);

		aux = NULL;

	}

	char s = '\0';
	fwrite(&s, sizeof(char), 1, resultado_apareo_global);
	fclose(resultado_apareo_global);
	free(ruta_reduccion_global);
	return nodo_designado;
}


int main(int argc, char** argv){


	t_list* lista= list_create();
	t_red_global* nodo_prueba = armar_reduccion(string_duplicate("Nodo1"), 0, string_duplicate("127.0.0.1:5011"), string_duplicate("J1-Nodo1-r-local"), string_duplicate("J1-FINAL"));
	list_add(lista, nodo_prueba);
	nodo_prueba = armar_reduccion(string_duplicate("Nodo2"), 0, string_duplicate("127.0.0.1:5005"), string_duplicate("J1-Nodo2-r-local"), string_duplicate("J1-FINAL"));
	list_add(lista, nodo_prueba);
	nodo_prueba = armar_reduccion(string_duplicate("Nodo3"), 0, string_duplicate("127.0.0.1:5006"), string_duplicate("J1-Nodo3-r-local"), string_duplicate("J1-FINAL"));
	list_add(lista, nodo_prueba);
	nodo_prueba = armar_reduccion(string_duplicate("Nodo4"), 1, string_duplicate("127.0.0.1:5007"), string_duplicate("J1-Nodo4-r-local"), string_duplicate("J1-FINAL"));
	list_add(lista, nodo_prueba);

	merge_global(lista);


	fgetc(stdin);
}

