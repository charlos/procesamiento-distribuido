/*
 * worker_generales.c
 *
 *  Created on: 13/9/2017
 *      Author: utnso
 */
#include <commons/config.h>
#include "worker.h"

extern t_worker_conf* worker_conf;
//extern FILE *fptr;
extern t_log* logger;
extern void * data_bin_mf_ptr;

void load_properties(char * pathcfg) {
	t_config * conf = config_create(pathcfg);
	worker_conf = malloc(sizeof(t_worker_conf));
	worker_conf->filesystem_ip = config_get_string_value(conf, "IP_FILESYSTEM");
	worker_conf->filesystem_port = config_get_int_value(conf, "PUERTO_FILESYSTEM");
	worker_conf->nodo_name = config_get_string_value(conf, "NOMBRE_NODO");
	worker_conf->worker_port = config_get_int_value(conf, "PUERTO_WORKER");
	worker_conf->databin_path = config_get_string_value(conf, "RUTA_DATABIN");
	free(conf);
}


void create_script_file(char *script_filename, int script_size, void* script ){
	FILE* fptr = fopen(script_filename, "w+");
	fwrite(script, sizeof(char), script_size, fptr);
	fflush(fptr);
	fclose(fptr);
	chmod(script_filename, 0777);
}

void create_block_file(char *filename, int size, void* block ){
	FILE* fptr = fopen(filename, "w+");
	fwrite(block, sizeof(char), size, fptr);
	fflush(fptr);
	fclose(fptr);
	chmod(filename, 0777);
}

int merge_temp_files(char** temp_files, char* result_file){
	char* path_file_aux = string_new();
	string_append(&path_file_aux,PATH);
	string_append(&path_file_aux,result_file);
	FILE *file_aux = fopen(path_file_aux, "w");
	fclose(file_aux);
	file_aux = fopen(path_file_aux, "r+");
	FILE *file_temp;
	char* result = string_new();
	char* path_file_temp = string_new();
	size_t lenght_result=0;
	int i = 0;
	while(temp_files[i]!=NULL){
		string_append(&path_file_temp,PATH);
		string_append(&path_file_temp,temp_files[i]);
		file_temp = fopen(path_file_temp, "r");
	    if (file_temp == NULL) {
	    	log_error(logger, "WORKER - Apareo - Error al abrir archivo temporal");
	    	return 1;
	    }
		lenght_result = merge_two_files(file_temp, file_aux, &result);
		fclose(file_temp);
		free(path_file_temp);
		path_file_temp = string_new();
		fwrite(result, sizeof(char), lenght_result, file_aux);
		free(result);
		result = string_new();
		rewind(file_aux);
		i++;
	}
	free(path_file_temp);
	return 0;
}

size_t merge_two_files(FILE* file1, FILE* file2, char** result){
    char * line1 = NULL;
    char * line2 = NULL;
	size_t len1 = 0;
    size_t len2 = 0;
    ssize_t read1;
	ssize_t read2;

	size_t lenght_result=0;

	read1 = getline(&line1, &len1, file1);
	read2 = getline(&line2, &len2, file2);

	while(!feof(file1) || !feof(file2)){
		if(feof(file2) || (!feof(file1) && strcmp(line1, line2)<0)){
			//fprintf(result_file, "%s", line1);
			string_append(result, line1);
			lenght_result+=strlen(line1);
			//free(line1);
			read1 = getline(&line1, &len1, file1);
		}else{
			//fprintf(result_file, "%s", line2);
			string_append(result, line2);
			lenght_result+=strlen(line2);
			//free(line2);
			read2 = getline(&line2, &len2, file2);
		}
	}
	return lenght_result;
}


int processRequest(uint8_t task_code, void* pedido){
	void* buffer;
	int buffer_size;
	int status;
	int result;
	char* script_filename = string_new();
	string_append(&script_filename,PATH);
	string_append(&script_filename,"script.sh");

	char* instruccion = string_new();
	switch (task_code) {
			case TRANSFORM_OC:{
				t_request_transformation* request = (t_request_transformation*)pedido;
				if (request->exec_code == SUCCESS){
					//Creo el archivo y guardo el script a ejecutar
					create_script_file(script_filename, request->script_size, request->script );

					buffer_size = request->used_size;
					//Leer el archivo data.bin y obtener el bloque pedido
					buffer = malloc(buffer_size);
					//mapeo el archivo data.bin
					data_bin_mf_ptr = map_file(worker_conf->databin_path, O_RDWR); //O_RDONLY
					memcpy(buffer, data_bin_mf_ptr + (BLOCK_SIZE * (request->block)), buffer_size);
					char* filename = string_new();
					string_append(&filename,PATH);
					string_append(&filename, "Block_");
					//string_append(&filename, temporal_get_string_time());
					//string_append(&filename, request->block);
					create_block_file(filename, buffer_size, buffer);

					//compongo instrucción a ejecutar: cat del archivo + script de transformacion + ordenar + guardar en archivo temp
					string_append(&instruccion, " cat ");
					string_append(&instruccion, filename);
					string_append(&instruccion, " | sh ");
					string_append(&instruccion, script_filename);
					string_append(&instruccion, " | sort > ");
					string_append(&instruccion, PATH);
					string_append(&instruccion, request->result_file);
					//string_append(&instruccion, "'");

					log_trace(logger, "WORKER - Ejecutar: %s", instruccion);

					//Probamos con system
					status = system(instruccion);
					//status = system("strace /bin/ls > lalalala.txt");

					//Prueba con Fork
					//status = run_instruction(instruccion);

					//TODO verificar la creacion del archivo para validad que salió ok
					if (!status){
						result = SUCCESS;
					}else {
						result= ERROR;
					}

					log_trace(logger, "WORKER - Transformación finalizada (Resultado %d)", result);

				}
				break;
			}
			case REDUCE_LOCALLY_OC:{
				 log_trace(logger, "WORKER - Dentro de reduccion local");
				t_request_local_reduction* request = (t_request_local_reduction*) pedido;

				//Creo el archivo y guardo el script a ejecutar
				create_script_file(script_filename, request->script_size, request->script );

				//Pruebo hacer el merge directamente con sort en la misma instruccion
				char** temp_files = string_split(request->temp_files, ";");
				//merge_temp_files(temp_files, request->result_file);
				string_append(&instruccion, " sort ");
				int i = 0;
				while(temp_files[i]!=NULL){
					string_append(&instruccion,PATH);
					string_append(&instruccion,temp_files[i]);
					string_append(&instruccion, " ");
					i++;
				}

				 log_trace(logger, "WORKER - merge realizado");
				//compongo instrucción a ejecutar: cat para mostrar por salida standard el archivo a reducir + script de reducción + ordenar + guardar en archivo temp
				//string_append(&instruccion, "cat ");
				//string_append(&instruccion, PATH);
				//string_append(&instruccion, request->result_file);
				string_append(&instruccion, "| ");
				string_append(&instruccion, script_filename);
				string_append(&instruccion, "|sort > ");
				string_append(&instruccion, PATH);
				string_append(&instruccion, request->result_file);
				//string_append(&instruccion, "'");

				log_trace(logger, "WORKER - Ejecutar: %s", instruccion);
				status = system(instruccion);

				if (!status){
					result = SUCCESS;
				}else {
					result= ERROR;
				}
				log_trace(logger, "WORKER - Reducción local finalizada (Status %d)", result);

				break;
			}
			case REDUCE_GLOBAL_OC:
				merge_global(pedido, "[nombre-de-tmp-local-propio]");
				break;
			case STORAGE_OC:
			//case REQUEST_TEMP_FILE:
			default:
				log_error(logger,"WORKER - Código de tarea inválido: %d", task_code);
				break;
		}
		return result;
}

struct_file * read_file(char * path) {
	struct stat sb;
		if ((stat(path, &sb) < 0) || (stat(path, &sb) == 0 && !(S_ISREG(sb.st_mode)))) {
			log_error(logger,"WORKER - No se pudo abrir el archivo: %s",path);
			return NULL;
		}

	struct_file * file_struct = malloc(sizeof(struct_file));
	file_struct->filesize =  sb.st_size;
	file_struct->file = map_file(path, O_RDWR);

	return file_struct;

}

void free_request(int task_code, void* buffer){
	switch (task_code) {
		case TRANSFORM_OC:{
			t_request_transformation* request = (t_request_transformation*)buffer;
			free_request_transformation(request);
			break;
		}
		case REDUCE_LOCALLY_OC:{
			t_request_local_reduction* request = (t_request_local_reduction*)buffer;
			free_request_local_reduction(request);
			break;
		}
		case REDUCE_GLOBAL_OC:
		case STORAGE_OC:
			break;
	}

}

void free_request_transformation(t_request_transformation* request){
	free(request->result_file);
	free(request->script);
	free(request);
}

void free_request_local_reduction(t_request_local_reduction* request){
	free(request->result_file);
	free(request->script);
	free(request->temp_files);
	free(request);
}

void * map_file(char * file_path, int flags) {
	struct stat sb;
	size_t size;
	int fd; // file descriptor
	int status;

	fd = open(file_path, flags);
	//check(fd < 0, "open %s failed: %s", file_path, strerror(errno));

	status = fstat(fd, &sb);
	//check(status < 0, "stat %s failed: %s", file_path, strerror(errno));
	size = sb.st_size;

	void * mapped_file_ptr = mmap((caddr_t) 0, size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
	//check((mapped_file_ptr == MAP_FAILED), "mmap %s failed: %s", file_path, strerror(errno));

	return mapped_file_ptr;
}

void leer_linea(t_estructura_loca_apareo *est_apareo){
	if(est_apareo->fd != 0){
		socket_recv(&(est_apareo->fd), &(est_apareo->longitud_linea), sizeof(int));
		if(est_apareo->longitud_linea == 0){
			est_apareo->linea = NULL;
		}else {
			est_apareo->linea = malloc(est_apareo->longitud_linea + 1);
			socket_recv(&(est_apareo->fd), est_apareo->linea, est_apareo->longitud_linea);
			est_apareo->linea[est_apareo->longitud_linea] = '\0';
		}
	} else {

	}
}

t_estructura_loca_apareo *convertir_a_estructura_loca(t_red_global *red_global){
	t_estructura_loca_apareo *apareo = malloc(sizeof(t_estructura_loca_apareo));

	ip_port_combo* combo= split_ipport(red_global->ip_puerto);
	apareo->fd = connect_to_socket(combo->ip, combo->port);
	liberar_combo_ip(combo);
	return apareo;
}

int es_designado(t_red_global *nodo){
	return nodo->designado;
}
void merge_global(t_list *lista_reduc_global, char *archivo_propio){
	t_red_global * red_global = list_remove_by_condition(lista_reduc_global, es_designado);
	t_list *lista = list_map(lista_reduc_global, convertir_a_estructura_loca);

	FILE *f, *g;
	f = fopen(red_global->archivo_rg, "w+");
	g = fopen(archivo_propio, "r");
	char *buffer, *linea_archivo_propio = NULL;
	size_t size = 0;
	getline(&linea_archivo_propio, &size, g);

	t_estructura_loca_apareo *estructura_apareo_auxiliar = malloc(sizeof(t_estructura_loca_apareo));
	int i;
	list_iterate(lista, leer_linea);
	while(quedan_datos_por_leer(lista)){
		for(i = 0; i != list_size(lista); i++){
			t_estructura_loca_apareo *apareo = list_get(lista, i);
			if(estructura_apareo_auxiliar->linea == NULL || ((apareo->linea != NULL) && strcmp(apareo->linea, estructura_apareo_auxiliar->linea)<0)){
				estructura_apareo_auxiliar = apareo;
			}else{
				// Se deja el apareo auxiliar como esta
			}
		}
		if(linea_archivo_propio != NULL && strcmp(linea_archivo_propio, estructura_apareo_auxiliar->linea) < 0 ){
			fwrite(linea_archivo_propio, sizeof(char), strlen(linea_archivo_propio), f);
			if(getline(&linea_archivo_propio, &size, g) == -1){
				free(linea_archivo_propio);
				linea_archivo_propio = NULL;
			}
		}else{
			buffer = string_duplicate(estructura_apareo_auxiliar->linea);
			fwrite(buffer, sizeof(char), strlen(buffer), f);
			free(estructura_apareo_auxiliar->linea);
			free(buffer);
			leer_linea(estructura_apareo_auxiliar);
		}
	}
	char s = '\0';
	fwrite(&s, sizeof(char), 1, f);
	fclose(f);
}

bool quedan_datos_por_leer(t_list *lista){
	int linea_no_nula(t_estructura_loca_apareo *estructura){
		return estructura->linea != NULL;
	}
	return list_any_satisfy(lista, linea_no_nula);
}
