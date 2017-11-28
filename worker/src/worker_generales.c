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
	worker_conf->filesystem_port = config_get_string_value(conf, "PUERTO_FILESYSTEM");
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
	char* instruccion = string_new();
	switch (task_code) {
			case TRANSFORM_OC:{
				t_request_transformation* request = (t_request_transformation*)pedido;
				if (request->exec_code == SUCCESS){
					size_t databin_size;
					string_append(&script_filename,PATH);
					string_append(&script_filename,"script_transf_");
					char *tiempo = temporal_get_string_time_bis();
					string_append(&script_filename,tiempo);
					//string_append(&script_filename,".pl");
					free(tiempo);
					//Creo el archivo y guardo el script a ejecutar
					create_script_file(script_filename, request->script_size, request->script);
					buffer_size = request->used_size;
					//Leer el archivo data.bin y obtener el bloque pedido
					buffer = malloc(buffer_size);
					//mapeo el archivo data.bin
					data_bin_mf_ptr = map_file(worker_conf->databin_path,&databin_size, O_RDWR);
					memcpy(buffer, data_bin_mf_ptr + (BLOCK_SIZE * (request->block)), buffer_size);
					//libero el archivo mapeado
					munmap(data_bin_mf_ptr,  databin_size);
					char* filename = string_new();
					string_append(&filename,PATH);
					string_append(&filename,worker_conf->nodo_name);
					string_append(&filename, "_Block_");
					char* bloque = string_itoa(request->block);
					string_append(&filename, bloque);
					free(bloque);
					create_block_file(filename, buffer_size, buffer);

					//compongo instrucción a ejecutar: cat del archivo + script de transformacion + ordenar + guardar en archivo temp
					string_append(&instruccion, "export PATH=$PATH:$(pwd) | cat ");
					string_append(&instruccion, filename);
					string_append(&instruccion, " | ");
					string_append(&instruccion, script_filename);
					string_append(&instruccion, " | sort > ");
					string_append(&instruccion, PATH);
					string_append(&instruccion, request->result_file);
					//string_append(&instruccion, "'");
					log_trace(logger, "WORKER - Ejecutar: %s", instruccion);

					//Probamos con system
					status = system(instruccion);

					//Prueba con Fork
					//status = run_instruction(instruccion);

					//TODO verificar la creacion del archivo para validad que salió ok
					if (!status){
						result = SUCCESS;
					}else {
						result= ERROR;
					}

					log_trace(logger, "WORKER - Transformación finalizada (Resultado %d)", result);

					//elimino archivos temporales creados
					   if(remove(filename) != 0) {
						   log_error(logger, "WORKER - Error al intentar eliminar el archivo %s", filename);
					   }
					   if(remove(script_filename) != 0) {
						   log_error(logger, "WORKER - Error al intentar eliminar el archivo %s", script_filename);
					   }
					   free(script_filename);
					   free(filename);
					   free(instruccion);
				}
				break;
			}
			case REDUCE_LOCALLY_OC:{
				 log_trace(logger, "WORKER - Dentro de reduccion local");
				t_request_local_reduction* request = (t_request_local_reduction*) pedido;
				char *tiempo = temporal_get_string_time_bis();
				string_append(&script_filename,PATH);
				string_append(&script_filename,"script_reducloc_");
				string_append(&script_filename,tiempo);
				//string_append(&script_filename,".pl");
				free(tiempo);
				//Creo el archivo y guardo el script a ejecutar
				create_script_file(script_filename, request->script_size, request->script );

				//Pruebo hacer el merge directamente con sort en la misma instruccion
				char** temp_files = string_split(request->temp_files, ";");
				//merge_temp_files(temp_files, request->result_file);
				string_append(&instruccion, "export PATH=$PATH:$(pwd) | sort ");
				int i = 0;
				while(temp_files[i]!=NULL){
					string_append(&instruccion,PATH);
					string_append(&instruccion,temp_files[i]);
					string_append(&instruccion, " ");
					i++;
				}

				 log_trace(logger, "WORKER - merge realizado");
				//compongo instrucción a ejecutar: cat para mostrar por salida standard el archivo a reducir + script de reducción + ordenar + guardar en archivo temp
				string_append(&instruccion, "| ");
				string_append(&instruccion, script_filename);
				string_append(&instruccion, "|sort > ");
				string_append(&instruccion, PATH);
				string_append(&instruccion, request->result_file);

				log_trace(logger, "WORKER - Ejecutar: %s", instruccion);
				status = system(instruccion);

				if (!status){
					result = SUCCESS;
				}else {
					result= ERROR;
				}
				log_trace(logger, "WORKER - Reducción local finalizada (Status %d)", result);

				//elimino archivos temporales creados
				   if(remove(script_filename) != 0) {
					   log_error(logger, "WORKER - Error al intentar eliminar el archivo %s", script_filename);
				   }
				   free(script_filename);
				   free(instruccion);

				break;
			}
			case REDUCE_GLOBAL_OC:{

				t_request_global_reduction * request = (t_request_global_reduction *) pedido;
				t_red_global * nodo_designado = merge_global(request->lista_nodos_reduccion_global);


				char *tiempo = temporal_get_string_time_bis();
				string_append(&script_filename,PATH);
				string_append(&script_filename,"script_reducglobal_");
				string_append(&script_filename,tiempo);
				//string_append(&script_filename,".pl");
				free(tiempo);
				//Creo el archivo y guardo el script a ejecutar
				create_script_file(script_filename, request->script_size, request->script );

				 log_trace(logger, "WORKER - merge realizado");
				//compongo instrucción a ejecutar: cat para mostrar por salida standard el archivo a reducir + script de reducción + ordenar + guardar en archivo temp
				string_append(&instruccion, "export PATH=$PATH:$(pwd) | ");
				string_append(&instruccion, "cat ");
				string_append(&instruccion, PATH);
				string_append(&instruccion, nodo_designado->archivo_rg);
				string_append(&instruccion, "_temp | ");
				string_append(&instruccion, script_filename);
				string_append(&instruccion, " > "); //|sort
				string_append(&instruccion, PATH);
				string_append(&instruccion, nodo_designado->archivo_rg);
				//string_append(&instruccion, "'");

				log_trace(logger, "WORKER - Ejecutar: %s", instruccion);
				status = system(instruccion);

				if (!status){
					result = SUCCESS;
				}else {
					result= ERROR;
				}
				log_trace(logger, "WORKER - Reducción global finalizada (Status %d)", result);

				//elimino archivos temporales creados
				   if(remove(script_filename) != 0) {
					   log_error(logger, "WORKER - Error al intentar eliminar el archivo %s", script_filename);
				   }
				   free(script_filename);
				   free(instruccion);

				break;
			}
			case STORAGE_OC:{
				log_trace(logger, "WORKER - Dentro de Almacenamiento final");
				t_request_storage_file * request = (t_request_storage_file*) pedido;
				char* file_to_save = string_new();
				string_append(&file_to_save, PATH);
				string_append(&file_to_save, request->temp_file);

				struct_file * archivo_a_enviar = read_file(file_to_save);
				if(archivo_a_enviar== NULL){
					result= ERROR;
					log_error(logger, "WORKER - Error al leer archivo final");
					break;
				}
				int socket_filesystem = connect_to_socket(worker_conf->filesystem_ip, worker_conf->filesystem_port);

				if(socket_filesystem==1){
					result= ERROR;
					log_error(logger, "WORKER - Error al conectar con Filesystem");
					break;
				}

				status = fs_handshake(socket_filesystem,WORKER, NULL, NULL, NULL,  logger);
				if (status!=SUCCESS){
					result= ERROR;
					log_error(logger, "WORKER - Error al hacer handshake con Filesystem (%d)",status);
					break;
				}

				log_trace(logger, "WORKER - Almacenando en yamafs: %s", request->final_file);
				status = fs_upload_file(socket_filesystem, request->final_file, TEXT, archivo_a_enviar->filesize, archivo_a_enviar->file, logger);

				//TODO agregar detalle de error
				if (status==SUCCESS){
					result = SUCCESS;
				}else {
					result= ERROR;
					log_error(logger, "WORKER - Error al enviar el archivo al Filesystem (%d)",status);
					break;
				}
				log_trace(logger, "WORKER - Almacenamiento finalizado (Status %d)", result);
				break;
			}
			case REDUCE_GLOBAL_OC_N:{
				t_request_local_reducion_filename* argumento = pedido;
				log_trace(logger, "Por mandar archivo a worker designado");
				mandar_archivo_temporal(argumento->fd, argumento->local_reduction_filename, logger);
				break;
			}
			default:
				log_error(logger,"WORKER - Código de tarea inválido: %d", task_code);
				break;
		}
		return result;
}

struct_file * read_file(char * path) {
	FILE * file;
	struct stat st;
	// este trim nose porque rompe
//	string_trim(&path);
	file = fopen(path, "r");

	if (file) {
//		fstat(file, &st);
		fseek(file, 0L, SEEK_END);
		size_t size = ftell(file); // st.st_size;
		fseek(file, 0L, SEEK_SET);
		struct_file * file_struct = malloc(sizeof(struct_file));
		file_struct->filesize = size;

		file_struct->file = map_file(path,&(file_struct->filesize), O_RDWR);
		fclose(file);
		return file_struct;
	}
	return NULL;
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
		case REDUCE_GLOBAL_OC:{
			t_request_global_reduction* request = (t_request_global_reduction*) buffer;
			free_request_global_reduction(request);
			break;
		}
		case REDUCE_GLOBAL_OC_N:{
			t_request_local_reducion_filename* request = (t_request_local_reducion_filename*) buffer;
			free_request_global_reduction_n(request);
			break;
		}
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
void free_request_global_reduction(t_request_global_reduction* request){
	free(request->script);
	list_destroy_and_destroy_elements(request->lista_nodos_reduccion_global, (void*) free_nodo);
	free(request);
}

void free_request_global_reduction_n(t_request_local_reducion_filename* request){
	free(request->local_reduction_filename);
	free(request);
}

void free_nodo(t_red_global* nodo){
	free(nodo->archivo_rg);
	free(nodo->archivo_rl_temp);
	free(nodo->ip_puerto);
	free(nodo->nodo);
	free(nodo);
}

void * map_file(char * file_path, size_t* size, int flags) {
	struct stat sb;
	//size_t size2;
	int fd; // file descriptor
	int status;

	fd = open(file_path, flags);

	status = fstat(fd, &sb);
	*size = sb.st_size;
	//size2 = sb.st_size;

	void * mapped_file_ptr = mmap((caddr_t) 0, *size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
	//check((mapped_file_ptr == MAP_FAILED), "mmap %s failed: %s", file_path, strerror(errno));

	return mapped_file_ptr;
}






















void leer_linea(t_estructura_loca_apareo * est_apareo) {
	if(est_apareo->es_designado){
		size_t size;
		if (!(est_apareo->termine_leer_rl_asignado) && (getline(&est_apareo->linea, &size, est_apareo->archivo_rl_designado) == -1)) {
			est_apareo->termine_leer_rl_asignado = true;
			est_apareo->linea = NULL;
			est_apareo->longitud_linea = 0;
			fclose(est_apareo->archivo_rl_designado);
		} else {
			est_apareo->longitud_linea = strlen(est_apareo->linea);
		}
	} else {
		if (est_apareo->fd > 0) {
			socket_recv(&(est_apareo->fd), &(est_apareo->longitud_linea), sizeof(int));
			if (est_apareo->longitud_linea == 0) {
				int recibido = 1;
				socket_send(&(est_apareo->fd), &recibido, sizeof(int), 0);
				close_socket(est_apareo->fd);
				est_apareo->fd = -1;
			}else {
				est_apareo->linea = malloc((est_apareo->longitud_linea) + 1);
				socket_recv(&(est_apareo->fd), est_apareo->linea, (est_apareo->longitud_linea));
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
		if(resultado_enviado == -1)
			log_error(logger, "Hubo un problema al enviar nombre de archivo reduccion local a worker auxiliar. socket: %d", apareo->fd);
		apareo->es_designado = false;
		apareo->termine_leer_rl_asignado = true;
		liberar_combo_ip(combo);
	}
	return apareo;
}

int es_designado(t_red_global *nodo){
	return nodo->designado;
}

t_red_global* merge_global(t_list * lista_reduc_global){

	t_red_global * nodo_designado = list_remove_by_condition(lista_reduc_global, es_designado);
	t_list * lista = list_map(lista_reduc_global, convertir_a_estructura_loca);

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
		free(aux->linea);
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

bool quedan_datos_por_leer(t_list *lista){
	int linea_no_nula(t_estructura_loca_apareo * estructura){
		return estructura->longitud_linea > 0;
	}
	return list_any_satisfy(lista, linea_no_nula);
}


char *temporal_get_string_time_bis() {
	struct tm *log_tm = malloc(sizeof(struct tm));
	char *str_time = string_duplicate("hh_mm_ss_mmmm");
	struct timeb tmili;
	time_t log_time;

	if ((log_time = time(NULL)) == -1) {
		error_show("Error getting date!");
		return 0;
	}

	localtime_r(&log_time, log_tm);

	if (ftime(&tmili)) {
		error_show("Error getting time!");
		return 0;
	}

	char *partial_time = string_duplicate("hh_mm_ss");
	strftime(partial_time, 127, "%H_%M_%S", log_tm);
	sprintf(str_time, "%s_%hu", partial_time, tmili.millitm);
	free(partial_time);
	free(log_tm);

	//Adjust memory allocation
	str_time = realloc(str_time, strlen(str_time) + 1);
	return str_time;
}


void gen_random(char *s, const int len) {

    static const char alphanum[] =
        "0123456789"
        "abcdefghijklmnopqrstuvwxyz";
    int i;
    for (i = 0; i < len; ++i) {
        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }
    s[len] = 0;
}

void mandar_archivo_temporal(int fd, char *nombre_archivo, t_log *logger){

	char * ruta_archivo = string_from_format("%s%s", PATH, nombre_archivo);
	FILE * f = fopen(ruta_archivo, "r");

	int largo_linea;
	char * linea = NULL;

	void * buffer;
	size_t size = 0;
	while((getline(&linea, &size, f) != -1)){
		largo_linea = strlen(linea) + 1;
		buffer = malloc(sizeof(int) + largo_linea);
		memcpy(buffer, &largo_linea, sizeof(int));
		memcpy(buffer + sizeof(int), linea, largo_linea);
		socket_send(&fd, buffer, sizeof(int) + largo_linea, 0);
		free(buffer);
	}

	int fin = 0;
	socket_send(&fd, &fin, sizeof(int), 0);
	int rec;
	log_trace(logger, "Esperando a recibir confirmacion de nodo designado");
	int bytes_recibidos = socket_recv(&fd, &rec, sizeof(int));

	log_trace(logger, "Termino el auxiliar");
	fclose(f);
}
