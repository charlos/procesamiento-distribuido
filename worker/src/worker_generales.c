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

void load_properties(void) {
	t_config * conf = config_create("./nodo.cfg");
	worker_conf = malloc(sizeof(t_worker_conf));
	worker_conf->filesystem_ip = config_get_string_value(conf, "IP_FILESYSTEM");
	worker_conf->filesystem_port = config_get_int_value(conf, "PUERTO_FILESYSTEM");
	worker_conf->nodo_name = config_get_string_value(conf, "NOMBRE_NODO");
	worker_conf->worker_port = config_get_int_value(conf, "PUERTO_WORKER");
	worker_conf->databin_path = config_get_string_value(conf, "RUTA_DATABIN");
	free(conf);
}


void create_script_file(char *script_filename, int script_size, void* script ){
	void* buffer;
	FILE* fptr = fopen(script_filename, O_RDWR | O_CREAT | O_SYNC);
	buffer = malloc(script_size);
	memcpy(buffer,script,script_size);
	fputs(buffer,fptr);
	fflush(fptr);
	fclose(fptr);
	free(buffer);
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
		lenght_result = merge_two_files(file_temp, file_aux, result);
		fclose(file_temp);
		free(path_file_temp);
		path_file_temp = string_new();
		fwrite(result, sizeof(char), lenght_result, file_aux);
		free(result);
		rewind(file_aux);
		i++;
	}
	free(path_file_temp);
	return 0;
}

size_t merge_two_files(FILE* file1, FILE* file2, char* result){
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
			string_append(&result, line1);
			lenght_result+=len1;
			free(line1);
			read1 = getline(&line1, &len1, file1);
		}else{
			//fprintf(result_file, "%s", line2);
			string_append(&result, line2);
			lenght_result+=len2;
			free(line2);
			read2 = getline(&line2, &len2, file2);
		}
	}
	return lenght_result;
}


int processRequest(uint8_t task_code, void* pedido){
	void* buffer;
	int buffer_size;
	int status;
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
					memcpy(buffer, data_bin_mf_ptr + (BLOCK_SIZE * (request->block)), buffer_size);

					//compongo instrucción a ejecutar: script de trasnformacion + ordenar + guardar en archivo temp
					string_append(&instruccion, script_filename);
					string_append(&instruccion, "|sort > ");
					string_append(&instruccion, PATH);
					string_append(&instruccion, request->result_file);

					FILE* input = popen (instruccion, "w");
					if (!input){
						log_error(logger, "WORKER - Transformación - Error al ejecutar");
					    //return -1;
					}
					fwrite(buffer, sizeof(char), buffer_size, input);
					status = pclose(input);
					log_trace(logger, "WORKER - Transformación realizada");

				}
				break;
			}
			case REDUCE_LOCALLY_OC:{

				t_request_local_reduction* request = (t_request_local_reduction*) pedido;

				//Creo el archivo y guardo el script a ejecutar
				create_script_file(script_filename, request->script_size, request->script );

				char** temp_files = string_split(request->temp_files, " ");
				merge_temp_files(temp_files, request->result_file);

				//compongo instrucción a ejecutar: cat para mostrar por salida standard el archivo a reducir + script de reducción + ordenar + guardar en archivo temp
				string_append(&instruccion, "cat ");
				string_append(&instruccion, PATH);
				string_append(&instruccion, request->result_file);
				string_append(&instruccion, "|");
				string_append(&instruccion, script_filename);
				string_append(&instruccion, "|sort > ");
				string_append(&instruccion, PATH);
				string_append(&instruccion, request->result_file);

				status = system(instruccion);
				log_trace(logger, "WORKER - Reducción local realizada");

				break;
			}
			case REDUCE_GLOBAL_OC:
			case STORAGE_OC:
			//case REQUEST_TEMP_FILE:
			default:
				log_error(logger,"WORKER - Código de tarea inválido: %d", task_code);
				break;
		}
		return status;
}

struct_file * read_file(char * path) {
	FILE * file;
	struct stat st;
	// este trim nose porque rompe
//	string_trim(&path);
	file = fopen(path, "r");

	if (file) {
		fseek(file, 0L, SEEK_END);
		size_t size = ftell(file); // st.st_size;
		fseek(file, 0L, SEEK_SET);
		struct_file * file_struct = malloc(sizeof(struct_file));
		file_struct->filesize = size;
		file_struct->file = malloc(file_struct->filesize);

		file_struct->file = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, file, 0);

		return file_struct;
	}
	return NULL;
}
