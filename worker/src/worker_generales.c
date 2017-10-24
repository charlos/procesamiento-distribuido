/*
 * worker_generales.c
 *
 *  Created on: 13/9/2017
 *      Author: utnso
 */
#include <commons/config.h>
#include "worker.h"

extern t_worker_conf* worker_conf;
extern FILE *fptr;
extern t_log* logger;

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
	fptr = fopen(script_filename, O_RDWR | O_CREAT | O_SYNC);
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
	char* result;
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
