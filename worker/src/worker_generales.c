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
