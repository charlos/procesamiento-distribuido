/*
 ============================================================================
 Name        : worker.c
 Author      : Carlos Flores
 Version     :
 Copyright   : GitHub @Charlos
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include "worker.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <commons/log.h>
#include <commons/string.h>
#include <shared-library/generales.h>


int listenning_socket;
int pipe_padreAHijo[2];
int * new_socket;
pid_t pid;
int status;
t_worker_conf* worker_conf;
t_log* logger;

int main(void) {

	char* buffer=malloc(BUFFER_SIZE);

	//Crear log
	crear_logger("./worker", &logger, true, LOG_LEVEL_TRACE);
	log_trace(logger, "Proceso Worker iniciando");

	//Cargar archivo de configuración del nodo
	load_properties();

	//Crear pipe de comunicación entre padre e hijo
	pipe(pipe_padreAHijo);


	listenning_socket = open_socket(SOCKET_BACKLOG, (worker_conf->worker_port));
	while (1) {
		new_socket = malloc(sizeof(int));
		*new_socket = accept_connection(listenning_socket);

		  if ((pid=fork()) == 0 ){
			  log_trace(logger, "Proceso Hijo PID %d (PID del padre: %d)",getpid(),getppid());

			  dup2(pipe_padreAHijo[0],STDIN_FILENO);

			  close( pipe_padreAHijo[1]);
			  close( pipe_padreAHijo[0]);

			  //Para correr el script de transformacion/reduccion
		      char *argv[] = {NULL};
		      char *envp[] = {NULL};
		      execve("./script_transformacion.py", argv, envp);
		    	exit(1);


		  }
		  else {
			  log_trace(logger, "Proceso Padre PID %d (PID del creado: %d)",getpid(),pid);

		  }

	}

	close_socket(listenning_socket);

	return EXIT_SUCCESS;
}
