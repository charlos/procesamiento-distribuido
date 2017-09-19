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
int pipe_hijoAPadre[2];
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
	pipe(pipe_hijoAPadre);

	listenning_socket = open_socket(SOCKET_BACKLOG, (worker_conf->worker_port));
	while (1) {
		new_socket = malloc(sizeof(int));
		*new_socket = accept_connection(listenning_socket);

		  if ((pid=fork()) == 0 ){
			  log_trace(logger, "Proceso Hijo PID %d (PID del padre: %d)",getpid(),getppid());

			  //dup2(pipe_padreAHijo[0],STDIN_FILENO);
			  dup2(pipe_hijoAPadre[1],STDOUT_FILENO);

			  read( pipe_padreAHijo[0], buffer, 2 );  //BUFFER_SIZE

			  close( pipe_padreAHijo[1]);
			  close( pipe_padreAHijo[0]);
			  close( pipe_hijoAPadre[0]);
			  close( pipe_hijoAPadre[1]);
			  log_trace(logger, "lo que se lee de pipe: %s", buffer);
			  //creo string con comando a ejecutar
	          char *command = string_new();
	          string_append(&command, "/home/utnso/test.sh");
	          //string_append(&command, string_substring_from(buffer,2));
	          //string_append(&command, " > /home/utnso/prueba_res.dat");

			  //Para correr el script de transformacion/reduccion
		      char *argv[] = {buffer,NULL};
		      char *envp[] = {NULL};
		      execve(command, argv, envp);
		      exit(1);


		  }
		  else {
			  log_trace(logger, "Proceso Padre PID %d (PID del creado: %d)",getpid(),pid);
			  close( pipe_padreAHijo[0] ); //Lado de lectura de lo que el padre le pasa al hijo.
			  close( pipe_hijoAPadre[1] ); //Lado de escritura de lo que hijo le pasa al padre.

			  //Como ejemplo le paso al proceso hijo un valor que se usará como parámetro del script
			  //TODO hay que pasarle el path del script a ejecutar, los datos sobre los que trabajará ese script y el nombre del archivo resultado
			  // t_pedido_transformacion
			  write( pipe_padreAHijo[1],"ex",BUFFER_SIZE);

			  close( pipe_padreAHijo[1]);
			    /*Ya esta, como termine de escribir cierro esta parte del pipe*/

			  //para esperar a que termine el hijo y evitar que quede zombie
			  waitpid(pid,&status,0);

			  // Leo el resultado del proceso hijo, él lo muestra por salida standard, que nosotros hicimos que fuera el pipe
		      read( pipe_hijoAPadre[0], buffer, BUFFER_SIZE );

		      log_trace(logger, "Lo que el hijo nos dejo:  %s",buffer);

		      close( pipe_hijoAPadre[0]);
		      log_trace(logger, "Fin Proceso Hijo PID %d con el estado %d",pid, status);

		  }

	}

	close_socket(listenning_socket);

	return EXIT_SUCCESS;
}
