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

int listenning_socket;
int pipe_padreAHijo[2];
int pipe_hijoAPadre[2];
int * new_socket;
pid_t pid;
int status;
t_worker_conf* worker_conf;
t_log* logger;
FILE *fptr;
void* buffer;

int main(void) {

	char* buffer=malloc(BUFFER_SIZE);
	uint8_t task_code;
	char* script_filename = string_duplicate("/tmp/script/script.sh");

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

		//Recibo el código de operación para saber que tarea recibir
		int received_bytes = socket_recv(new_socket, &task_code, sizeof(uint8_t));

		if (received_bytes <= 0) {
			log_error(logger, "WORKER - Problema al recibir código de operación >> disconnected");
			return 1;
		}
		switch (task_code) {
			case TRANSFORM_OC:{
				t_request_transformation* pedido = transform_req_recv(new_socket, logger);
				//Creo el archivo y guardo el script a ejecutar
				fptr = fopen(script_filename, O_RDWR | O_CREAT | O_SYNC);
				buffer = malloc(pedido->script_size);
				memcpy(buffer,pedido->script,pedido->script_size);
				fputs(buffer,fptr);
				fflush(fptr);
				fclose(fptr);
				free(buffer);

				//Leer el archivo data.bin y obtener el bloque pedido

				break;
			}
			case REDUCE_LOCALLY_OC:{

				t_request_local_reduction* pedido = local_reduction_req_recv(new_socket, logger);

				break;
			}
			case REDUCE_GLOBAL_OC:
			case STORAGE_OC:
			default:
				log_error(logger,"WORKER - Código de tarea inválido: %d", task_code);
				break;
		}


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

			  //Para correr el script de transformacion/reduccion/lukivenga
		      char *argv[] = {NULL};
		      char *envp[] = {NULL};
		      execve(script_filename, argv, envp);
		      exit(1);


		  }
		  else {
			  log_trace(logger, "Proceso Padre PID %d (PID del creado: %d)",getpid(),pid);
			  close( pipe_padreAHijo[0] ); //Lado de lectura de lo que el padre le pasa al hijo.
			  close( pipe_hijoAPadre[1] ); //Lado de escritura de lo que hijo le pasa al padre.

			  //path del script a ejecutar es fijo, los datos sobre los que trabajará ese script van por entarda standard se le pasan por pipe
			  // y el nombre del archivo resultado lo usamos en padre al terminar hijo
			  // t_pedido_transformacion
			  write( pipe_padreAHijo[1],"ex",BUFFER_SIZE);

			  close( pipe_padreAHijo[1]);
			    /*Ya esta, como termine de escribir cierro esta parte del pipe*/

			  //para esperar a que termine el hijo y evitar que quede zombie
			  waitpid(pid,&status,0);

			  // Leo el resultado del proceso hijo, él lo saca por salida standard, que nosotros hicimos que fuera el pipe
		      read( pipe_hijoAPadre[0], buffer, BUFFER_SIZE );

		      log_trace(logger, "Lo que el hijo nos dejo:  %s",buffer);

		      close( pipe_hijoAPadre[0]);
		      log_trace(logger, "Fin Proceso Hijo PID %d con el estado %d",pid, status);


		      //le mando la respuesta a Master sobre el resultado de la etapa en el proceso hijo
		      //new_socket

		  }

	}

	close_socket(listenning_socket);

	return EXIT_SUCCESS;
}
