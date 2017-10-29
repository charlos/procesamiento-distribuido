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
//FILE *fptr;
void * data_bin_mf_ptr;

int main(void) {

	void* buffer;
	int child_status;
	//int buffer_size;
	uint8_t task_code;
	char* script_filename = string_new();
	string_append(&script_filename,PATH);
	string_append(&script_filename,"script.sh");

	char* instruccion = string_new();

	//Crear log
	crear_logger("./worker", &logger, true, LOG_LEVEL_TRACE);
	log_trace(logger, "Proceso Worker iniciando");

	//Cargar archivo de configuración del nodo
	load_properties();

	//mapeo el archivo data.bin
	data_bin_mf_ptr = map_file(worker_conf->databin_path, O_RDONLY);

	//Crear pipe de comunicación entre padre e hijo
	//pipe(pipe_padreAHijo);
	//pipe(pipe_hijoAPadre);

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
			case TRANSFORM_OC:
				//t_request_transformation* pedido = transform_req_recv(new_socket, logger);
				buffer = transform_req_recv(new_socket, logger);
//				if (pedido->exec_code == SUCCESS){
//					//Creo el archivo y guardo el script a ejecutar
//					create_script_file(script_filename, pedido->script_size, pedido->script );
//
//					buffer_size = pedido->used_size;
//					//Leer el archivo data.bin y obtener el bloque pedido
//					buffer = malloc(buffer_size);
//					memcpy(buffer, data_bin_mf_ptr + (BLOCK_SIZE * (pedido->block)), buffer_size);
//
//					string_append(&instruccion, script_filename);
//					string_append(&instruccion, "|sort");
//				}
				break;

			case REDUCE_LOCALLY_OC:
				//t_request_local_reduction* pedido = local_reduction_req_recv(new_socket, logger);
				buffer  = local_reduction_req_recv(new_socket, logger);
//				char** temp_files = string_split(pedido->temp_files, " ");
//				merge_temp_files(temp_files, pedido->result_file);

				break;
			case REDUCE_GLOBAL_OC:
			case STORAGE_OC:
			//case REQUEST_TEMP_FILE:
			default:
				log_error(logger,"WORKER - Código de tarea inválido: %d", task_code);
				break;
		}


		  if ((pid=fork()) == 0 ){
			  log_trace(logger, "Proceso Hijo PID %d (PID del padre: %d)",getpid(),getppid());

//			  dup2(pipe_padreAHijo[0],STDIN_FILENO);
//			  dup2(pipe_hijoAPadre[1],STDOUT_FILENO);

			  //read( pipe_padreAHijo[0], buffer, buffer_size );

//			  close( pipe_padreAHijo[1]);
//			  close( pipe_padreAHijo[0]);
//			  close( pipe_hijoAPadre[0]);
//			  close( pipe_hijoAPadre[1]);
			  //log_trace(logger, "lo que se lee de pipe: %s", buffer);

			  //Para correr el script de transformacion/reduccion/lukivenga
//		      char *argv[] = {NULL};
//		      char *envp[] = {NULL};
//		      execve(script_filename, argv, envp);


			  child_status = processRequest(task_code, buffer);

			  //TODO reponder a Master el fin del ejecución del pedido

			  //cierro el hijo
		      exit(0);

		  }
		  else {

			  free(buffer);

//			  log_trace(logger, "Proceso Padre PID %d (PID del creado: %d)",getpid(),pid);
//			  close( pipe_padreAHijo[0] ); //Lado de lectura de lo que el padre le pasa al hijo.
//			  close( pipe_hijoAPadre[1] ); //Lado de escritura de lo que hijo le pasa al padre.

			  //path del script a ejecutar es fijo, los datos sobre los que trabajará ese script van por entarda standard se le pasan por pipe
			  // y el nombre del archivo resultado lo usamos en padre al terminar hijo
			  // t_pedido_transformacion
			 // write( pipe_padreAHijo[1],"ex",buffer_size);

//			  close( pipe_padreAHijo[1]);
			    /*Ya esta, como termine de escribir cierro esta parte del pipe*/

			  //para esperar a que termine el hijo y evitar que quede zombie
//			  waitpid(pid,&status,0);

			  // Leo el resultado del proceso hijo, él lo saca por salida standard, que nosotros hicimos que fuera el pipe
//		      read( pipe_hijoAPadre[0], buffer, buffer_size );

//		      log_trace(logger, "Lo que el hijo nos dejo:  %s",buffer);

//		      close( pipe_hijoAPadre[0]);
//		      log_trace(logger, "Fin Proceso Hijo PID %d con el estado %d",pid, status);

		  }

	}

	close_socket(listenning_socket);

	return EXIT_SUCCESS;
}
