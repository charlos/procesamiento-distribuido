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
int new_socket;
pid_t pid;
int status;
t_worker_conf* worker_conf;
t_log* logger;
//FILE *fptr;
void * data_bin_mf_ptr;

int main(int argc, char * argv[]) {

	void* buffer;
	int child_status;
	//int buffer_size;
	uint8_t task_code;
	// -----
	t_red_global * r1,*r2,*r3;
	r1 = malloc(sizeof(t_red_global));
	r2 = malloc(sizeof(t_red_global));
	r3 = malloc(sizeof(t_red_global));
	r1->designado = 1;
	r2->designado = 0;
	r3->designado = 0;
	r2->ip_puerto = string_duplicate("127.0.0.1:6000");
	r3->ip_puerto = string_duplicate("127.0.0.1:6001");
	t_list *lista = list_create();
	list_add(lista, r1);
	list_add(lista, r2);
//	list_add(lista, r3);
	merge_global(lista);
	char* script_filename = string_new();
	string_append(&script_filename,PATH);
	string_append(&script_filename,"script.sh");

	//Crear log
	crear_logger("./worker", &logger, true, LOG_LEVEL_TRACE);
	log_trace(logger, "Proceso Worker iniciando");

	//Cargar archivo de configuración del nodo
	load_properties(argv[1]);

	listenning_socket = open_socket(SOCKET_BACKLOG, (worker_conf->worker_port));
	while (1) {
		new_socket = accept_connection(listenning_socket);

		//Recibo el código de operación para saber que tarea recibir
		int received_bytes = socket_recv(&new_socket, &task_code, sizeof(uint8_t));

		if (received_bytes <= 0) {
			log_error(logger, "WORKER - Problema al recibir código de operación >> disconnected");
			return 1;
		}
		switch (task_code) {
			case TRANSFORM_OC:{
				buffer = transform_req_recv(&new_socket, logger);
				t_request_transformation* request = (t_request_transformation*)buffer;
				request_send_resp(&new_socket, request->exec_code);

				break;
			}
			case REDUCE_LOCALLY_OC:{
				buffer  = local_reduction_req_recv(new_socket, logger);
				t_request_local_reduction* request = (t_request_local_reduction*)buffer;
				request_send_resp(&new_socket, request->exec_code);
				break;
			}
			case REDUCE_GLOBAL_OC:
				buffer = global_reduction_req_recv(&new_socket, logger);
				t_request_global_reduction * request = buffer;
				request_send_resp(&new_socket, request->exec_code);
				break;
			case STORAGE_OC:
			//case REQUEST_TEMP_FILE:
			default:
				log_error(logger,"WORKER - Código de tarea inválido: %d", task_code);
				break;
		}


		  if ((pid=fork()) == 0 ){
			  log_trace(logger, "Proceso Hijo PID %d (PID del padre: %d)",getpid(),getppid());

			  child_status = processRequest(task_code, buffer);
			  task_response_send(&new_socket,task_code, child_status, logger);

			  log_trace(logger, "WORKER - Fin de Proceso Hijo PID %d",getpid());

			  // En la ejecución del hijo libero el buffer recibido cuando se termino de utilizar
			  free_request(task_code, buffer);

			  //cierro el hijo
		      exit(0);

		  }
		  else {
				close(new_socket);
		  }

	}

	close_socket(listenning_socket);

	return EXIT_SUCCESS;
}
